import streamlit as st
import pandas as pd
from sqlalchemy import create_engine, text
import requests
import base64
import urllib.parse
import io
from datetime import datetime

# --- [페이지 설정] ---
st.set_page_config(page_title="카페24 적립금 통합 관리 시스템", layout="wide")

# ==========================================
# DB 연결 상태 관리 및 토글 버튼
# ==========================================
if 'db_connected' not in st.session_state:
    # 앱을 처음 켰을 때는 DB 에러 방지를 위해 무조건 '연결 해제' 상태로 시작합니다.
    st.session_state['db_connected'] = False

st.sidebar.title("🔌 시스템 모드")
if st.session_state['db_connected']:
    st.sidebar.success("🟢 DB 연결 모드 (기록 저장 및 조회 가능)")
    if st.sidebar.button("DB 연결 끊기 (API 전용)", use_container_width=True):
        st.session_state['db_connected'] = False
        st.rerun()
else:
    st.sidebar.warning("🟡 DB 연결 해제 모드 (적립금 지급만 가능)")
    if st.sidebar.button("DB 연결 시도하기", use_container_width=True, type="primary"):
        st.session_state['db_connected'] = True
        st.rerun()

st.sidebar.divider()

# --- [DB 연결 및 초기화] ---
@st.cache_resource
def init_connection():
    db_info = st.secrets["mysql"]
    return create_engine(
        f"mysql+pymysql://{db_info['user']}:{db_info['password']}@{db_info['host']}:{db_info['port']}/{db_info['database']}?charset=utf8mb4"
    )

REQUIRED_COLS = ['아이디', '주문자명', '고객명', '브랜드', '상품', '색상', '사이즈']

engine = None
if st.session_state['db_connected']:
    # 1) 연결 + 테이블 생성 (실패하면 DB 모드 자체를 해제)
    try:
        engine = init_connection()
        with engine.connect() as conn:
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS mileage_records (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    아이디 VARCHAR(255),
                    주문자명 VARCHAR(255),
                    고객명 VARCHAR(255),
                    브랜드 VARCHAR(255),
                    상품 TEXT,
                    색상 VARCHAR(100),
                    사이즈 VARCHAR(100),
                    주문일 VARCHAR(100),
                    금액 INT,
                    비고 TEXT,
                    지급일시 DATETIME DEFAULT CURRENT_TIMESTAMP
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
            """))
            conn.commit()
    except Exception as e:
        # 🚨 뭉뚱그려진 에러 대신 실제 에러 메시지를 노출합니다.
        st.sidebar.error(f"🚨 DB 연결 실패: {e}")
        st.session_state['db_connected'] = False
        engine = None

    # 2) '주문일' 컬럼 존재 여부를 information_schema로 정확히 확인 후에만 ALTER
    #    (기존에는 무조건 ALTER 시도 + 무조건 예외 무시라서 진짜 에러도 함께 숨겨졌습니다)
    if engine is not None:
        try:
            with engine.connect() as conn:
                col_exists = conn.execute(text("""
                    SELECT COUNT(*) FROM information_schema.COLUMNS
                    WHERE TABLE_SCHEMA = DATABASE()
                      AND TABLE_NAME = 'mileage_records'
                      AND COLUMN_NAME = '주문일'
                """)).scalar()
                if not col_exists:
                    conn.execute(text("ALTER TABLE mileage_records ADD COLUMN 주문일 VARCHAR(100) AFTER 사이즈;"))
                    conn.commit()
        except Exception as e:
            st.sidebar.warning(f"⚠️ 테이블 컬럼 점검 중 문제 발생 (기능에는 영향 없을 수 있음): {e}")


# --- [카페24 설정 정보] ---
cafe24_info = st.secrets["cafe24"]
MALL_ID = cafe24_info["mall_id"]
CLIENT_ID = cafe24_info["client_id"]
CLIENT_SECRET = cafe24_info["client_secret"]
REDIRECT_URI = "https://cafe24-mileage-app.streamlit.app"
SCOPE = "mall.read_customer,mall.write_customer,mall.read_mileage,mall.write_mileage"


def get_access_token(auth_code):
    url = f"https://{MALL_ID}.cafe24api.com/api/v2/oauth/token"
    auth_str = f"{CLIENT_ID}:{CLIENT_SECRET}"
    b64_auth = base64.b64encode(auth_str.encode('utf-8')).decode('utf-8')
    headers = {"Authorization": f"Basic {b64_auth}", "Content-Type": "application/x-www-form-urlencoded"}
    data = {"grant_type": "authorization_code", "code": auth_code, "redirect_uri": REDIRECT_URI}
    try:
        response = requests.post(url, headers=headers, data=data, timeout=15)
    except Exception as e:
        return None, f"요청 실패: {e}"
    if response.status_code == 200:
        return response.json().get("access_token"), None
    return None, response.text


def send_points(access_token, member_id, amount, action_label, reason):
    """카페24 적립금 지급/차감 API 호출. (성공여부, 에러메시지) 반환."""
    url = f"https://{MALL_ID}.cafe24api.com/api/v2/admin/points"
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json",
        "X-Cafe24-Api-Version": "2026-03-01",
    }
    api_type = "increase" if "적립" in action_label or "추가" in action_label else "decrease"
    payload = {"request": {
        "member_id": str(member_id).strip(),
        "amount": abs(int(amount)),
        "type": api_type,
        "reason": reason if reason and str(reason).strip() else "적립금 처리",
    }}
    try:
        res = requests.post(url, json=payload, headers=headers, timeout=15)
    except Exception as e:
        return False, f"요청 실패: {e}"
    if res.status_code in (200, 201):
        return True, None
    return False, res.text[:300]


def make_excel_bytes(df: pd.DataFrame) -> bytes:
    out = io.BytesIO()
    with pd.ExcelWriter(out, engine='xlsxwriter') as writer:
        df.to_excel(writer, index=False)
    return out.getvalue()


def reset_state_if_new_file(uploaded_file, key_prefix):
    """새 파일이 업로드되면 이전 파일로 만들어진 세션 상태(합산표, 확인단계 등)를 정리해
    서로 다른 파일의 상태가 섞이는 것을 방지합니다."""
    sig_key = f"{key_prefix}_file_sig"
    file_sig = f"{uploaded_file.name}_{uploaded_file.size}" if uploaded_file is not None else None
    if file_sig != st.session_state.get(sig_key):
        for k in list(st.session_state.keys()):
            if k.startswith(key_prefix) and k != sig_key:
                del st.session_state[k]
        st.session_state[sig_key] = file_sig


def load_simple_format(file):
    """간편 양식(아이디+금액, 시트명 적립/차감) 엑셀을 읽어 표준 DataFrame으로 변환합니다."""
    xls = pd.ExcelFile(file)
    frames = []
    for sheet in xls.sheet_names:
        sdf = xls.parse(sheet)
        sdf.columns = sdf.columns.astype(str).str.strip()
        if '아이디' not in sdf.columns:
            continue
        amt_col = '금액' if '금액' in sdf.columns else next((c for c in sdf.columns if '금액' in c), None)
        if amt_col is None:
            continue
        name_col = next((c for c in sdf.columns if c in ['주문자명', '고객명', '이름']), None)
        reason_col = next((c for c in sdf.columns if ('내용' in c) or ('비고' in c) or ('사유' in c)), None)
        date_col = next((c for c in sdf.columns if ('날짜' in c) or ('주문일' in c)), None)

        if '차감' in sheet:
            action = '차감'
        elif '적립' in sheet:
            action = '적립'
        else:
            action = None  # 사용자에게 별도 확인

        tmp = pd.DataFrame()
        tmp['아이디'] = sdf['아이디'].astype(str).str.strip()
        tmp['주문자명'] = sdf[name_col].astype(str).str.strip() if name_col else ''
        tmp['금액'] = pd.to_numeric(sdf[amt_col], errors='coerce').fillna(0).astype(int)
        tmp['사유'] = sdf[reason_col].astype(str).str.strip().replace('nan', '') if reason_col else ''
        tmp['날짜'] = sdf[date_col].astype(str).str.strip() if date_col else ''
        tmp['구분'] = action
        tmp['시트명'] = sheet

        tmp = tmp[(tmp['아이디'].notna()) & (tmp['아이디'] != '') & (tmp['아이디'].str.lower() != 'nan')]
        frames.append(tmp)

    if not frames:
        return None
    return pd.concat(frames, ignore_index=True)


# --- [사이드바 메뉴] ---
st.sidebar.title("🚀 메뉴 선택")
menu = st.sidebar.radio("원하시는 작업을 선택하세요", ["적립금 지급하기", "기록 조회 및 다운로드", "DB 기록 삭제"])

# ==========================================
# 화면 1: 적립금 지급하기
# ==========================================
if menu == "적립금 지급하기":
    st.title("💰 적립금 자동 지급/차감 시스템")

    if "code" in st.query_params and "access_token" not in st.session_state:
        token, error_msg = get_access_token(st.query_params["code"])
        if token:
            st.session_state["access_token"] = token
            st.query_params.clear()
            st.rerun()
        else:
            st.error(f"🚨 카페24 인증 실패: {error_msg}")

    if "access_token" not in st.session_state:
        auth_url = (
            f"https://{MALL_ID}.cafe24api.com/api/v2/oauth/authorize?response_type=code"
            f"&client_id={CLIENT_ID}&state=random&redirect_uri={urllib.parse.quote(REDIRECT_URI)}&scope={SCOPE}"
        )
        st.link_button("🔐 카페24 로그인 및 연동하기", auth_url, type="primary")
        st.stop()
    else:
        st.success(f"✅ {MALL_ID} 연결 성공!")

    excel_mode = st.radio(
        "📋 사용할 엑셀 양식을 선택하세요",
        ["상세 양식 (품목별 상세 기록)", "간편 양식 (아이디 + 금액, 적립/차감 시트)"],
        horizontal=True,
    )
    st.divider()

    # ------------------------------------------------------------------
    # 모드 A: 기존 상세 양식
    # ------------------------------------------------------------------
    if excel_mode == "상세 양식 (품목별 상세 기록)":
        uploaded_file = st.file_uploader("📂 엑셀 파일 업로드 (상세 양식)", type=["xlsx", "xls", "csv"], key="detail_uploader")

        if uploaded_file:
            reset_state_if_new_file(uploaded_file, "detail")
            try:
                df = pd.read_excel(uploaded_file) if uploaded_file.name.endswith(('xlsx', 'xls')) else pd.read_csv(uploaded_file)
                df.columns = df.columns.astype(str).str.strip()

                amt_col = next((n for n in ['적립금액', '적립금', '금액', '결제금액'] if n in df.columns), None)
                date_col = next((n for n in ['주문일', '주문일시', '날짜'] if n in df.columns), None)

                missing_req = [c for c in REQUIRED_COLS if c not in df.columns]
                if missing_req:
                    st.error(f"⚠️ 엑셀 파일에서 필수 컬럼을 찾을 수 없습니다: {', '.join(missing_req)}")
                    st.stop()
                if not date_col:
                    st.error("⚠️ 엑셀 파일에서 '주문일' 컬럼을 찾을 수 없습니다.")
                    st.stop()
                if not amt_col:
                    st.error("⚠️ 엑셀 파일에서 금액 관련 컬럼(적립금액/적립금/금액/결제금액)을 찾을 수 없습니다.")
                    st.stop()

                target_df = df[REQUIRED_COLS + [date_col, amt_col]].copy()
                target_df.columns = REQUIRED_COLS + ['주문일', '금액']
                target_df['금액'] = pd.to_numeric(target_df['금액'], errors='coerce').fillna(0).astype(int)
                target_df['주문일'] = target_df['주문일'].astype(str).str.strip()
                for c in REQUIRED_COLS:
                    target_df[c] = target_df[c].astype(str).str.strip()

                existing_keys = set()
                # DB가 연결되어 있을 때만 중복 체크 실행
                if st.session_state['db_connected'] and engine is not None:
                    try:
                        db_df = pd.read_sql(f"SELECT {', '.join(REQUIRED_COLS)}, 주문일, 금액 FROM mileage_records", con=engine)
                        db_df['금액'] = pd.to_numeric(db_df['금액'], errors='coerce').fillna(0).astype(int)
                        existing_keys = set(db_df.astype(str).apply(lambda x: '|'.join(x.fillna('')), axis=1).tolist())
                    except Exception as e:
                        st.warning(f"⚠️ 중복 체크용 DB 조회에 실패했습니다. 중복 체크 없이 진행합니다. (사유: {e})")

                current_keys = target_df.astype(str).apply(lambda x: '|'.join(x.fillna('')), axis=1)
                target_df['DB상태'] = current_keys.apply(lambda x: '🚨 중복' if x in existing_keys else '✅ 신규/DB없음')
                target_df.insert(0, '삭제선택', False)
                target_df.loc[target_df['DB상태'] == '🚨 중복', '삭제선택'] = True

                duplicate_only = target_df[target_df['DB상태'] == '🚨 중복'].drop(columns=['삭제선택'])
                if not duplicate_only.empty and st.session_state['db_connected']:
                    st.download_button(
                        label=f"📥 중복 데이터 다운로드 ({len(duplicate_only)}건)",
                        data=make_excel_bytes(duplicate_only),
                        file_name="duplicates.xlsx",
                    )

                edited_df = st.data_editor(target_df, hide_index=True, use_container_width=True, key="detail_editor")

                if st.button("🔄 체크 항목 제외 후 합산하기", type="secondary"):
                    cleaned = edited_df[edited_df['삭제선택'] == False].drop(columns=['삭제선택', 'DB상태'])
                    if cleaned.empty:
                        st.warning("⚠️ 합산할 데이터가 없습니다. (모든 항목이 제외되었습니다)")
                    else:
                        st.session_state['detail_cleaned_df'] = cleaned
                        st.session_state['detail_summary_df'] = cleaned.groupby(
                            ['아이디', '주문자명'], as_index=False
                        ).agg({'고객명': 'first', '금액': 'sum'})
                        st.rerun()

                if 'detail_summary_df' in st.session_state:
                    st.divider()
                    s_df = st.session_state['detail_summary_df']
                    c1, c2 = st.columns(2)
                    c1.metric("총 인원", f"{len(s_df)} 명")
                    c2.metric("총 합계", f"{s_df['금액'].sum():,.0f} 원")
                    st.dataframe(s_df, use_container_width=True, hide_index=True)

                    action = st.radio("작업 선택", ["적립금 추가 (지급)", "적립금 차감 (회수)"], key="detail_action")
                    reason = st.text_input("📝 사유 입력", key="detail_reason")

                    b1, b2 = st.columns(2)
                    with b1:
                        if st.session_state['db_connected'] and engine is not None:
                            if st.button("💾 1. 원본 상세 내역을 DB에 기록", use_container_width=True, type="secondary"):
                                st.session_state['detail_db_confirm_step'] = True

                            if st.session_state.get('detail_db_confirm_step'):
                                st.warning("❓ 상세 내역을 DB에 저장하시겠습니까?")
                                cc1, cc2 = st.columns(2)
                                if cc1.button("⭕ 예 (저장)", use_container_width=True, key="detail_save_yes"):
                                    try:
                                        save_df = st.session_state['detail_cleaned_df'].copy()
                                        save_df['비고'] = f"[{action}] {reason if reason.strip() else '상세내역 기록'}"
                                        save_df['지급일시'] = datetime.now()
                                        save_df.to_sql(name='mileage_records', con=engine, if_exists='append', index=False)
                                        st.success("🎉 DB 저장 완료!")
                                    except Exception as e:
                                        st.error(f"🚨 DB 저장 실패: {e}")
                                    st.session_state['detail_db_confirm_step'] = False
                                if cc2.button("❌ 아니요 (취소)", use_container_width=True, key="detail_save_no"):
                                    st.session_state['detail_db_confirm_step'] = False
                                    st.rerun()
                        else:
                            st.info("💡 DB 연결 해제 모드: 내역이 DB에 저장되지 않습니다.")

                    with b2:
                        if st.button(f"🚀 2. 카페24로 {action} 실행", use_container_width=True, type="primary", key="detail_exec"):
                            if not reason.strip():
                                st.warning("⚠️ 사유를 입력해주세요.")
                            else:
                                success = 0
                                failed_rows = []
                                bar = st.progress(0)
                                for i, (idx, row) in enumerate(s_df.iterrows()):
                                    ok, err = send_points(st.session_state['access_token'], row['아이디'], row['금액'], action, reason)
                                    if ok:
                                        success += 1
                                    else:
                                        failed_rows.append({"아이디": row['아이디'], "주문자명": row['주문자명'], "금액": row['금액'], "오류": err})
                                    bar.progress((i + 1) / len(s_df))

                                st.success(f"🎉 카페24로 {success}/{len(s_df)}건 적립금 처리 완료!")

                                # 처리 결과 로그는 DB 연결 여부와 무관하게 항상 다운로드 제공 (추적성 확보)
                                log_df = s_df.copy()
                                log_df['처리결과'] = log_df.apply(
                                    lambda r: '실패' if r['아이디'] in [f['아이디'] for f in failed_rows] else '성공', axis=1
                                )
                                log_df['구분'] = action
                                log_df['사유'] = reason
                                log_df['처리시각'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                                st.download_button(
                                    "📥 처리 결과 로그 다운로드", data=make_excel_bytes(log_df), file_name="처리결과_로그.xlsx"
                                )

                                if failed_rows:
                                    st.error(f"⚠️ {len(failed_rows)}건 실패했습니다. 아래에서 확인하세요.")
                                    fail_df = pd.DataFrame(failed_rows)
                                    st.dataframe(fail_df, use_container_width=True, hide_index=True)
                                    st.download_button(
                                        "📥 실패 목록 다운로드", data=make_excel_bytes(fail_df), file_name="실패목록.xlsx", key="detail_fail_dl"
                                    )

            except Exception as e:
                st.error(f"오류: {e}")

    # ------------------------------------------------------------------
    # 모드 B: 신규 간편 양식 (아이디 + 금액, 적립/차감 시트)
    # ------------------------------------------------------------------
    else:
        st.caption("엑셀에 '아이디', '금액' 컬럼이 있으면 되고, 시트명이 '적립' / '차감'이면 자동으로 지급/차감을 구분합니다. (예: 첨부해주신 0713적립금관리.xlsx 형식)")
        uploaded_file2 = st.file_uploader("📂 엑셀 파일 업로드 (간편 양식)", type=["xlsx", "xls"], key="simple_uploader")

        if uploaded_file2:
            reset_state_if_new_file(uploaded_file2, "simple")
            try:
                raw_df = load_simple_format(uploaded_file2)
                if raw_df is None or raw_df.empty:
                    st.error("⚠️ '아이디'와 '금액' 컬럼을 가진 시트를 찾을 수 없습니다. 파일 양식을 확인해주세요.")
                    st.stop()

                # 시트명만으로 적립/차감을 판별할 수 없는 경우, 사용자에게 직접 확인
                unresolved_sheets = sorted(raw_df.loc[raw_df['구분'].isna(), '시트명'].unique().tolist())
                if unresolved_sheets:
                    st.warning("⚠️ 아래 시트는 이름만으로 적립/차감 여부를 알 수 없습니다. 직접 선택해주세요.")
                    for sh in unresolved_sheets:
                        choice = st.selectbox(f"시트 '{sh}' 는 무엇인가요?", ["적립", "차감"], key=f"simple_sheet_choice_{sh}")
                        raw_df.loc[raw_df['시트명'] == sh, '구분'] = choice

                # 아이디+구분 기준으로 합산 (같은 아이디가 여러 행에 걸쳐 있어도 안전하게 처리)
                grouped = raw_df.groupby(['아이디', '구분'], as_index=False).agg({
                    '주문자명': 'first',
                    '금액': 'sum',
                    '사유': lambda x: ' / '.join(sorted(set([s for s in x if s and str(s).lower() != 'nan']))),
                    '날짜': 'first',
                })
                grouped = grouped[grouped['금액'] != 0]
                if grouped.empty:
                    st.error("⚠️ 유효한 금액이 있는 행이 없습니다.")
                    st.stop()

                # --- 중복 체크 (DB 연결 시, 아이디+날짜+금액+구분 기준) ---
                # 비고에 "[적립]", "[차감]", 혹은 상세양식의 "[적립금 추가 (지급)]" 등이 남아있으므로
                # 문자열에 '적립'/'차감'이 포함되는지로 구분을 역추정해 두 양식 간에도 중복을 잡아낸다.
                existing_keys = set()
                if st.session_state['db_connected'] and engine is not None:
                    try:
                        db_df = pd.read_sql("SELECT 아이디, 주문일, 금액, 비고 FROM mileage_records", con=engine)
                        db_df['아이디'] = db_df['아이디'].astype(str).str.strip()
                        db_df['주문일'] = db_df['주문일'].astype(str).str.strip()
                        db_df['금액'] = pd.to_numeric(db_df['금액'], errors='coerce').fillna(0).astype(int)

                        def _extract_action(remark):
                            # 주의: "적립금 차감 (회수)"처럼 '차감' 문구 안에도 '적립'이라는 글자가
                            # 포함되어 있으므로(적립금), 반드시 '차감'을 먼저 검사해야 한다.
                            s = str(remark)
                            if '차감' in s:
                                return '차감'
                            if '적립' in s:
                                return '적립'
                            return ''

                        db_df['구분_추정'] = db_df['비고'].apply(_extract_action)
                        existing_keys = set(
                            (db_df['아이디'] + '|' + db_df['주문일'] + '|' + db_df['금액'].astype(str) + '|' + db_df['구분_추정']).tolist()
                        )
                    except Exception as e:
                        st.warning(f"⚠️ 중복 체크용 DB 조회에 실패했습니다. 중복 체크 없이 진행합니다. (사유: {e})")

                current_keys = (
                    grouped['아이디'].astype(str).str.strip() + '|' +
                    grouped['날짜'].astype(str).str.strip() + '|' +
                    grouped['금액'].astype(int).astype(str) + '|' +
                    grouped['구분'].astype(str)
                )
                grouped['DB상태'] = current_keys.apply(lambda x: '🚨 중복' if x in existing_keys else '✅ 신규/DB없음')
                grouped.insert(0, '실행선택', True)
                grouped.loc[grouped['DB상태'] == '🚨 중복', '실행선택'] = False

                duplicate_only = grouped[grouped['DB상태'] == '🚨 중복'].drop(columns=['실행선택'])
                if not duplicate_only.empty and st.session_state['db_connected']:
                    st.download_button(
                        label=f"📥 중복 데이터 다운로드 ({len(duplicate_only)}건)",
                        data=make_excel_bytes(duplicate_only),
                        file_name="간편양식_duplicates.xlsx",
                    )

                st.divider()
                c1, c2, c3, c4 = st.columns(4)
                c1.metric("총 건수", f"{len(grouped)} 건")
                c2.metric("적립 합계", f"{grouped.loc[grouped['구분']=='적립','금액'].sum():,.0f} 원")
                c3.metric("차감 합계", f"{grouped.loc[grouped['구분']=='차감','금액'].sum():,.0f} 원")
                c4.metric("🚨 중복 건수", f"{len(duplicate_only)} 건")

                edited = st.data_editor(grouped, hide_index=True, use_container_width=True, key="simple_editor")
                common_reason = st.text_input("📝 공통 사유 (엑셀에 개별 사유가 없는 행에 사용됩니다)", key="simple_common_reason")

                run_df = edited[edited['실행선택'] == True].drop(columns=['실행선택', 'DB상태'])
                st.caption(f"실행 대상: {len(run_df)}건")

                b1, b2 = st.columns(2)
                with b1:
                    if st.session_state['db_connected'] and engine is not None:
                        if st.button("💾 1. 내역을 DB에 기록", use_container_width=True, type="secondary", key="simple_db_btn"):
                            st.session_state['simple_db_confirm_step'] = True

                        if st.session_state.get('simple_db_confirm_step'):
                            st.warning("❓ 위 내역을 DB에 저장하시겠습니까?")
                            cc1, cc2 = st.columns(2)
                            if cc1.button("⭕ 예 (저장)", use_container_width=True, key="simple_save_yes"):
                                try:
                                    save_df = pd.DataFrame({
                                        '아이디': run_df['아이디'],
                                        '주문자명': run_df['주문자명'],
                                        '고객명': run_df['주문자명'],
                                        '브랜드': '',
                                        '상품': '',
                                        '색상': '',
                                        '사이즈': '',
                                        '주문일': run_df['날짜'],
                                        '금액': run_df['금액'],
                                        '비고': run_df.apply(
                                            lambda r: f"[{r['구분']}] {r['사유'] if r['사유'] else (common_reason or '간편양식 기록')}", axis=1
                                        ),
                                        '지급일시': datetime.now(),
                                    })
                                    save_df.to_sql(name='mileage_records', con=engine, if_exists='append', index=False)
                                    st.success("🎉 DB 저장 완료!")
                                except Exception as e:
                                    st.error(f"🚨 DB 저장 실패: {e}")
                                st.session_state['simple_db_confirm_step'] = False
                            if cc2.button("❌ 아니요 (취소)", use_container_width=True, key="simple_save_no"):
                                st.session_state['simple_db_confirm_step'] = False
                                st.rerun()
                    else:
                        st.info("💡 DB 연결 해제 모드: 내역이 DB에 저장되지 않습니다.")

                with b2:
                    if st.button("🚀 2. 카페24로 일괄 실행 (적립/차감 자동 구분)", use_container_width=True, type="primary", key="simple_exec"):
                        if run_df.empty:
                            st.warning("⚠️ 실행할 항목이 없습니다.")
                        else:
                            success = 0
                            failed_rows = []
                            bar = st.progress(0)
                            for i, (idx, row) in enumerate(run_df.iterrows()):
                                reason = row['사유'] if row['사유'] else common_reason
                                if not str(reason).strip():
                                    failed_rows.append({"아이디": row['아이디'], "주문자명": row['주문자명'], "금액": row['금액'], "구분": row['구분'], "오류": "사유 없음 (건너뜀)"})
                                    bar.progress((i + 1) / len(run_df))
                                    continue
                                ok, err = send_points(st.session_state['access_token'], row['아이디'], row['금액'], row['구분'], reason)
                                if ok:
                                    success += 1
                                else:
                                    failed_rows.append({"아이디": row['아이디'], "주문자명": row['주문자명'], "금액": row['금액'], "구분": row['구분'], "오류": err})
                                bar.progress((i + 1) / len(run_df))

                            st.success(f"🎉 카페24로 {success}/{len(run_df)}건 처리 완료!")

                            log_df = run_df.copy()
                            failed_ids = {f['아이디'] for f in failed_rows}
                            log_df['처리결과'] = log_df['아이디'].apply(lambda x: '실패' if x in failed_ids else '성공')
                            log_df['처리시각'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                            st.download_button(
                                "📥 처리 결과 로그 다운로드", data=make_excel_bytes(log_df), file_name="간편양식_처리결과_로그.xlsx"
                            )

                            if failed_rows:
                                st.error(f"⚠️ {len(failed_rows)}건 실패/건너뜀. 아래에서 확인하세요.")
                                fail_df = pd.DataFrame(failed_rows)
                                st.dataframe(fail_df, use_container_width=True, hide_index=True)
                                st.download_button(
                                    "📥 실패 목록 다운로드", data=make_excel_bytes(fail_df), file_name="간편양식_실패목록.xlsx", key="simple_fail_dl"
                                )

            except Exception as e:
                st.error(f"오류: {e}")

# ==========================================
# 화면 2 & 3: DB 관련 화면 (DB 없을 시 차단)
# ==========================================
elif menu in ["기록 조회 및 다운로드", "DB 기록 삭제"]:
    if not st.session_state['db_connected'] or engine is None:
        st.warning("⚠️ 이 기능은 DB에 연결된 상태에서만 사용할 수 있습니다. 좌측 사이드바에서 [DB 연결 시도하기]를 눌러주세요.")
        st.stop()

    if menu == "기록 조회 및 다운로드":
        st.title("🔍 DB 기록 조회 및 다운로드")
        try:
            raw_df = pd.read_sql("SELECT * FROM mileage_records ORDER BY 지급일시 DESC", con=engine)
        except Exception as e:
            st.error(f"🚨 DB 조회 실패: {e}")
            st.stop()

        if raw_df.empty:
            st.info("기록이 없습니다.")
        else:
            c1, c2, c3 = st.columns(3)
            sid, sname, srs = c1.text_input("아이디"), c2.text_input("이름"), c3.text_input("사유")
            f_df = raw_df.copy()
            if sid:
                f_df = f_df[f_df['아이디'].str.contains(sid, na=False)]
            if sname:
                f_df = f_df[f_df['주문자명'].str.contains(sname, na=False)]
            if srs:
                f_df = f_df[f_df['비고'].str.contains(srs, na=False)]
            st.dataframe(f_df, use_container_width=True, hide_index=True)
            if not f_df.empty:
                st.download_button(label="📥 결과 다운로드", data=make_excel_bytes(f_df), file_name="history.xlsx")
            else:
                st.info("검색 조건에 맞는 기록이 없습니다.")

    elif menu == "DB 기록 삭제":
        st.title("🗑️ DB 기록 삭제 (묶음별)")
        try:
            total_count = int(
                pd.read_sql("SELECT COUNT(*) AS cnt FROM mileage_records", con=engine).iloc[0]["cnt"]
            )

            q = """
                SELECT DATE(지급일시) AS 날짜, 비고, COUNT(*) AS 건수
                FROM mileage_records
                GROUP BY DATE(지급일시), 비고
                ORDER BY 날짜 DESC
            """
            gs = pd.read_sql(q, con=engine)

            m1, m2 = st.columns(2)
            m1.metric("📊 DB 전체 건수", f"{total_count:,}건")
            m2.metric("📦 삭제 묶음 수", f"{len(gs):,}개")

            if gs.empty:
                st.info("삭제할 데이터가 없습니다.")
            else:
                gs = gs.reset_index(drop=True)
                gs["날짜_str"] = pd.to_datetime(gs["날짜"]).dt.strftime("%Y-%m-%d")
                gs["opt"] = (
                    gs["날짜_str"]
                    + " | "
                    + gs["비고"].astype(str)
                    + " ("
                    + gs["건수"].astype(str)
                    + "건)"
                )

                selected_idx = st.selectbox(
                    "삭제할 묶음 선택",
                    options=range(len(gs)),
                    format_func=lambda i: gs.loc[i, "opt"],
                )
                row = gs.loc[selected_idx]
                s_date = row["날짜_str"]
                s_reason = str(row["비고"])

                st.caption(f"선택 확인 → 날짜: {s_date} | 비고: {s_reason} | {row['건수']}건")

                if st.button("🧨 선택 데이터 삭제", type="primary"):
                    with engine.begin() as conn:
                        result = conn.execute(
                            text(
                                "DELETE FROM mileage_records "
                                "WHERE DATE(지급일시) = :d AND 비고 = :r"
                            ),
                            {"d": s_date, "r": s_reason},
                        )
                        deleted = result.rowcount

                    if deleted > 0:
                        st.success(f"✅ {deleted}건 삭제 완료!")
                        st.rerun()
                    else:
                        st.error(
                            "❌ 삭제된 데이터가 없습니다. "
                            "날짜/비고 조건이 일치하지 않습니다."
                        )
        except Exception as e:
            st.error(f"오류: {e}")
