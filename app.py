import streamlit as st
import pandas as pd
from sqlalchemy import create_engine, text
import requests
import base64
import time
import urllib.parse
import io
from datetime import datetime

# --- [페이지 설정] ---
st.set_page_config(page_title="카페24 적립금 통합 관리 시스템", layout="wide")

# --- [DB 연결 및 초기화] ---
@st.cache_resource
def init_connection():
    try:
        db_info = st.secrets["mysql"]
        return create_engine(f"mysql+pymysql://{db_info['user']}:{db_info['password']}@{db_info['host']}:{db_info['port']}/{db_info['database']}?charset=utf8mb4")
    except Exception as e:
        st.error(f"❌ DB 연결 설정 오류: {e}")
        st.stop()

engine = init_connection()

def prepare_db():
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS mileage_records (
        id INT AUTO_INCREMENT PRIMARY KEY,
        아이디 VARCHAR(255),
        주문자명 VARCHAR(255),
        고객명 VARCHAR(255),
        브랜드 VARCHAR(255),
        상품 TEXT,
        색상 VARCHAR(100),
        사이즈 VARCHAR(100),
        금액 INT,
        비고 TEXT,
        지급일시 DATETIME DEFAULT CURRENT_TIMESTAMP
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """
    with engine.connect() as conn:
        conn.execute(text(create_table_sql))
        conn.commit()

prepare_db()

# --- [카페24 설정 정보 정보] ---
try:
    cafe24_info = st.secrets["cafe24"]
    MALL_ID = cafe24_info["mall_id"]
    CLIENT_ID = cafe24_info["client_id"]
    CLIENT_SECRET = cafe24_info["client_secret"]
except Exception as e:
    st.error(f"❌ Streamlit Secrets에서 [cafe24] 섹션을 확인해주세요: {e}")
    st.stop()

REDIRECT_URI = "https://cafe24-mileage-app.streamlit.app"
SCOPE = "mall.read_customer,mall.write_customer,mall.read_mileage,mall.write_mileage"

# --- [토큰 발급 함수] ---
def get_access_token(auth_code):
    url = f"https://{MALL_ID}.cafe24api.com/api/v2/oauth/token"
    auth_str = f"{CLIENT_ID}:{CLIENT_SECRET}"
    b64_auth = base64.b64encode(auth_str.encode('utf-8')).decode('utf-8')
    headers = {
        "Authorization": f"Basic {b64_auth}",
        "Content-Type": "application/x-www-form-urlencoded"
    }
    data = {
        "grant_type": "authorization_code",
        "code": auth_code,
        "redirect_uri": REDIRECT_URI
    }
    
    try:
        response = requests.post(url, headers=headers, data=data)
        if response.status_code == 200:
            return response.json().get("access_token"), None
        else:
            return None, f"상태코드 {response.status_code}: {response.text}"
    except Exception as e:
        return None, str(e)

# --- [사이드바 메뉴] ---
st.sidebar.title("🚀 메뉴 선택")
menu = st.sidebar.radio("원하시는 작업을 선택하세요", ["적립금 지급하기", "기록 조회 및 다운로드", "DB 기록 삭제"])

# ==========================================
# 화면 1: 적립금 지급하기
# ==========================================
if menu == "적립금 지급하기":
    st.title("💰 적립금 자동 지급/차감 시스템")
    
    # 🔍 [인증 로직] 주소창의 code 파라미터를 실시간 감시
    current_params = st.query_params
    
    if "access_token" not in st.session_state:
        if "code" in current_params:
            auth_code = current_params["code"]
            with st.spinner("🔄 카페24 서버와 최종 연결 중..."):
                token, err_detail = get_access_token(auth_code)
                if token:
                    st.session_state["access_token"] = token
                    st.query_params.clear() # 주소창 정리
                    st.rerun()
                else:
                    st.error("❌ 카페24 연동 토큰 발급에 실패했습니다.")
                    with st.expander("🛠️ 상세 에러 원인 확인 (카페24 응답)"):
                        st.code(err_detail)
                    if st.button("처음부터 다시 시도"):
                        st.query_params.clear()
                        st.rerun()
                    st.stop()
        else:
            # 로그인 전 화면
            st.info(f"현재 접속 대상 쇼핑몰 ID: **{MALL_ID}**")
            auth_url = f"https://{MALL_ID}.cafe24api.com/api/v2/oauth/authorize?response_type=code&client_id={CLIENT_ID}&state=random&redirect_uri={urllib.parse.quote(REDIRECT_URI)}&scope={SCOPE}"
            st.link_button("🔐 카페24 로그인 및 연동하기", auth_url, type="primary")
            st.stop()
    else:
        st.success(f"✅ {MALL_ID} 계정과 연결되었습니다!")
        if st.button("🚪 연동 해제 (로그아웃)"):
            del st.session_state["access_token"]
            st.rerun()

    # --- 엑셀 업로드 및 처리 로직 ---
    st.divider()
    uploaded_file = st.file_uploader("📂 엑셀 파일 업로드", type=["xlsx", "xls", "csv"])

    if uploaded_file:
        try:
            df = pd.read_excel(uploaded_file) if uploaded_file.name.endswith(('xlsx', 'xls')) else pd.read_csv(uploaded_file)
            df.columns = df.columns.astype(str).str.strip()
            amt_col = next((n for n in ['적립금액', '적립금', '금액', '결제금액'] if n in df.columns), None)
            req_cols = ['아이디', '주문자명', '고객명', '브랜드', '상품', '색상', '사이즈']
            
            target_df = df[req_cols + [amt_col]].copy()
            target_df.columns = req_cols + ['금액']
            target_df['금액'] = pd.to_numeric(target_df['금액'], errors='coerce').fillna(0)

            # 중복 체크
            try:
                db_df = pd.read_sql(f"SELECT {', '.join(req_cols)}, 금액 FROM mileage_records", con=engine)
                existing_keys = set(db_df.astype(str).apply(lambda x: '|'.join(x.fillna('')), axis=1).tolist())
            except:
                existing_keys = set()
            
            current_keys = target_df.astype(str).apply(lambda x: '|'.join(x.fillna('')), axis=1)
            target_df['DB상태'] = current_keys.apply(lambda x: '🚨 중복' if x in existing_keys else '✅ 신규')
            target_df.insert(0, '삭제선택', False)
            target_df.loc[target_df['DB상태'] == '🚨 중복', '삭제선택'] = True
            
            # 중복 데이터 다운로드
            duplicate_only = target_df[target_df['DB상태'] == '🚨 중복'].drop(columns=['삭제선택'])
            if not duplicate_only.empty:
                dup_out = io.BytesIO()
                with pd.ExcelWriter(dup_out, engine='xlsxwriter') as writer:
                    duplicate_only.to_excel(writer, index=False)
                st.download_button(label=f"📥 중복 데이터 다운로드 ({len(duplicate_only)}건)", data=dup_out.getvalue(), file_name="duplicates.xlsx")

            edited_df = st.data_editor(target_df, hide_index=True, use_container_width=True)

            if st.button("🔄 체크 항목 제외 후 합산하기", type="secondary"):
                cleaned = edited_df[edited_df['삭제선택'] == False].drop(columns=['삭제선택', 'DB상태'])
                st.session_state['cleaned_df'] = cleaned
                st.session_state['summary_df'] = cleaned.groupby(['아이디', '주문자명'], as_index=False).agg({'고객명': 'first', '금액': 'sum'})
                st.rerun()

            if 'summary_df' in st.session_state:
                st.divider()
                s_df = st.session_state['summary_df']
                c1, c2 = st.columns(2)
                c1.metric("총 인원", f"{len(s_df)} 명")
                c2.metric("총 합계", f"{s_df['금액'].sum():,.0f} 원")
                st.dataframe(s_df, use_container_width=True, hide_index=True)
                
                action = st.radio("작업 선택", ["적립금 추가 (지급)", "적립금 차감 (회수)"])
                reason = st.text_input("📝 사유 입력 (API 전송 시 필수)")
                
                b1, b2 = st.columns(2)
                with b1:
                    if st.button("💾 1. 원본 상세 내역을 DB에 기록", use_container_width=True, type="primary"):
                        st.session_state['db_confirm'] = True
                    if st.session_state.get('db_confirm'):
                        st.warning("❓ 상세 내역을 DB에 저장하시겠습니까?")
                        cc1, cc2 = st.columns(2)
                        if cc1.button("⭕ 예 (저장)", use_container_width=True):
                            save_df = st.session_state['cleaned_df'].copy()
                            save_df['비고'] = f"[{action}] {reason if reason.strip() else '상세내역 기록'}"
                            save_df['지급일시'] = datetime.now()
                            save_df.to_sql(name='mileage_records', con=engine, if_exists='append', index=False)
                            st.success("🎉 DB 저장 완료!")
                            st.session_state['db_confirm'] = False
                        if cc2.button("❌ 아니요 (취소)", use_container_width=True):
                            st.session_state['db_confirm'] = False
                            st.rerun()

                with b2:
                    if st.button(f"🚀 2. 카페24로 {action} 실행", use_container_width=True, type="primary"):
                        if not reason.strip(): st.warning("⚠️ 사유를 입력해주세요.")
                        else:
                            url = f"https://{MALL_ID}.cafe24api.com/api/v2/admin/points"
                            headers = {"Authorization": f"Bearer {st.session_state['access_token']}", "Content-Type": "application/json", "X-Cafe24-Api-Version": "2026-03-01"}
                            api_type = "increase" if "추가" in action else "decrease"
                            success = 0
                            bar = st.progress(0)
                            for idx, row in s_df.iterrows():
                                payload = {"request": {"member_id": str(row['아이디']).strip(), "amount": abs(int(row['금액'])), "type": api_type, "reason": reason}}
                                try:
                                    res = requests.post(url, json=payload, headers=headers)
                                    if res.status_code in [200, 201]: success += 1
                                    else: st.error(f"❌ {row['아이디']} 실패: {res.text}")
                                except: pass
                                bar.progress((idx + 1) / len(s_df))
                            st.success(f"🎉 {success}건 처리 완료!")
                            del st.session_state["access_token"]
        except Exception as e: st.error(f"오류: {e}")

# ==========================================
# 화면 2: 기록 조회 및 다운로드
# ==========================================
elif menu == "기록 조회 및 다운로드":
    st.title("🔍 DB 기록 조회 및 다운로드")
    try:
        raw_df = pd.read_sql("SELECT * FROM mileage_records ORDER BY 지급일시 DESC", con=engine)
        c1, c2, c3 = st.columns(3)
        sid, sname, srs = c1.text_input("아이디"), c2.text_input("이름"), c3.text_input("사유")
        f_df = raw_df.copy()
        if sid: f_df = f_df[f_df['아이디'].str.contains(sid, na=False)]
        if sname: f_df = f_df[f_df['주문자명'].str.contains(sname, na=False)]
        if srs: f_df = f_df[f_df['비고'].str.contains(srs, na=False)]
        st.dataframe(f_df, use_container_width=True, hide_index=True)
        if not f_df.empty:
            out = io.BytesIO()
            with pd.ExcelWriter(out, engine='xlsxwriter') as w: f_df.to_excel(w, index=False)
            st.download_button(label="📥 결과 다운로드", data=out.getvalue(), file_name="history.xlsx")
    except: st.info("기록이 없습니다.")

# ==========================================
# 화면 3: DB 기록 삭제
# ==========================================
elif menu == "DB 기록 삭제":
    st.title("🗑️ DB 기록 삭제 (묶음별)")
    try:
        q = "SELECT DATE(지급일시) as 날짜, 비고, COUNT(*) as 건수 FROM mileage_records GROUP BY DATE(지급일시), 비고 ORDER BY 날짜 DESC"
        gs = pd.read_sql(q, con=engine)
        if gs.empty: st.info("삭제할 데이터가 없습니다.")
        else:
            gs['opt'] = gs['날짜'].astype(str) + " | " + gs['비고'].astype(str) + " (" + gs['건수'].astype(str) + "건)"
            sel = st.selectbox("삭제할 묶음 선택", gs['opt'].tolist())
            s_date, s_reason = sel.split(" | ")[0], sel.split(" | ")[1].split(" (")[0]
            if st.button("🧨 선택 데이터 삭제", type="primary"):
                with engine.connect() as conn:
                    conn.execute(text("DELETE FROM mileage_records WHERE DATE(지급일시) = :d AND 비고 = :r"), {"d": s_date, "r": s_reason})
                    conn.commit()
                st.success("✅ 삭제 완료!")
                st.rerun()
    except Exception as e: st.error(f"오류: {e}")
