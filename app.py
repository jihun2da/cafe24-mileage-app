import streamlit as st
import pandas as pd
from sqlalchemy import create_engine, text
import requests
import base64
import urllib.parse
import io
import hmac
import hashlib
import time
from datetime import datetime

# --- [페이지 설정] ---
st.set_page_config(page_title="카페24 적립금 통합 관리 시스템", layout="wide")

# ==========================================
# [공용 유틸] 금액 정규화 / 중복 방지 키 생성
#  - DB에는 금액을 INT로 저장하지만, 업로드 엑셀은 pandas가 float으로 읽는 경우가 많아
#    문자열로 이어붙여 중복 키를 만들면 "1000" vs "1000.0" 처럼 서로 달라져
#    실제 중복인데도 신규로 오인식하는 문제가 있었습니다.
#  -> 저장 시점에 정규화된 dedup_key 컬럼을 DB에 함께 저장해두고,
#     조회 시에는 그 값을 그대로 비교합니다.
# ==========================================
DEDUP_COLS = ['아이디', '주문자명', '고객명', '브랜드', '상품', '색상', '사이즈', '주문일']


def normalize_amount(x):
    try:
        return int(round(float(x)))
    except (TypeError, ValueError):
        return 0


def make_dedup_key(row):
    parts = [str(row.get(c, '') or '').strip() for c in DEDUP_COLS]
    parts.append(str(normalize_amount(row.get('금액', 0))))
    return '|'.join(parts)


# ==========================================
# [신규 추가] DB 연결 상태 관리 및 토글 버튼
# ==========================================
if 'db_connected' not in st.session_state:
    # 앱을 처음 켰을 때는 DB 에러 방지를 위해 무조건 '연결 해제' 상태로 시작합니다.
    st.session_state['db_connected'] = False

st.sidebar.title("🔌 시스템 모드")
if st.session_state['db_connected']:
    st.sidebar.success("🟢 DB 연결 모드 (기록 저장 및 조회 가능)")
    if st.sidebar.button("DB 연결 끊기 (API 전용)", use_container_width=True):
        st.session_state['db_connected'] = False
        st.session_state.pop('db_migrated', None)
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
        f"mysql+pymysql://{db_info['user']}:{db_info['password']}@{db_info['host']}:{db_info['port']}/{db_info['database']}?charset=utf8mb4",
        pool_pre_ping=True,
    )


def _column_exists(conn, table, column):
    row = conn.execute(text("""
        SELECT COUNT(*) AS cnt FROM information_schema.COLUMNS
        WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = :t AND COLUMN_NAME = :c
    """), {"t": table, "c": column}).fetchone()
    return row.cnt > 0


def _index_exists(conn, table, index_name):
    row = conn.execute(text("""
        SELECT COUNT(*) AS cnt FROM information_schema.STATISTICS
        WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = :t AND INDEX_NAME = :i
    """), {"t": table, "i": index_name}).fetchone()
    return row.cnt > 0


engine = None
if st.session_state['db_connected']:
    try:
        engine = init_connection()
        # 마이그레이션/점검 로직은 세션당 1회만 실행 (매 rerun마다 반복 실행되는 것을 방지)
        if not st.session_state.get('db_migrated'):
            with engine.begin() as conn:
                # 1. 테이블 생성
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
                # 2. 기존 테이블에 '주문일' 컬럼이 없는 경우 추가
                if not _column_exists(conn, "mileage_records", "주문일"):
                    conn.execute(text("ALTER TABLE mileage_records ADD COLUMN 주문일 VARCHAR(100) AFTER 사이즈;"))
                # 3. 중복 방지용 dedup_key 컬럼이 없는 경우 추가
                if not _column_exists(conn, "mileage_records", "dedup_key"):
                    conn.execute(text("ALTER TABLE mileage_records ADD COLUMN dedup_key VARCHAR(600);"))

                # 4. 기존 데이터 중 dedup_key가 비어있는 행을 채워넣기 (최초 1회성 마이그레이션)
                null_rows = conn.execute(text(
                    "SELECT id, 아이디, 주문자명, 고객명, 브랜드, 상품, 색상, 사이즈, 주문일, 금액 "
                    "FROM mileage_records WHERE dedup_key IS NULL OR dedup_key = ''"
                )).fetchall()
                for r in null_rows:
                    k = make_dedup_key({
                        '아이디': r.아이디, '주문자명': r.주문자명, '고객명': r.고객명, '브랜드': r.브랜드,
                        '상품': r.상품, '색상': r.색상, '사이즈': r.사이즈, '주문일': r.주문일, '금액': r.금액,
                    })
                    conn.execute(text("UPDATE mileage_records SET dedup_key=:k WHERE id=:id"), {"k": k, "id": r.id})

                # 5. 중복 방지용 유니크 인덱스 (기존 데이터에 중복이 있으면 생성이 실패할 수 있어 별도 처리)
                if not _index_exists(conn, "mileage_records", "uq_dedup_key"):
                    try:
                        conn.execute(text(
                            "ALTER TABLE mileage_records ADD UNIQUE INDEX uq_dedup_key (dedup_key);"
                        ))
                    except Exception as idx_err:
                        st.sidebar.info(f"ℹ️ 중복 방지 인덱스는 생성하지 못했습니다 (기존 데이터에 중복 가능성): {idx_err}")

                # 6. 카페24 OAuth 토큰 영속화 테이블 (2시간마다 만료되는 access_token 자동 갱신용)
                conn.execute(text("""
                    CREATE TABLE IF NOT EXISTS cafe24_oauth_token (
                        id INT PRIMARY KEY,
                        access_token TEXT,
                        refresh_token TEXT,
                        issued_at DATETIME
                    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
                """))
            st.session_state['db_migrated'] = True
    except Exception as e:
        # 뭉뚱그려진 에러 대신, 실제 에러 메시지를 출력
        st.sidebar.error(f"🚨 상세 에러: {e}")
        st.session_state['db_connected'] = False
        st.session_state.pop('db_migrated', None)
        engine = None


# --- [카페24 설정 정보] ---
cafe24_info = st.secrets["cafe24"]
MALL_ID = cafe24_info["mall_id"]
CLIENT_ID = cafe24_info["client_id"]
CLIENT_SECRET = cafe24_info["client_secret"]
REDIRECT_URI = "https://cafe24-mileage-app.streamlit.app"
SCOPE = "mall.read_customer,mall.write_customer,mall.read_mileage,mall.write_mileage"

# access_token 수명(분). 카페24 access_token은 발급 후 약 2시간(120분) 뒤 만료되므로
# 여유를 두고 110분이 지나면 만료 전에 미리 자동 갱신합니다.
TOKEN_LIFETIME_MIN = 110


def _basic_auth_header():
    auth_str = f"{CLIENT_ID}:{CLIENT_SECRET}"
    b64_auth = base64.b64encode(auth_str.encode('utf-8')).decode('utf-8')
    return {"Authorization": f"Basic {b64_auth}", "Content-Type": "application/x-www-form-urlencoded"}


def get_access_token(auth_code):
    url = f"https://{MALL_ID}.cafe24api.com/api/v2/oauth/token"
    headers = _basic_auth_header()
    data = {"grant_type": "authorization_code", "code": auth_code, "redirect_uri": REDIRECT_URI}
    try:
        response = requests.post(url, headers=headers, data=data, timeout=15)
    except requests.RequestException as e:
        return None, None, str(e)
    if response.status_code == 200:
        j = response.json()
        return j.get("access_token"), j.get("refresh_token"), None
    return None, None, response.text


def refresh_access_token(refresh_token):
    url = f"https://{MALL_ID}.cafe24api.com/api/v2/oauth/token"
    headers = _basic_auth_header()
    data = {"grant_type": "refresh_token", "refresh_token": refresh_token}
    try:
        response = requests.post(url, headers=headers, data=data, timeout=15)
    except requests.RequestException as e:
        return None, None, str(e)
    if response.status_code == 200:
        j = response.json()
        return j.get("access_token"), j.get("refresh_token"), None
    return None, None, response.text


def _store_token(access_token, refresh_token):
    """세션에 토큰을 저장하고, DB가 연결되어 있으면 영속화하여
    앱이 재시작되거나 세션이 끊겨도 refresh_token으로 자동 갱신할 수 있게 합니다."""
    now = datetime.now()
    st.session_state['access_token'] = access_token
    st.session_state['refresh_token'] = refresh_token
    st.session_state['token_issued_at'] = now
    if st.session_state.get('db_connected') and engine is not None:
        try:
            with engine.begin() as conn:
                conn.execute(text("""
                    INSERT INTO cafe24_oauth_token (id, access_token, refresh_token, issued_at)
                    VALUES (1, :a, :r, :t)
                    ON DUPLICATE KEY UPDATE access_token=:a, refresh_token=:r, issued_at=:t
                """), {"a": access_token, "r": refresh_token, "t": now})
        except Exception as persist_err:
            # 영속화 실패해도 현재 세션 동작은 막지 않되, 원인 파악을 위해 에러는 노출합니다.
            st.sidebar.warning(f"⚠️ 토큰 DB 저장 실패 (현재 세션은 정상 동작): {persist_err}")


def _load_token_from_db():
    if not (st.session_state.get('db_connected') and engine is not None):
        return None
    try:
        with engine.connect() as conn:
            row = conn.execute(text(
                "SELECT access_token, refresh_token, issued_at FROM cafe24_oauth_token WHERE id=1"
            )).fetchone()
        if row and row.access_token:
            return row.access_token, row.refresh_token, row.issued_at
    except Exception as load_err:
        st.sidebar.warning(f"⚠️ 토큰 DB 조회 실패 (재로그인이 필요할 수 있습니다): {load_err}")
    return None


def ensure_valid_token():
    """항상 유효한 access_token을 보장합니다.
    세션에 토큰이 없으면 DB에서 복구를 시도하고, 만료가 임박했으면 refresh_token으로 자동 갱신합니다."""
    if 'access_token' not in st.session_state:
        loaded = _load_token_from_db()
        if loaded:
            st.session_state['access_token'], st.session_state['refresh_token'], st.session_state['token_issued_at'] = loaded
        else:
            return False

    issued_at = st.session_state.get('token_issued_at')
    needs_refresh = True
    if issued_at:
        elapsed_min = (datetime.now() - issued_at).total_seconds() / 60
        needs_refresh = elapsed_min >= TOKEN_LIFETIME_MIN

    if needs_refresh and st.session_state.get('refresh_token'):
        new_access, new_refresh, err = refresh_access_token(st.session_state['refresh_token'])
        if new_access:
            _store_token(new_access, new_refresh or st.session_state['refresh_token'])
        else:
            st.session_state.pop('access_token', None)
            st.session_state.pop('refresh_token', None)
            st.session_state.pop('token_issued_at', None)
            st.warning(f"⚠️ 인증 토큰 자동 갱신에 실패했습니다. 다시 로그인해주세요. ({err})")
            return False

    return 'access_token' in st.session_state


# ==========================================
# [수정] OAuth state(CSRF 방지) 값을 세션 상태에 의존하지 않고 생성/검증합니다.
#  - 카페24 로그인 페이지로 이동했다가 redirect_uri로 되돌아오는 과정은
#    브라우저의 완전한 풀 리로드(새 페이지 로드)이기 때문에, 그 사이에
#    Streamlit의 st.session_state는 통째로 초기화됩니다.
#  - 따라서 로그인 이전에 session_state에 저장해둔 state 값과, 로그인 후 돌아왔을 때의
#    session_state를 비교하는 방식은 항상 실패해서 "인증 상태값이 일치하지 않습니다"
#    에러가 반복되고, 로그인 화면을 절대 벗어나지 못하는 무한 루프에 빠지는 버그가 있었습니다.
#  - 세션 없이도 검증 가능하도록 타임스탬프 + HMAC 서명 조합의 stateless state 값을 사용합니다.
# ==========================================
OAUTH_STATE_MAX_AGE_SEC = 600  # state 값 유효 시간(초). 이 시간 내에 로그인을 완료해야 합니다.


def _make_oauth_state():
    ts = str(int(time.time()))
    sig = hmac.new(CLIENT_SECRET.encode('utf-8'), ts.encode('utf-8'), hashlib.sha256).hexdigest()
    return f"{ts}.{sig}"


def _verify_oauth_state(state_value):
    if not state_value or "." not in state_value:
        return False
    ts_str, sig = state_value.split(".", 1)
    expected_sig = hmac.new(CLIENT_SECRET.encode('utf-8'), ts_str.encode('utf-8'), hashlib.sha256).hexdigest()
    if not hmac.compare_digest(sig, expected_sig):
        return False
    try:
        return (time.time() - int(ts_str)) <= OAUTH_STATE_MAX_AGE_SEC
    except ValueError:
        return False


# --- [사이드바 메뉴] ---
st.sidebar.title("🚀 메뉴 선택")
menu = st.sidebar.radio("원하시는 작업을 선택하세요", ["적립금 지급하기", "기록 조회 및 다운로드", "DB 기록 삭제"])

# ==========================================
# 화면 1: 적립금 지급하기
# ==========================================
if menu == "적립금 지급하기":
    st.title("💰 적립금 자동 지급/차감 시스템")

    if "code" in st.query_params and "access_token" not in st.session_state:
        returned_state = st.query_params.get("state")
        if not _verify_oauth_state(returned_state):
            st.error("🚨 인증 요청이 만료되었거나 위조되었을 수 있습니다. 아래 버튼으로 다시 로그인해주세요.")
            st.query_params.clear()
        else:
            token, refresh_token, error_msg = get_access_token(st.query_params["code"])
            if token:
                _store_token(token, refresh_token)
                st.query_params.clear()
                st.rerun()
            else:
                st.error(f"🚨 토큰 발급 실패: {error_msg}")
                st.query_params.clear()

    if not ensure_valid_token():
        auth_url = (
            f"https://{MALL_ID}.cafe24api.com/api/v2/oauth/authorize?response_type=code"
            f"&client_id={CLIENT_ID}&state={_make_oauth_state()}"
            f"&redirect_uri={urllib.parse.quote(REDIRECT_URI)}&scope={SCOPE}"
        )
        # [되돌림] target=_self / target=_top 둘 다 시도했지만, Streamlit Cloud가 앱을
        # iframe으로 감싸 호스팅하는 구조 때문에 실패했습니다.
        #  - target=_self: iframe 안에서만 이동되어, 카페24 로그인 후 앱으로 되돌아오는 과정이
        #    iframe 내에서 꼬여 로그인 화면으로 계속 되돌아가는 문제가 있었습니다.
        #  - target=_top: iframe의 sandbox 정책이 최상위 창 이동 자체를 차단해 버튼이
        #    아예 반응하지 않는 문제가 있었습니다.
        # 결론적으로 Streamlit Cloud 환경에서는 새 창(st.link_button, target=_blank)으로 여는
        # 방식만 안정적으로 동작하여 원래 방식으로 되돌립니다. (새 창이 뜨는 점은 감수합니다)
        st.link_button("🔐 카페24 로그인 및 연동하기", auth_url, type="primary")
        st.stop()
    else:
        st.success(f"✅ {MALL_ID} 연결 성공! (토큰 만료 시 자동 갱신됩니다)")

    uploaded_file = st.file_uploader("📂 엑셀 파일 업로드", type=["xlsx", "xls", "csv"])

    if uploaded_file:
        # 새 파일이 업로드되면 이전 파일의 합산/편집 상태가 남아있지 않도록 초기화
        file_id = f"{uploaded_file.name}_{uploaded_file.size}"
        if st.session_state.get('last_file_id') != file_id:
            for k in ['cleaned_df', 'summary_df', 'db_confirm_step']:
                st.session_state.pop(k, None)
            st.session_state['last_file_id'] = file_id

        try:
            df = pd.read_excel(uploaded_file) if uploaded_file.name.endswith(('xlsx', 'xls')) else pd.read_csv(uploaded_file)
            df.columns = df.columns.astype(str).str.strip()

            amt_col = next((n for n in ['적립금액', '적립금', '금액', '결제금액'] if n in df.columns), None)
            date_col = next((n for n in ['주문일', '주문일시', '날짜'] if n in df.columns), None)
            req_cols = ['아이디', '주문자명', '고객명', '브랜드', '상품', '색상', '사이즈']

            missing_cols = [c for c in req_cols if c not in df.columns]
            if missing_cols:
                st.error(f"⚠️ 엑셀 파일에 다음 필수 컬럼이 없습니다: {', '.join(missing_cols)}")
                st.stop()
            if not amt_col:
                st.error("⚠️ 엑셀 파일에서 금액 컬럼(적립금액/적립금/금액/결제금액 중 하나)을 찾을 수 없습니다.")
                st.stop()
            if not date_col:
                st.error("⚠️ 엑셀 파일에서 '주문일' 컬럼을 찾을 수 없습니다.")
                st.stop()

            target_df = df[req_cols + [date_col, amt_col]].copy()
            target_df.columns = req_cols + ['주문일', '금액']
            target_df['금액'] = target_df['금액'].apply(normalize_amount)
            target_df['주문일'] = target_df['주문일'].astype(str).str.strip()

            # 저장/비교 기준이 되는 정규화된 중복 방지 키를 미리 계산해둠
            current_keys = target_df.apply(make_dedup_key, axis=1)

            existing_keys = set()
            # DB가 연결되어 있을 때만 중복 체크 실행
            if st.session_state['db_connected'] and engine is not None:
                try:
                    db_df = pd.read_sql("SELECT dedup_key FROM mileage_records", con=engine)
                    existing_keys = set(db_df['dedup_key'].dropna().tolist())
                except Exception as db_err:
                    st.warning(f"⚠️ DB 중복 조회 중 오류가 발생해 중복 체크를 건너뜁니다: {db_err}")

            target_df['DB상태'] = current_keys.apply(lambda x: '🚨 DB중복' if x in existing_keys else '✅ 신규')
            # 업로드한 파일 자체에 동일한 행이 여러 번 있는 경우도 표시 (기존에는 감지되지 않던 부분)
            dup_in_file = current_keys.duplicated(keep=False)
            target_df.loc[dup_in_file & (target_df['DB상태'] == '✅ 신규'), 'DB상태'] = '⚠️ 파일내 중복'

            target_df.insert(0, '삭제선택', False)
            target_df.loc[target_df['DB상태'] != '✅ 신규', '삭제선택'] = True

            duplicate_only = target_df[target_df['DB상태'] != '✅ 신규'].drop(columns=['삭제선택'])
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
                reason = st.text_input("📝 사유 입력")

                b1, b2 = st.columns(2)
                with b1:
                    # DB가 연결된 상태에서만 DB 저장 버튼 표시
                    if st.session_state['db_connected'] and engine is not None:
                        if st.button("💾 1. 원본 상세 내역을 DB에 기록", use_container_width=True, type="secondary"):
                            st.session_state['db_confirm_step'] = True

                        if st.session_state.get('db_confirm_step'):
                            st.warning("❓ 상세 내역을 DB에 저장하시겠습니까?")
                            cc1, cc2 = st.columns(2)
                            if cc1.button("⭕ 예 (저장)", use_container_width=True):
                                save_df = st.session_state['cleaned_df'].copy()
                                save_df['금액'] = save_df['금액'].apply(normalize_amount)
                                save_df['비고'] = f"[{action}] {reason if reason.strip() else '상세내역 기록'}"
                                save_df['지급일시'] = datetime.now()
                                save_df['dedup_key'] = save_df.apply(make_dedup_key, axis=1)
                                try:
                                    save_df.to_sql(name='mileage_records', con=engine, if_exists='append', index=False)
                                    st.success("🎉 DB 저장 완료!")
                                except Exception as save_err:
                                    st.error(f"🚨 DB 저장 중 오류 (중복 데이터일 수 있습니다): {save_err}")
                                st.session_state['db_confirm_step'] = False
                            if cc2.button("❌ 아니요 (취소)", use_container_width=True):
                                st.session_state['db_confirm_step'] = False
                                st.rerun()
                    else:
                        st.info("💡 DB 연결 해제 모드: 내역이 DB에 저장되지 않습니다.")

                with b2:
                    if st.button(f"🚀 2. 카페24로 {action} 실행", use_container_width=True, type="primary"):
                        if not reason.strip():
                            st.warning("⚠️ 사유를 입력해주세요.")
                        elif not ensure_valid_token():
                            st.error("🚨 인증 토큰이 유효하지 않습니다. 새로고침 후 다시 로그인해주세요.")
                        else:
                            url = f"https://{MALL_ID}.cafe24api.com/api/v2/admin/points"
                            headers = {
                                "Authorization": f"Bearer {st.session_state['access_token']}",
                                "Content-Type": "application/json",
                                "X-Cafe24-Api-Version": "2026-03-01",
                            }
                            api_type = "increase" if "추가" in action else "decrease"
                            success = 0
                            failed_rows = []
                            bar = st.progress(0)
                            total = len(s_df)
                            for i, (idx, row) in enumerate(s_df.iterrows()):
                                payload = {"request": {
                                    "member_id": str(row['아이디']).strip(),
                                    "amount": abs(normalize_amount(row['금액'])),
                                    "type": api_type,
                                    "reason": reason,
                                }}
                                try:
                                    res = requests.post(url, json=payload, headers=headers, timeout=15)
                                    if res.status_code in (200, 201):
                                        success += 1
                                    else:
                                        failed_rows.append({
                                            "아이디": row['아이디'], "주문자명": row['주문자명'],
                                            "금액": row['금액'], "실패사유": res.text[:300],
                                        })
                                except requests.RequestException as req_err:
                                    failed_rows.append({
                                        "아이디": row['아이디'], "주문자명": row['주문자명'],
                                        "금액": row['금액'], "실패사유": str(req_err),
                                    })
                                bar.progress((i + 1) / total)

                            st.success(f"🎉 카페24로 {success}건 적립금 처리 완료! (실패 {len(failed_rows)}건)")
                            if failed_rows:
                                fail_df = pd.DataFrame(failed_rows)
                                st.error("아래 목록은 처리에 실패했습니다. 사유 확인 후 재시도해주세요.")
                                st.dataframe(fail_df, use_container_width=True, hide_index=True)
                                fbuf = io.BytesIO()
                                with pd.ExcelWriter(fbuf, engine='xlsxwriter') as w:
                                    fail_df.to_excel(w, index=False)
                                st.download_button(label="📥 실패 목록 다운로드", data=fbuf.getvalue(), file_name="failed_points.xlsx")

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
            c1, c2, c3 = st.columns(3)
            sid, sname, srs = c1.text_input("아이디"), c2.text_input("이름"), c3.text_input("사유")
            f_df = raw_df.copy()
            if sid: f_df = f_df[f_df['아이디'].str.contains(sid, na=False)]
            if sname: f_df = f_df[f_df['주문자명'].str.contains(sname, na=False)]
            if srs: f_df = f_df[f_df['비고'].str.contains(srs, na=False)]
            if f_df.empty:
                st.info("조건에 맞는 기록이 없습니다.")
            else:
                st.dataframe(f_df, use_container_width=True, hide_index=True)
                out = io.BytesIO()
                with pd.ExcelWriter(out, engine='xlsxwriter') as w:
                    f_df.to_excel(w, index=False)
                st.download_button(label="📥 결과 다운로드", data=out.getvalue(), file_name="history.xlsx")
        except Exception as e:
            st.error(f"조회 중 오류가 발생했습니다: {e}")

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
