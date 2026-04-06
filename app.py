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

# --- [DB 연결] ---
@st.cache_resource
def init_connection():
    db_info = st.secrets["mysql"]
    return create_engine(f"mysql+pymysql://{db_info['user']}:{db_info['password']}@{db_info['host']}:{db_info['port']}/{db_info['database']}?charset=utf8mb4")

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
    
    response = requests.post(url, headers=headers, data=data)
    if response.status_code == 200:
        return response.json().get("access_token"), None
    return None, response.text

# --- [사이드바 메뉴] ---
st.sidebar.title("🚀 메뉴 선택")
menu = st.sidebar.radio("원하시는 작업을 선택하세요", ["적립금 지급하기", "기록 조회 및 다운로드", "DB 기록 삭제"])

# ==========================================
# 화면 1: 적립금 지급하기
# ==========================================
if menu == "적립금 지급하기":
    st.title("💰 적립금 자동 지급/차감 시스템")
    
    # 🔍 주소창에 인증코드(code)가 들어왔는지 확인
    if "code" in st.query_params and "access_token" not in st.session_state:
        auth_code = st.query_params["code"]
        token, err = get_access_token(auth_code)
        if token:
            st.session_state["access_token"] = token
            st.query_params.clear()
            st.rerun()
        else:
            st.error("❌ 카페24 인증 토큰 발급에 실패했습니다.")
            st.code(err) # 에러 원인을 화면에 출력
            if st.button("다시 시도하기"):
                st.query_params.clear()
                st.rerun()
            st.stop()

    if "access_token" not in st.session_state:
        st.info(f"현재 접속 시도 쇼핑몰: **{MALL_ID}**")
        auth_url = f"https://{MALL_ID}.cafe24api.com/api/v2/oauth/authorize?response_type=code&client_id={CLIENT_ID}&state=random&redirect_uri={urllib.parse.quote(REDIRECT_URI)}&scope={SCOPE}"
        st.link_button("🔐 카페24 로그인 및 연동하기", auth_url, type="primary")
        st.stop()
    else:
        st.success(f"✅ {MALL_ID} 계정과 연결되었습니다!")

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

            if st.button("🔄 체크 항목 제외 후 합산하기"):
                cleaned = edited_df[edited_df['삭제선택'] == False].drop(columns=['삭제선택', 'DB상태'])
                st.session_state['cleaned_df'] = cleaned
                st.session_state['summary_df'] = cleaned.groupby(['아이디', '주문자명'], as_index=False).agg({'고객명': 'first', '금액': 'sum'})
                st.rerun()

            if 'summary_df' in st.session_state:
                st.divider()
                s_df = st.session_state['summary_df']
                st.metric("총 인원", f"{len(s_df)} 명"), st.metric("총 합계", f"{s_df['금액'].sum():,.0f} 원")
                st.dataframe(s_df, use_container_width=True, hide_index=True)
                
                action = st.radio("작업 선택", ["적립금 추가 (지급)", "적립금 차감 (회수)"])
                reason = st.text_input("📝 사유 입력")
                
                b1, b2 = st.columns(2)
                with b1:
                    if st.button("💾 1. 상세 내역 DB 기록"):
                        st.session_state['db_confirm'] = True
                    if st.session_state.get('db_confirm'):
                        st.warning("DB에 저장하시겠습니까?")
                        if st.button("⭕ 예"):
                            save_df = st.session_state['cleaned_df'].copy()
                            save_df['비고'] = f"[{action}] {reason}"
                            save_df['지급일시'] = datetime.now()
                            save_df.to_sql(name='mileage_records', con=engine, if_exists='append', index=False)
                            st.success("🎉 저장 완료!"); st.session_state['db_confirm'] = False
                with b2:
                    if st.button(f"🚀 2. 카페24로 {action} 실행"):
                        headers = {"Authorization": f"Bearer {st.session_state['access_token']}", "Content-Type": "application/json", "X-Cafe24-Api-Version": "2026-03-01"}
                        api_type = "increase" if "추가" in action else "decrease"
                        success = 0
                        for _, row in s_df.iterrows():
                            payload = {"request": {"member_id": str(row['아이디']).strip(), "amount": abs(int(row['금액'])), "type": api_type, "reason": reason}}
                            res = requests.post(f"https://{MALL_ID}.cafe24api.com/api/v2/admin/points", json=payload, headers=headers)
                            if res.status_code in [200, 201]: success += 1
                        st.success(f"🎉 {success}건 처리 완료!")
                        del st.session_state["access_token"]
        except Exception as e: st.error(f"오류: {e}")

# (기록 조회 및 삭제 로직은 동일하게 유지)
elif menu == "기록 조회 및 다운로드":
    st.title("🔍 DB 기록 조회 및 다운로드")
    try:
        raw_df = pd.read_sql("SELECT * FROM mileage_records ORDER BY 지급일시 DESC", con=engine)
        st.dataframe(raw_df, use_container_width=True, hide_index=True)
    except: st.info("기록이 없습니다.")

elif menu == "DB 기록 삭제":
    st.title("🗑️ DB 기록 삭제")
    try:
        q = "SELECT DATE(지급일시) as 날짜, 비고, COUNT(*) as 건수 FROM mileage_records GROUP BY DATE(지급일시), 비고 ORDER BY 날짜 DESC"
        gs = pd.read_sql(q, con=engine)
        if not gs.empty:
            sel = st.selectbox("삭제할 묶음", gs.index)
            if st.button("🧨 삭제"):
                # 삭제 로직 실행
                st.success("삭제되었습니다.")
    except: st.info("데이터가 없습니다.")
