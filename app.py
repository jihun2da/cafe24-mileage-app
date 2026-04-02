import streamlit as st
import pandas as pd
from sqlalchemy import create_engine
import requests
import base64
import time
import urllib.parse

# --- [페이지 설정] ---
st.set_page_config(page_title="카페24 적립금 자동 지급 시스템", layout="wide")
st.title("💰 적립금 자동 지급/차감 시스템")

# --- [DB 및 카페24 초기 설정] ---
@st.cache_resource
def init_connection():
    db_info = st.secrets["mysql"]
    return create_engine(f"mysql+pymysql://{db_info['user']}:{db_info['password']}@{db_info['host']}:{db_info['port']}/{db_info['database']}")

engine = init_connection()

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
    headers = {
        "Authorization": f"Basic {b64_auth}",
        "Content-Type": "application/x-www-form-urlencoded"
    }
    data = {
        "grant_type": "authorization_code",
        "code": auth_code,
        "redirect_uri": REDIRECT_URI
    }
    response = requests.post(url, headers=headers, data=data)
    if response.status_code == 200:
        return response.json().get("access_token"), None
    else:
        return None, response.text 

# ==========================================
# STEP 1: 카페24 연동
# ==========================================
st.header("🔑 STEP 1: 카페24 계정 연동")

if "code" in st.query_params and "access_token" not in st.session_state:
    auth_code = st.query_params["code"]
    with st.spinner("🔄 카페24 인증 중..."):
        token, error_msg = get_access_token(auth_code)
        if token:
            st.session_state["access_token"] = token
            st.query_params.clear()
            st.rerun()
        else:
            st.error(f"❌ 토큰 발급 실패: {error_msg}")
            st.stop()

if "access_token" not in st.session_state:
    auth_url = f"https://{MALL_ID}.cafe24api.com/api/v2/oauth/authorize?response_type=code&client_id={CLIENT_ID}&state=random&redirect_uri={urllib.parse.quote(REDIRECT_URI)}&scope={SCOPE}"
    st.link_button("🔐 카페24 로그인 및 연동하기", auth_url, type="primary")
    st.stop()
else:
    st.success("✅ 카페24 시스템과 연결되었습니다!")

# ==========================================
# STEP 2: 엑셀 파일 업로드
# ==========================================
st.divider()
st.header("📂 STEP 2: 엑셀 파일 업로드 및 정밀 중복 체크")
uploaded_file = st.file_uploader("처리할 엑셀 파일을 업로드하세요", type=["xlsx", "xls", "csv"])

if uploaded_file:
    try:
        try:
            df = pd.read_excel(uploaded_file)
        except:
            uploaded_file.seek(0)
            df = pd.read_csv(uploaded_file)
            
        df.columns = df.columns.astype(str).str.strip()
        amt_col_name = next((name for name in ['적립금액', '적립금', '금액', '결제금액'] if name in df.columns), None)
        required_cols = ['아이디', '주문자명', '고객명', '브랜드', '상품', '색상', '사이즈']
        
        if not amt_col_name:
            st.error("❌ '적립금액' 열을 찾을 수 없습니다.")
            st.stop()

        target_df = df[required_cols + [amt_col_name]].copy()
        target_df.columns = ['아이디', '주문자명', '고객명', '브랜드', '상품', '색상', '사이즈', '금액']
        target_df = target_df.dropna(subset=['아이디'])
        target_df['금액'] = pd.to_numeric(target_df['금액'], errors='coerce').fillna(0)

        # 중복 체크
        try:
            db_df = pd.read_sql("SELECT 주문자명, 고객명, 브랜드, 상품, 색상, 사이즈, 금액 FROM mileage_records", con=engine)
            db_df['비교키'] = db_df['주문자명'].astype(str) + "|" + db_df['고객명'].astype(str) + "|" + db_df['브랜드'].astype(str) + "|" + db_df['상품'].astype(str) + "|" + db_df['색상'].astype(str) + "|" + db_df['사이즈'].astype(str) + "|" + db_df['금액'].astype(str)
            existing_keys = set(db_df['비교키'].tolist())
        except:
            existing_keys = set()
        
        target_df['비교키'] = target_df['주문자명'].astype(str) + "|" + target_df['고객명'].astype(str) + "|" + target_df['브랜드'].astype(str) + "|" + target_df['상품'].astype(str) + "|" + target_df['색상'].astype(str) + "|" + target_df['사이즈'].astype(str) + "|" + target_df['금액'].astype(str)
        target_df['DB상태'] = target_df['비교키'].apply(lambda x: '🚨 중복(DB존재)' if x in existing_keys else '✅ 신규')
        target_df = target_df.drop(columns=['비교키'])
        target_df.insert(0, '삭제선택', False)
        target_df.loc[target_df['DB상태'] == '🚨 중복(DB존재)', '삭제선택'] = True

        edited_raw_df = st.data_editor(target_df, hide_index=True, use_container_width=True)

        if st.button("🔄 체크된 항목 제외하고 안전하게 합산하기", type="secondary"):
            cleaned_df = edited_raw_df[edited_raw_df['삭제선택'] == False].drop(columns=['삭제선택', 'DB상태'])
            st.session_state['cleaned_df'] = cleaned_df
            summary_df = cleaned_df.groupby(['아이디', '주문자명'], as_index=False).agg({'고객명': 'first', '금액': 'sum'})
            summary_df = summary_df[summary_df['금액'] != 0] # 0원은 제외
            st.session_state['summary_df'] = summary_df
            st.rerun()

        # ==========================================
        # STEP 3: 최종 내역 확인 및 추가/차감 전송
        # ==========================================
        if 'summary_df' in st.session_state:
            st.divider()
            st.header("STEP 3: 최종 확인 및 카페24 전송")
            st.dataframe(st.session_state['summary_df'], use_container_width=True, hide_index=True)
            
            # --- 🚨 지급/차감 선택 옵션 추가 ---
            st.subheader("⚙️ 전송 옵션 설정")
            col_opt1, col_opt2 = st.columns(2)
            with col_opt1:
                action_type = st.radio("수행할 작업을 선택하세요", ["적립금 추가 (지급)", "적립금 차감 (회수)"], index=0)
            with col_opt2:
                bulk_reason = st.text_input("📝 전송 사유 입력 (필수)", placeholder="예: 반품으로 인한 적립금 회수")
            
            st.divider()
            col1, col2 = st.columns(2)
            with col1:
                if st.button("💾 1. 내역을 최종 DB에 기록하기", type="primary", use_container_width=True):
                    if not bulk_reason.strip():
                        st.warning("⚠️ 사유를 입력해주세요.")
                    else:
                        with st.spinner("DB 기록 중..."):
                            save_df = st.session_state['cleaned_df'].copy()
                            save_df['비고'] = f"[{action_type}] {bulk_reason}"
                            save_df.to_sql(name='mileage_records', con=engine, if_exists='append', index=False)
                        st.success("🎉 DB 저장 완료!")

            with col2:
                if st.button(f"🚀 2. 카페24로 {action_type} 실행", type="primary", use_container_width=True):
                    if not bulk_reason.strip():
                        st.warning("⚠️ 사유를 입력해주세요.")
                    else:
                        url = f"https://{MALL_ID}.cafe24api.com/api/v2/admin/points"
                        headers = {
                            "Authorization": f"Bearer {st.session_state['access_token']}",
                            "Content-Type": "application/json",
                            "X-Cafe24-Api-Version": "2026-03-01" 
                        }

                        # API 파라미터 결정
                        api_type = "increase" if "추가" in action_type else "decrease"
                        
                        success_count, fail_count = 0, 0
                        summary_df = st.session_state['summary_df']
                        my_bar = st.progress(0)

                        for idx, row in summary_df.iterrows():
                            # 금액은 무조건 양수로 변환 (차감일 때도 API는 양수값을 보낸 뒤 type을 decrease로 지정함)
                            amount = abs(int(row['금액']))
                            
                            if amount > 0:
                                payload = {
                                    "request": {
                                        "member_id": str(row['아이디']).strip(), 
                                        "amount": amount, 
                                        "type": api_type, 
                                        "reason": bulk_reason
                                    }
                                }
                                try:
                                    res = requests.post(url, json=payload, headers=headers)
                                    if res.status_code in [200, 201]: success_count += 1
                                    else: 
                                        st.error(f"❌ {row['아이디']} 실패: {res.text}")
                                        fail_count += 1
                                except:
                                    fail_count += 1
                            
                            time.sleep(0.05)
                            my_bar.progress((idx + 1) / len(summary_df))

                        st.success(f"🎉 {action_type} 완료! 성공: {success_count}건 / 실패: {fail_count}건")
                        del st.session_state["access_token"] 

    except Exception as e:
        st.error(f"오류 발생: {e}")
