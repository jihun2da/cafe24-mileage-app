import streamlit as st
import pandas as pd
from sqlalchemy import create_engine
import requests
import base64
import time
import urllib.parse

# --- [페이지 설정] ---
st.set_page_config(page_title="카페24 적립금 자동 지급 시스템", layout="wide")
st.title("💰 적립금 자동 지급 시스템 (원스탑 자동화)")

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
        # 카페24가 알려주는 정확한 실패 사유를 반환합니다.
        return None, response.text 

# ==========================================
# STEP 1: 카페24 연동 (가장 먼저 실행!)
# ==========================================
st.header("🔑 STEP 1: 카페24 계정 연동 (필수)")

if "code" in st.query_params and "access_token" not in st.session_state:
    auth_code = st.query_params["code"]
    with st.spinner("🔄 카페24 인증을 자동으로 처리하고 있습니다..."):
        token, error_msg = get_access_token(auth_code)
        if token:
            st.session_state["access_token"] = token
            st.query_params.clear()
            st.rerun() # 성공하면 깔끔하게 화면 새로고침
        else:
            # 🚨 실패 시 상세 에러 메시지를 화면에 뿌려줍니다!
            st.error(f"❌ 토큰 발급에 실패했습니다. 상세 원인: {error_msg}")
            if st.button("🔄 에러 지우고 처음부터 다시 시도하기"):
                st.query_params.clear()
                st.rerun()
            st.stop()

if "access_token" not in st.session_state:
    st.warning("⚠️ 적립금 작업을 시작하려면 먼저 카페24 쇼핑몰 연동이 필요합니다.")
    auth_url = f"https://{MALL_ID}.cafe24api.com/api/v2/oauth/authorize?response_type=code&client_id={CLIENT_ID}&state=random&redirect_uri={urllib.parse.quote(REDIRECT_URI)}&scope={SCOPE}"
    st.link_button("🔐 카페24 로그인 및 연동하기", auth_url, type="primary")
    st.info("💡 연동을 완료해야 다음 단계(엑셀 업로드)가 나타납니다.")
    st.stop()
else:
    st.success("✅ 카페24 시스템과 성공적으로 연결되었습니다! 이제 안심하고 아래 작업을 진행하세요.")

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
        
        if not amt_col_name or any(col not in df.columns for col in required_cols):
             st.error("❌ 엑셀 파일의 열이 올바르지 않습니다.")
             st.stop()

        target_df = df[required_cols + [amt_col_name]].copy()
        target_df.columns = ['아이디', '주문자명', '고객명', '브랜드', '상품', '색상', '사이즈', '금액']
        target_df = target_df.dropna(subset=['아이디'])
        target_df['금액'] = pd.to_numeric(target_df['금액'], errors='coerce').fillna(0)

        # --- 중복 체크 ---
        try:
            db_df = pd.read_sql("SELECT 주문자명, 고객명, 브랜드, 상품, 색상, 사이즈, 금액 FROM mileage_records", con=engine)
            db_df['비교키'] = db_df['주문자명'].astype(str) + "|" + db_df['고객명'].astype(str) + "|" + db_df['브랜드'].astype(str) + "|" + db_df['상품'].astype(str) + "|" + db_df['색상'].astype(str) + "|" + db_df['사이즈'].astype(str) + "|" + db_df['금액'].astype(str)
            existing_keys = set(db_df['비교키'].tolist())
        except Exception:
            existing_keys = set()
        
        target_df['비교키'] = target_df['주문자명'].astype(str) + "|" + target_df['고객명'].astype(str) + "|" + target_df['브랜드'].astype(str) + "|" + target_df['상품'].astype(str) + "|" + target_df['색상'].astype(str) + "|" + target_df['사이즈'].astype(str) + "|" + target_df['금액'].astype(str)
        target_df['DB상태'] = target_df['비교키'].apply(lambda x: '🚨 중복(DB존재)' if x in existing_keys else '✅ 신규')
        target_df = target_df.drop(columns=['비교키'])
        target_df.insert(0, '삭제선택', False)
        target_df.loc[target_df['DB상태'] == '🚨 중복(DB존재)', '삭제선택'] = True

        st.markdown("7가지 조건이 일치하는 내역은 중복으로 표시되며, **[삭제선택]**에 자동 체크됩니다.")
        edited_raw_df = st.data_editor(target_df, hide_index=True, use_container_width=True)

        if st.button("🔄 체크된 항목 제외하고 안전하게 합산하기", type="secondary"):
            cleaned_df = edited_raw_df[edited_raw_df['삭제선택'] == False].drop(columns=['삭제선택', 'DB상태'])
            st.session_state['cleaned_df'] = cleaned_df
            summary_df = cleaned_df.groupby(['아이디', '주문자명'], as_index=False).agg({'고객명': 'first', '금액': 'sum'})
            st.session_state['summary_df'] = summary_df
            st.rerun()

        # ==========================================
        # STEP 3: 결과 확인 및 전송 버튼
        # ==========================================
        if 'summary_df' in st.session_state:
            st.divider()
            st.header("STEP 3: 최종 내역 확인 및 카페24 전송")
            st.dataframe(st.session_state['summary_df'], use_container_width=True, hide_index=True)
            
            total_target = len(st.session_state['summary_df'])
            total_amount = st.session_state['summary_df']['금액'].sum()
            st.info(f"📌 최종 지급 대상: **{total_target}명** / 총 지급 예정 금액: **{total_amount:,.0f}원**")
            bulk_reason = st.text_input("📝 일괄 비고(사유) 입력", placeholder="예: 4월 이벤트 적립금 (필수 입력)")
            
            st.divider()
            col1, col2 = st.columns(2)
            with col1:
                if st.button("💾 1. 내역을 최종 DB에 기록하기", type="primary", use_container_width=True):
                    if not bulk_reason.strip():
                        st.warning("⚠️ 일괄 비고(사유)를 입력해야 전송할 수 있습니다.")
                    else:
                        with st.spinner("DB에 기록 중입니다..."):
                            save_df = st.session_state['cleaned_df'].copy()
                            save_df['비고'] = bulk_reason
                            save_df.to_sql(name='mileage_records', con=engine, if_exists='append', index=False)
                        st.success("🎉 DB 저장 완료!")

            with col2:
                if st.button("🚀 2. 카페24로 적립금 자동 쏘기 (API)", type="primary", use_container_width=True):
                    if not bulk_reason.strip():
                        st.warning("⚠️ 일괄 비고(사유)를 입력해야 전송할 수 있습니다.")
                    else:
                        url = f"https://{MALL_ID}.cafe24api.com/api/v2/admin/points"
                        headers = {
                            "Authorization": f"Bearer {st.session_state['access_token']}",
                            "Content-Type": "application/json",
                            "X-Cafe24-Api-Version": "2026-03-01" 
                        }

                        success_count, fail_count = 0, 0
                        progress_text = "카페24로 적립금을 전송하는 중..."
                        my_bar = st.progress(0, text=progress_text)
                        
                        summary_df = st.session_state['summary_df']
                        total_rows = len(summary_df)

                        for idx, row in summary_df.iterrows():
                            member_id = str(row['아이디']).strip()
                            amount = int(row['금액'])
                            payload = {
                                "request": {"member_id": member_id, "amount": amount, "type": "increase", "reason": bulk_reason}
                            }
                            
                            try:
                                res = requests.post(url, json=payload, headers=headers)
                                if res.status_code in [200, 201]: success_count += 1
                                else: 
                                    fail_count += 1
                                    st.error(f"❌ {member_id} 전송 실패: {res.text}")
                            except Exception as e:
                                fail_count += 1
                                st.error(f"❌ {member_id} 시스템 에러: {e}")
                                
                            time.sleep(0.1) 
                            my_bar.progress((idx + 1) / total_rows, text=f"{progress_text} ({idx+1}/{total_rows})")

                        if fail_count == 0:
                            st.success(f"🎉 총 {success_count}명에게 카페24 적립금 자동 전송을 완벽하게 완료했습니다!")
                            del st.session_state["access_token"] 
                        else:
                            st.warning(f"전송 완료. 성공: {success_count}건 / 실패: {fail_count}건")

    except Exception as e:
        st.error(f"오류가 발생했습니다: {e}")
