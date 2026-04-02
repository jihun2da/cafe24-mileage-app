import streamlit as st
import pandas as pd
from sqlalchemy import create_engine
import requests
import base64
import time
import urllib.parse
import io

# --- [페이지 설정] ---
st.set_page_config(page_title="카페24 적립금 통합 관리 시스템", layout="wide")

# --- [DB 및 카페24 초기 설정] ---
@st.cache_resource
def init_connection():
    db_info = st.secrets["mysql"]
    return create_engine(f"mysql+pymysql://{db_info['user']}:{db_info['password']}@{db_info['host']}:{db_info['port']}/{db_info['database']}")

engine = init_connection()

# Secrets 정보 로드
cafe24_info = st.secrets["cafe24"]
MALL_ID = cafe24_info["mall_id"]
CLIENT_ID = cafe24_info["client_id"]
CLIENT_SECRET = cafe24_info["client_secret"]
REDIRECT_URI = "https://cafe24-mileage-app.streamlit.app"
SCOPE = "mall.read_customer,mall.write_customer,mall.read_mileage,mall.write_mileage"

# --- [사이드바 메뉴 구성] ---
st.sidebar.title("🚀 메뉴 선택")
menu = st.sidebar.radio("원하시는 작업을 선택하세요", ["적립금 지급하기", "기록 조회 및 다운로드"])

# --- [공통 함수: 토큰 발급] ---
def get_access_token(auth_code):
    url = f"https://{MALL_ID}.cafe24api.com/api/v2/oauth/token"
    auth_str = f"{CLIENT_ID}:{CLIENT_SECRET}"
    b64_auth = base64.b64encode(auth_str.encode('utf-8')).decode('utf-8')
    headers = {"Authorization": f"Basic {b64_auth}", "Content-Type": "application/x-www-form-urlencoded"}
    data = {"grant_type": "authorization_code", "code": auth_code, "redirect_uri": REDIRECT_URI}
    response = requests.post(url, headers=headers, data=data)
    return (response.json().get("access_token"), None) if response.status_code == 200 else (None, response.text)

# ==========================================
# 화면 1: 적립금 지급하기 (기존 기능)
# ==========================================
if menu == "적립금 지급하기":
    st.title("💰 적립금 자동 지급/차감 시스템")
    
    st.header("🔑 STEP 1: 카페24 계정 연동")
    if "code" in st.query_params and "access_token" not in st.session_state:
        token, error_msg = get_access_token(st.query_params["code"])
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

    st.divider()
    st.header("📂 STEP 2: 엑셀 업로드 및 중복 체크")
    uploaded_file = st.file_uploader("파일 업로드", type=["xlsx", "xls", "csv"])

    if uploaded_file:
        try:
            df = pd.read_excel(uploaded_file) if uploaded_file.name.endswith(('xlsx', 'xls')) else pd.read_csv(uploaded_file)
            df.columns = df.columns.astype(str).str.strip()
            amt_col_name = next((name for name in ['적립금액', '적립금', '금액', '결제금액'] if name in df.columns), None)
            required_cols = ['아이디', '주문자명', '고객명', '브랜드', '상품', '색상', '사이즈']
            
            target_df = df[required_cols + [amt_col_name]].copy()
            target_df.columns = ['아이디', '주문자명', '고객명', '브랜드', '상품', '색상', '사이즈', '금액']
            target_df = target_df.dropna(subset=['아이디'])
            target_df['금액'] = pd.to_numeric(target_df['금액'], errors='coerce').fillna(0)

            # 중복 체크 로직
            db_df = pd.read_sql("SELECT 주문자명, 고객명, 브랜드, 상품, 색상, 사이즈, 금액 FROM mileage_records", con=engine)
            db_df['비교키'] = db_df['주문자명'].astype(str) + "|" + db_df['고객명'].astype(str) + "|" + db_df['브랜드'].astype(str) + "|" + db_df['상품'].astype(str) + "|" + db_df['색상'].astype(str) + "|" + db_df['사이즈'].astype(str) + "|" + db_df['금액'].astype(str)
            existing_keys = set(db_df['비교키'].tolist())
            
            target_df['비교키'] = target_df['주문자명'].astype(str) + "|" + target_df['고객명'].astype(str) + "|" + target_df['브랜드'].astype(str) + "|" + target_df['상품'].astype(str) + "|" + target_df['색상'].astype(str) + "|" + target_df['사이즈'].astype(str) + "|" + target_df['금액'].astype(str)
            target_df['DB상태'] = target_df['비교키'].apply(lambda x: '🚨 중복(DB존재)' if x in existing_keys else '✅ 신규')
            target_df.insert(0, '삭제선택', False)
            target_df.loc[target_df['DB상태'] == '🚨 중복(DB존재)', '삭제선택'] = True
            
            edited_raw_df = st.data_editor(target_df.drop(columns=['비교키']), hide_index=True, use_container_width=True)

            if st.button("🔄 체크 항목 제외 후 합산하기"):
                cleaned_df = edited_raw_df[edited_raw_df['삭제선택'] == False].drop(columns=['삭제선택', 'DB상태'])
                st.session_state['cleaned_df'] = cleaned_df
                summary_df = cleaned_df.groupby(['아이디', '주문자명'], as_index=False).agg({'고객명': 'first', '금액': 'sum'})
                st.session_state['summary_df'] = summary_df[summary_df['금액'] != 0]
                st.rerun()

            if 'summary_df' in st.session_state:
                st.divider()
                st.header("📊 STEP 3: 최종 확인 및 전송")
                s_df = st.session_state['summary_df']
                c1, c2 = st.columns(2)
                c1.metric("총 인원", f"{len(s_df)} 명")
                c2.metric("총 합계", f"{s_df['금액'].sum():,.0f} 원")
                st.dataframe(s_df, use_container_width=True, hide_index=True)
                
                action_type = st.radio("작업 선택", ["적립금 추가 (지급)", "적립금 차감 (회수)"])
                bulk_reason = st.text_input("📝 사유 입력 (필수)")
                
                b_c1, b_c2 = st.columns(2)
                if b_c1.button("💾 1. 원본 상세 내역을 DB에 기록", use_container_width=True, type="primary"):
                    save_df = st.session_state['cleaned_df'].copy()
                    save_df['비고'] = f"[{action_type}] {bulk_reason}"
                    save_df.to_sql(name='mileage_records', con=engine, if_exists='append', index=False)
                    st.success("🎉 DB 저장 완료!")

                if b_c2.button(f"🚀 2. 카페24로 {action_type} 실행", use_container_width=True, type="primary"):
                    # API 전송 로직 (생략 - 기존과 동일)
                    st.info("API 전송 시작...")
                    # ... (전송 코드 생략) ...
                    st.success("전송 완료!")
        except Exception as e:
            st.error(f"오류: {e}")

# ==========================================
# 화면 2: 기록 조회 및 다운로드 (신규 기능!)
# ==========================================
elif menu == "기록 조회 및 다운로드":
    st.title("🔍 DB 기록 조회 및 엑셀 다운로드")
    st.markdown("데이터베이스에 저장된 모든 **상세 내역**을 검색하고 파일로 저장할 수 있습니다.")

    # 1. DB에서 전체 데이터 불러오기
    try:
        # mileage_records 테이블에 'created_at' 같은 날짜 컬럼이 없다면 생성일자 기준으로 조회는 어렵지만, 
        # 우선 전체 데이터를 불러와서 아이디/이름으로 필터링하게 구성합니다.
        raw_db_df = pd.read_sql("SELECT * FROM mileage_records", con=engine)
        
        # 2. 검색 필터 설정 (사이드바 또는 상단)
        st.subheader("🔎 검색 필터")
        f_col1, f_col2, f_col3 = st.columns(3)
        
        search_id = f_col1.text_input("아이디 검색", "")
        search_name = f_col2.text_input("이름(주문자) 검색", "")
        search_reason = f_col3.text_input("비고(사유) 검색", "")

        # 3. 필터링 로직
        filtered_df = raw_db_df.copy()
        if search_id:
            filtered_df = filtered_df[filtered_df['아이디'].str.contains(search_id, na=False)]
        if search_name:
            filtered_df = filtered_df[filtered_df['주문자명'].str.contains(search_name, na=False)]
        if search_reason:
            filtered_df = filtered_df[filtered_df['비고'].str.contains(search_reason, na=False)]

        # 4. 결과 출력
        st.divider()
        st.subheader(f"✅ 조회 결과 (총 {len(filtered_df)}건)")
        st.dataframe(filtered_df, use_container_width=True, hide_index=True)

        # 5. 엑셀 다운로드 버튼
        if not filtered_df.empty:
            output = io.BytesIO()
            with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
                filtered_df.to_excel(writer, index=False, sheet_name='Search_Result')
            
            st.download_button(
                label="📥 검색 결과 엑셀로 다운로드",
                data=output.getvalue(),
                file_name="mileage_history_report.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )
        else:
            st.warning("조회된 내역이 없습니다.")

    except Exception as e:
        st.error(f"기록을 불러오는 중 오류가 발생했습니다: {e}")
