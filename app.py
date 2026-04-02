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

# --- [DB 및 카페24 초기 설정] ---
@st.cache_resource
def init_connection():
    db_info = st.secrets["mysql"]
    # 데이터베이스 연결 시 한글 깨짐 방지를 위해 charset 설정 추가
    return create_engine(f"mysql+pymysql://{db_info['user']}:{db_info['password']}@{db_info['host']}:{db_info['port']}/{db_info['database']}?charset=utf8mb4")

engine = init_connection()

# 앱 시작 시 테이블이 없으면 자동 생성하는 함수
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

# DB 준비 실행
try:
    prepare_db()
except Exception as e:
    st.error(f"데이터베이스 초기화 중 오류가 발생했습니다: {e}")

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
# 화면 1: 적립금 지급하기
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

            # --- 중복 체크 로직 ---
            try:
                db_df = pd.read_sql("SELECT 주문자명, 고객명, 브랜드, 상품, 색상, 사이즈, 금액 FROM mileage_records", con=engine)
                db_df['비교키'] = db_df['주문자명'].astype(str) + "|" + db_df['고객명'].astype(str) + "|" + db_df['브랜드'].astype(str) + "|" + db_df['상품'].astype(str) + "|" + db_df['색상'].astype(str) + "|" + db_df['사이즈'].astype(str) + "|" + db_df['금액'].astype(str)
                existing_keys = set(db_df['비교키'].tolist())
            except:
                existing_keys = set()
            
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
                    if not bulk_reason.strip():
                        st.warning("⚠️ 사유를 입력해주세요.")
                    else:
                        save_df = st.session_state['cleaned_df'].copy()
                        save_df['비고'] = f"[{action_type}] {bulk_reason}"
                        save_df['지급일시'] = datetime.now()
                        save_df.to_sql(name='mileage_records', con=engine, if_exists='append', index=False)
                        st.success("🎉 DB 저장 완료!")

                if b_c2.button(f"🚀 2. 카페24로 {action_type} 실행", use_container_width=True, type="primary"):
                    if not bulk_reason.strip():
                        st.warning("⚠️ 사유를 입력해주세요.")
                    else:
                        url = f"https://{MALL_ID}.cafe24api.com/api/v2/admin/points"
                        headers = {
                            "Authorization": f"Bearer {st.session_state['access_token']}",
                            "Content-Type": "application/json",
                            "X-Cafe24-Api-Version": "2026-03-01" 
                        }
                        api_type = "increase" if "추가" in action_type else "decrease"
                        
                        success_count, fail_count = 0, 0
                        my_bar = st.progress(0)
                        
                        for idx, row in s_df.iterrows():
                            amount = abs(int(row['금액']))
                            payload = {"request": {"member_id": str(row['아이디']).strip(), "amount": amount, "type": api_type, "reason": bulk_reason}}
                            try:
                                res = requests.post(url, json=payload, headers=headers)
                                if res.status_code in [200, 201]: success_count += 1
                                else: st.error(f"❌ {row['아이디']} 실패: {res.text}"); fail_count += 1
                            except: fail_count += 1
                            my_bar.progress((idx + 1) / len(s_df))
                        st.success(f"🎉 완료! (성공: {success_count} / 실패: {fail_count})")
                        del st.session_state["access_token"]
        except Exception as e:
            st.error(f"오류: {e}")

# ==========================================
# 화면 2: 기록 조회 및 다운로드
# ==========================================
elif menu == "기록 조회 및 다운로드":
    st.title("🔍 DB 기록 조회 및 엑셀 다운로드")
    try:
        raw_db_df = pd.read_sql("SELECT * FROM mileage_records ORDER BY 지급일시 DESC", con=engine)
        
        st.subheader("🔎 검색 필터")
        f_col1, f_col2, f_col3 = st.columns(3)
        search_id = f_col1.text_input("아이디 검색")
        search_name = f_col2.text_input("이름 검색")
        search_reason = f_col3.text_input("사유 검색")

        filtered_df = raw_db_df.copy()
        if search_id: filtered_df = filtered_df[filtered_df['아이디'].str.contains(search_id, na=False)]
        if search_name: filtered_df = filtered_df[filtered_df['주문자명'].str.contains(search_name, na=False)]
        if search_reason: filtered_df = filtered_df[filtered_df['비고'].str.contains(search_reason, na=False)]

        st.divider()
        st.subheader(f"✅ 조회 결과 ({len(filtered_df)}건)")
        st.dataframe(filtered_df, use_container_width=True, hide_index=True)

        if not filtered_df.empty:
            output = io.BytesIO()
            with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
                filtered_df.to_excel(writer, index=False, sheet_name='Result')
            st.download_button(label="📥 검색 결과 엑셀 다운로드", data=output.getvalue(), file_name="mileage_history.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
    except Exception as e:
        st.info("아직 저장된 내역이 없습니다. 먼저 적립금을 지급하여 데이터를 DB에 기록해 보세요.")
