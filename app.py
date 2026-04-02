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

# 테이블 자동 생성 및 유지
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

cafe24_info = st.secrets["cafe24"]
MALL_ID = cafe24_info["mall_id"]
CLIENT_ID = cafe24_info["client_id"]
CLIENT_SECRET = cafe24_info["client_secret"]
REDIRECT_URI = "https://cafe24-mileage-app.streamlit.app"
SCOPE = "mall.read_customer,mall.write_customer,mall.read_mileage,mall.write_mileage"

# --- [사이드바 메뉴] ---
st.sidebar.title("🚀 메뉴 선택")
menu = st.sidebar.radio("원하시는 작업을 선택하세요", ["적립금 지급하기", "기록 조회 및 다운로드", "DB 기록 삭제"])

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
    
    # 카페24 인증 세션 관리
    if "code" in st.query_params and "access_token" not in st.session_state:
        token, error_msg = get_access_token(st.query_params["code"])
        if token:
            st.session_state["access_token"] = token
            st.query_params.clear()
            st.rerun()

    if "access_token" not in st.session_state:
        auth_url = f"https://{MALL_ID}.cafe24api.com/api/v2/oauth/authorize?response_type=code&client_id={CLIENT_ID}&state=random&redirect_uri={urllib.parse.quote(REDIRECT_URI)}&scope={SCOPE}"
        st.link_button("🔐 카페24 로그인 및 연동하기", auth_url, type="primary")
        st.stop()
    else:
        st.success("✅ 카페24 연결 성공!")

    uploaded_file = st.file_uploader("파일 업로드", type=["xlsx", "xls", "csv"])

    if uploaded_file:
        try:
            df = pd.read_excel(uploaded_file) if uploaded_file.name.endswith(('xlsx', 'xls')) else pd.read_csv(uploaded_file)
            df.columns = df.columns.astype(str).str.strip()
            amt_col_name = next((name for name in ['적립금액', '적립금', '금액', '결제금액'] if name in df.columns), None)
            required_cols = ['아이디', '주문자명', '고객명', '브랜드', '상품', '색상', '사이즈']
            
            target_df = df[required_cols + [amt_col_name]].copy()
            target_df.columns = ['아이디', '주문자명', '고객명', '브랜드', '상품', '색상', '사이즈', '금액']
            target_df['금액'] = pd.to_numeric(target_df['금액'], errors='coerce').fillna(0)

            # 중복 체크
            db_df = pd.read_sql("SELECT 주문자명, 고객명, 브랜드, 상품, 색상, 사이즈, 금액 FROM mileage_records", con=engine)
            db_df['비교키'] = db_df['주문자명'].astype(str) + "|" + db_df['고객명'].astype(str) + "|" + db_df['브랜드'].astype(str) + "|" + db_df['상품'].astype(str) + "|" + db_df['색상'].astype(str) + "|" + db_df['사이즈'].astype(str) + "|" + db_df['금액'].astype(str)
            existing_keys = set(db_df['비교키'].tolist())
            
            target_df['비교키'] = target_df['주문자명'].astype(str) + "|" + target_df['고객명'].astype(str) + "|" + target_df['브랜드'].astype(str) + "|" + target_df['상품'].astype(str) + "|" + target_df['색상'].astype(str) + "|" + target_df['사이즈'].astype(str) + "|" + target_df['금액'].astype(str)
            target_df['DB상태'] = target_df['비교키'].apply(lambda x: '🚨 중복' if x in existing_keys else '✅ 신규')
            target_df.insert(0, '삭제선택', False)
            target_df.loc[target_df['DB상태'] == '🚨 중복', '삭제선택'] = True
            
            edited_raw_df = st.data_editor(target_df.drop(columns=['비교키']), hide_index=True, use_container_width=True)

            if st.button("🔄 체크 항목 제외 후 합산하기"):
                st.session_state['cleaned_df'] = edited_raw_df[edited_raw_df['삭제선택'] == False].drop(columns=['삭제선택', 'DB상태'])
                s_df = st.session_state['cleaned_df'].groupby(['아이디', '주문자명'], as_index=False).agg({'고객명': 'first', '금액': 'sum'})
                st.session_state['summary_df'] = s_df[s_df['금액'] != 0]
                st.rerun()

            if 'summary_df' in st.session_state:
                st.divider()
                s_df = st.session_state['summary_df']
                st.metric("총 인원", f"{len(s_df)} 명"), st.metric("총 합계", f"{s_df['금액'].sum():,.0f} 원")
                st.dataframe(s_df, use_container_width=True, hide_index=True)
                
                action_type = st.radio("작업 선택", ["적립금 추가 (지급)", "적립금 차감 (회수)"])
                bulk_reason = st.text_input("📝 사유 입력 (API 전송 시 필요)")
                
                b_c1, b_c2 = st.columns(2)
                
                # --- 💾 1. DB 기록 버튼 (확인 절차 포함) ---
                with b_c1:
                    if st.button("💾 1. 원본 상세 내역을 DB에 기록", use_container_width=True, type="primary"):
                        st.session_state['show_confirm'] = True
                    
                    if st.session_state.get('show_confirm'):
                        st.warning("⚠️ 정말로 현재 상세 내역을 DB에 기록하시겠습니까?")
                        conf_c1, conf_c2 = st.columns(2)
                        if conf_c1.button("⭕ 예 (업로드)", use_container_width=True):
                            save_df = st.session_state['cleaned_df'].copy()
                            reason_tag = bulk_reason if bulk_reason.strip() else "상세내역 기록"
                            save_df['비고'] = f"[{action_type}] {reason_tag}"
                            save_df['지급일시'] = datetime.now()
                            save_df.to_sql(name='mileage_records', con=engine, if_exists='append', index=False)
                            st.success("🎉 DB 저장 완료!")
                            st.session_state['show_confirm'] = False
                        if conf_c2.button("❌ 아니요 (취소)", use_container_width=True):
                            st.session_state['show_confirm'] = False
                            st.rerun()

                with b_c2:
                    if st.button(f"🚀 2. 카페24로 {action_type} 실행", use_container_width=True, type="primary"):
                        if not bulk_reason.strip(): st.warning("⚠️ 전송 사유를 입력해주세요.")
                        else:
                            url = f"https://{MALL_ID}.cafe24api.com/api/v2/admin/points"
                            headers = {"Authorization": f"Bearer {st.session_state['access_token']}", "Content-Type": "application/json", "X-Cafe24-Api-Version": "2026-03-01"}
                            api_type = "increase" if "추가" in action_type else "decrease"
                            success_count = 0
                            my_bar = st.progress(0)
                            for idx, row in s_df.iterrows():
                                payload = {"request": {"member_id": str(row['아이디']).strip(), "amount": abs(int(row['금액'])), "type": api_type, "reason": bulk_reason}}
                                res = requests.post(url, json=payload, headers=headers)
                                if res.status_code in [200, 201]: success_count += 1
                                my_bar.progress((idx + 1) / len(s_df))
                            st.success(f"🎉 {success_count}건 전송 완료!")
                            del st.session_state["access_token"]
        except Exception as e: st.error(f"오류: {e}")

# ==========================================
# 화면 2: 기록 조회 및 다운로드
# ==========================================
elif menu == "기록 조회 및 다운로드":
    st.title("🔍 DB 기록 조회 및 다운로드")
    try:
        raw_db_df = pd.read_sql("SELECT * FROM mileage_records ORDER BY 지급일시 DESC", con=engine)
        st.subheader("🔎 검색 필터")
        f_c1, f_c2, f_c3 = st.columns(3)
        s_id, s_name, s_rs = f_c1.text_input("아이디"), f_c2.text_input("이름"), f_c3.text_input("사유")
        filtered_df = raw_db_df.copy()
        if s_id: filtered_df = filtered_df[filtered_df['아이디'].str.contains(s_id, na=False)]
        if s_name: filtered_df = filtered_df[filtered_df['주문자명'].str.contains(s_name, na=False)]
        if s_rs: filtered_df = filtered_df[filtered_df['비고'].str.contains(s_rs, na=False)]
        st.dataframe(filtered_df, use_container_width=True, hide_index=True)
        if not filtered_df.empty:
            output = io.BytesIO()
            with pd.ExcelWriter(output, engine='xlsxwriter') as writer: filtered_df.to_excel(writer, index=False)
            st.download_button(label="📥 결과 엑셀 다운로드", data=output.getvalue(), file_name="history.xlsx")
    except: st.info("데이터가 없습니다.")

# ==========================================
# 화면 3: DB 기록 삭제 (신규 기능!)
# ==========================================
elif menu == "DB 기록 삭제":
    st.title("🗑️ DB 기록 삭제 (날짜별)")
    st.warning("데이터를 삭제하면 복구할 수 없습니다. 신중하게 선택해 주세요.")
    
    try:
        # 날짜별로 그룹화하여 목록 가져오기
        group_query = "SELECT DATE(지급일시) as 날짜, COUNT(*) as 건수, 비고 FROM mileage_records GROUP BY DATE(지급일시), 비고 ORDER BY 날짜 DESC"
        groups = pd.read_sql(group_query, con=engine)
        
        if groups.empty:
            st.info("삭제할 데이터가 없습니다.")
        else:
            st.subheader("삭제할 데이터 묶음을 선택하세요")
            # 선택하기 편하게 문구 조합
            groups['선택지'] = groups['날짜'].astype(str) + " | " + groups['비고'].astype(str) + " (" + groups['건수'].astype(str) + "건)"
            selected_group = st.selectbox("기록 묶음 선택", groups['선택지'].tolist())
            
            # 선택된 묶음의 상세 정보 추출
            sel_date = selected_group.split(" | ")[0]
            sel_reason = selected_group.split(" | ")[1].split(" (")[0]
            
            st.write(f"선택된 내역: **{sel_date}** 에 등록된 **'{sel_reason}'** 관련 데이터")
            
            if st.button("🧨 선택한 데이터 전체 삭제", type="primary"):
                delete_sql = text("DELETE FROM mileage_records WHERE DATE(지급일시) = :d AND 비고 = :r")
                with engine.connect() as conn:
                    conn.execute(delete_sql, {"d": sel_date, "r": sel_reason})
                    conn.commit()
                st.success(f"✅ {sel_date} 내역이 성공적으로 삭제되었습니다.")
                st.rerun()
                
    except Exception as e:
        st.error(f"삭제 중 오류 발생: {e}")
