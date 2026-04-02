import streamlit as st
import pandas as pd
from sqlalchemy import create_engine

# --- [페이지 설정] ---
st.set_page_config(page_title="카페24 적립금 관리 시스템", layout="wide")
st.title("💰 적립금 일괄 지급 시스템 (정밀 중복체크형)")

@st.cache_resource
def init_connection():
    db_info = st.secrets["mysql"]
    return create_engine(f"mysql+pymysql://{db_info['user']}:{db_info['password']}@{db_info['host']}:{db_info['port']}/{db_info['database']}")

engine = init_connection()

# ==========================================
# 엑셀 컬럼 위치 설정 (업로드된 파일 기준)
# ==========================================
COL_ID = 3        # D열 (아이디)
COL_ORDERER = 4   # E열 (주문자명)
COL_CUSTOMER = 5  # F열 (고객명)
COL_BRAND = 6     # G열 (브랜드)
COL_PRODUCT = 7   # H열 (상품)
COL_COLOR = 8     # I열 (색상)
COL_SIZE = 9      # J열 (사이즈)
COL_MILEAGE = 11  # L열 (적립금액)
# ==========================================

# STEP 0: 엑셀 파일 업로드
uploaded_file = st.file_uploader("📂 처리할 엑셀 파일을 업로드하세요", type=["xlsx", "xls", "csv"])

if uploaded_file:
    try:
        # 1. 엑셀 로드 및 필요한 열만 추출 (csv와 xlsx 모두 지원하도록 예외처리)
        try:
            df = pd.read_excel(uploaded_file)
        except:
            uploaded_file.seek(0)
            df = pd.read_csv(uploaded_file)
            
        target_df = df.iloc[:, [COL_ID, COL_ORDERER, COL_CUSTOMER, COL_BRAND, COL_PRODUCT, COL_COLOR, COL_SIZE, COL_MILEAGE]].copy()
        
        # 컬럼 이름 깔끔하게 재지정 (원본 엑셀의 띄어쓰기 무시)
        target_df.columns = ['아이디', '주문자명', '고객명', '브랜드', '상품', '색상', '사이즈', '금액']
        
        target_df = target_df.dropna(subset=['아이디'])
        target_df['금액'] = pd.to_numeric(target_df['금액'], errors='coerce').fillna(0)

        # ==========================================
        # STEP 1: 정밀 중복 체크 및 삭제
        # ==========================================
        st.divider()
        st.header("STEP 1: 정밀 중복 체크")
        
        try:
            # DB에서 기존 내역을 가져와 비교용 '키(Key)' 생성
            db_df = pd.read_sql("SELECT 주문자명, 고객명, 브랜드, 상품, 색상, 사이즈, 금액 FROM mileage_records", con=engine)
            db_df['비교키'] = db_df['주문자명'].astype(str) + "|" + db_df['고객명'].astype(str) + "|" + db_df['브랜드'].astype(str) + "|" + db_df['상품'].astype(str) + "|" + db_df['색상'].astype(str) + "|" + db_df['사이즈'].astype(str) + "|" + db_df['금액'].astype(str)
            existing_keys = set(db_df['비교키'].tolist())
        except Exception:
            existing_keys = set() # DB가 처음이거나 비어있을 때
        
        # 엑셀 데이터에도 동일한 '키' 생성
        target_df['비교키'] = target_df['주문자명'].astype(str) + "|" + target_df['고객명'].astype(str) + "|" + target_df['브랜드'].astype(str) + "|" + target_df['상품'].astype(str) + "|" + target_df['색상'].astype(str) + "|" + target_df['사이즈'].astype(str) + "|" + target_df['금액'].astype(str)
        
        # 키를 대조하여 중복 판별
        target_df['DB상태'] = target_df['비교키'].apply(lambda x: '🚨 중복(DB존재)' if x in existing_keys else '✅ 신규')
        
        # 화면 출력을 위해 키 삭제 및 체크박스 추가
        target_df = target_df.drop(columns=['비교키'])
        target_df.insert(0, '삭제선택', False)
        
        # 중복인 항목은 자동으로 체크박스 선택됨 (편의성)
        target_df.loc[target_df['DB상태'] == '🚨 중복(DB존재)', '삭제선택'] = True

        st.markdown("주문자, 고객, 브랜드, 상품, 색상, 사이즈, 금액 등 **7가지 조건이 모두 일치하는 내역**만 🚨 중복으로 표시됩니다.\n(중복된 항목은 제외 처리를 위해 **[삭제선택]** 칸에 자동으로 체크됩니다.)")
        
        edited_raw_df = st.data_editor(target_df, hide_index=True, use_container_width=True)

        if st.button("🔄 체크된 항목 제외하고 안전하게 합산하기", type="secondary"):
            # 체크 안 된(False) 정상 건만 추출
            cleaned_df = edited_raw_df[edited_raw_df['삭제선택'] == False].drop(columns=['삭제선택', 'DB상태'])
            st.session_state['cleaned_df'] = cleaned_df
            
            # 사용자 화면에 보여줄 합산 데이터 생성
            summary_df = cleaned_df.groupby(['아이디', '주문자명'], as_index=False).agg({
                '고객명': 'first',
                '금액': 'sum'
            })
            st.session_state['summary_df'] = summary_df
            st.rerun()

        # ==========================================
        # STEP 2: 합산 결과 확인 및 일괄 사유 입력
        # ==========================================
        if 'summary_df' in st.session_state:
            st.divider()
            st.header("STEP 2: 최종 합산 내역 확인 및 사유 입력")
            
            # 금액 수정 불가한 읽기 전용 표 출력
            st.dataframe(st.session_state['summary_df'], use_container_width=True, hide_index=True)
            
            total_target = len(st.session_state['summary_df'])
            total_amount = st.session_state['summary_df']['금액'].sum()
            st.info(f"📌 최종 지급 대상: **{total_target}명** / 총 지급 예정 금액: **{total_amount:,.0f}원**")
            
            # 일괄 조정 사유 입력 (필수)
            bulk_reason = st.text_input("📝 일괄 비고(사유) 입력", placeholder="예: 4월 봄맞이 의류 리뷰 적립금")
            
            # ==========================================
            # STEP 3: DB 전송
            # ==========================================
            st.divider()
            st.header("STEP 3: DB 전송")
            if st.button("🚀 위 내역을 최종 DB에 전송하기", type="primary"):
                if not bulk_reason.strip():
                    st.warning("⚠️ 일괄 비고(사유)를 입력해야만 DB에 전송할 수 있습니다.")
                else:
                    try:
                        with st.spinner("향후 정밀 중복 체크를 위해 상세 내역을 안전하게 기록하고 있습니다..."):
                            # DB에는 요약본이 아닌 '상세 내역(cleaned_df)'을 저장합니다.
                            save_df = st.session_state['cleaned_df'].copy()
                            save_df['비고'] = bulk_reason
                            
                            save_df.to_sql(name='mileage_records', con=engine, if_exists='append', index=False)
                        
                        st.success("🎉 DB 저장 완료! 모든 데이터가 성공적으로 서버에 기록되었습니다.")
                        
                        # 완료 후 메모리 비우기
                        del st.session_state['cleaned_df']
                        del st.session_state['summary_df']
                        
                    except Exception as e:
                        st.error(f"DB 전송 중 오류가 발생했습니다: {e}")

    except Exception as e:
        st.error(f"오류가 발생했습니다. 상세내용: {e}")
