import streamlit as st
import pandas as pd
from sqlalchemy import create_engine
import requests
import time

# --- [페이지 설정] ---
st.set_page_config(page_title="카페24 적립금 자동 지급 시스템", layout="wide")
st.title("💰 적립금 자동 지급 시스템 (API 연동형)")

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
# ==========================================

# STEP 0: 엑셀 파일 업로드
uploaded_file = st.file_uploader("📂 처리할 엑셀 파일을 업로드하세요", type=["xlsx", "xls", "csv"])

if uploaded_file:
    try:
        try:
            df = pd.read_excel(uploaded_file)
        except:
            uploaded_file.seek(0)
            df = pd.read_csv(uploaded_file)
            
        df.columns = df.columns.astype(str).str.strip()
        
        amt_col_name = None
        for name in ['적립금액', '적립금', '금액', '결제금액']:
            if name in df.columns:
                amt_col_name = name
                break
                
        required_cols = ['아이디', '주문자명', '고객명', '브랜드', '상품', '색상', '사이즈']
        
        if not amt_col_name:
             st.error("❌ 엑셀 파일에서 '적립금액' 열을 찾을 수 없습니다.")
             st.stop()
             
        missing_cols = [col for col in required_cols if col not in df.columns]
        if missing_cols:
             st.error(f"❌ 엑셀 파일에서 다음 열을 찾을 수 없습니다: {', '.join(missing_cols)}")
             st.stop()

        target_df = df[required_cols + [amt_col_name]].copy()
        target_df.columns = ['아이디', '주문자명', '고객명', '브랜드', '상품', '색상', '사이즈', '금액']
        target_df = target_df.dropna(subset=['아이디'])
        target_df['금액'] = pd.to_numeric(target_df['금액'], errors='coerce').fillna(0)

        # ==========================================
        # STEP 1: 정밀 중복 체크
        # ==========================================
        st.divider()
        st.header("STEP 1: 정밀 중복 체크")
        
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
            
            summary_df = cleaned_df.groupby(['아이디', '주문자명'], as_index=False).agg({
                '고객명': 'first',
                '금액': 'sum'
            })
            st.session_state['summary_df'] = summary_df
            st.rerun()

        # ==========================================
        # STEP 2: 합산 결과 확인 및 사유 입력
        # ==========================================
        if 'summary_df' in st.session_state:
            st.divider()
            st.header("STEP 2: 최종 합산 내역 확인 및 사유 입력")
            
            st.dataframe(st.session_state['summary_df'], use_container_width=True, hide_index=True)
            
            total_target = len(st.session_state['summary_df'])
            total_amount = st.session_state['summary_df']['금액'].sum()
            st.info(f"📌 최종 지급 대상: **{total_target}명** / 총 지급 예정 금액: **{total_amount:,.0f}원**")
            
            bulk_reason = st.text_input("📝 일괄 비고(사유) 입력", placeholder="예: 4월 이벤트 적립금 (필수 입력)")
            
            # ==========================================
            # STEP 3: DB 전송 및 카페24 API 전송
            # ==========================================
            st.divider()
            st.header("STEP 3: DB 기록 및 카페24 API 자동 전송")
            
            col1, col2 = st.columns(2)
            
            # [버튼 1: DB 전송]
            with col1:
                if st.button("💾 1. 위 내역을 최종 DB에 전송하기", type="primary", use_container_width=True):
                    if not bulk_reason.strip():
                        st.warning("⚠️ 일괄 비고(사유)를 입력해야 전송할 수 있습니다.")
                    else:
                        try:
                            with st.spinner("DB에 상세 내역을 기록 중입니다..."):
                                save_df = st.session_state['cleaned_df'].copy()
                                save_df['비고'] = bulk_reason
                                save_df.to_sql(name='mileage_records', con=engine, if_exists='append', index=False)
                            st.success("🎉 DB 저장 완료!")
                            st.session_state['db_saved'] = True
                        except Exception as e:
                            st.error(f"DB 오류: {e}")

            # [버튼 2: 카페24 API 전송]
            with col2:
                if st.button("🚀 2. 카페24로 적립금 자동 쏘기 (API)", type="primary", use_container_width=True):
                    if not bulk_reason.strip():
                        st.warning("⚠️ 일괄 비고(사유)를 입력해야 전송할 수 있습니다.")
                    else:
                        cafe24_info = st.secrets["cafe24"]
                        mall_id = cafe24_info["mall_id"]
                        access_token = cafe24_info["access_token"]
                        
                        url = f"https://{mall_id}.cafe24api.com/api/v2/admin/points"
                        headers = {
                            "Authorization": f"Bearer {access_token}",
                            "Content-Type": "application/json",
                            "X-Cafe24-Api-Version": "2024-03-01" # 필요시 버전에 맞게 수정
                        }

                        success_count = 0
                        fail_count = 0
                        
                        # 진행 상황을 보여줄 프로그레스 바
                        progress_text = "카페24로 적립금을 전송하는 중..."
                        my_bar = st.progress(0, text=progress_text)
                        
                        summary_df = st.session_state['summary_df']
                        total_rows = len(summary_df)

                        for idx, row in summary_df.iterrows():
                            member_id = str(row['아이디']).strip()
                            amount = int(row['금액'])
                            
                            payload = {
                                "request": {
                                    "member_id": member_id,
                                    "amount": amount,
                                    "type": "increase", # 적립금 추가
                                    "reason": bulk_reason
                                }
                            }
                            
                            try:
                                response = requests.post(url, json=payload, headers=headers)
                                if response.status_code in [200, 201]:
                                    success_count += 1
                                else:
                                    fail_count += 1
                                    st.error(f"❌ {member_id} 전송 실패: {response.text}")
                            except Exception as e:
                                fail_count += 1
                                st.error(f"❌ {member_id} 시스템 에러: {e}")
                                
                            # API 호출 제한 방지를 위해 약간의 딜레이
                            time.sleep(0.1) 
                            
                            # 프로그레스 바 업데이트
                            progress = (idx + 1) / total_rows
                            my_bar.progress(progress, text=f"{progress_text} ({idx+1}/{total_rows})")

                        if fail_count == 0:
                            st.success(f"🎉 총 {success_count}명에게 카페24 적립금 자동 전송을 완료했습니다!")
                        else:
                            st.warning(f"전송 완료. 성공: {success_count}건 / 실패: {fail_count}건")

    except Exception as e:
        st.error(f"오류가 발생했습니다: {e}")
