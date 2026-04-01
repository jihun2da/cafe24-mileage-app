
import streamlit as st
import pandas as pd

# --- [페이지 설정] ---
st.set_page_config(page_title="카페24 적립금 관리 시스템", layout="wide")
st.title("💰 적립금 일괄 지급 - 1단계: 데이터 전처리")

st.markdown("""
### 📂 작업 가이드
1. **0401.xlsx** 양식의 파일을 업로드하세요.
2. 프로그램이 **아이디(D열)**와 **주문자명(E열)**을 기준으로 동일 인물을 찾아 **적립금(L열)**을 합산합니다.
""")

# --- [파일 업로드 섹션] ---
uploaded_file = st.file_uploader("엑셀 파일을 선택하세요 (.xlsx)", type=["xlsx"])

if uploaded_file:
    try:
        # 엑셀 로드 (0401.xlsx 양식 기준)
        # 엑셀의 실제 데이터 위치에 따라 header 값을 조정할 수 있습니다. 
        # 보통 1행이 제목이면 header=0입니다.
        df = pd.read_excel(uploaded_file)

        # 사용자 요청에 따른 컬럼 추출 (D, E, F, L열)
        # 파이썬 인덱스는 0부터 시작하므로 D=3, E=4, F=5, L=11 입니다.
        # 만약 엑셀에 컬럼명이 있다면 이름으로 추출하는 것이 정확합니다.
        
        # 안전하게 인덱스로 접근하여 필요한 컬럼만 슬라이싱
        target_df = df.iloc[:, [3, 4, 5, 11]] 
        target_df.columns = ['아이디', '주문자명', '고객명', '적립금']

        # 데이터 클리닝: 아이디가 없는 행 삭제 및 적립금 숫자 변환
        target_df = target_df.dropna(subset=['아이디'])
        target_df['적립금'] = pd.to_numeric(target_df['적립금'], errors='coerce').fillna(0)

        st.subheader("1. 원본 데이터 확인 (상위 5건)")
        st.write(target_df.head())

        # --- [핵심 로직: 아이디 & 주문자명 기준 합산] ---
        # 동일 아이디(D)와 동일 주문자(E)가 있다면 적립금(L)을 합산합니다.
        summary_df = target_df.groupby(['아이디', '주문자명', '고객명'], as_index=False)['적립금'].sum()

        st.divider()
        st.subheader("2. 아이디별 합산 결과")
        st.info(f"조회된 총 인원: {len(summary_df)}명 / 합산된 총 적립금: {summary_df['적립금'].sum():,.0f}원")
        
        # 결과 표 출력
        st.dataframe(summary_df, use_container_width=True)

        # 다음 단계 전용 세션 상태 저장
        st.session_state['summary_df'] = summary_df

    except Exception as e:
        st.error(f"엑셀 분석 중 오류가 발생했습니다: {e}")
        st.info("Tip: 엑셀 파일의 D, E, F, L열에 데이터가 정확히 있는지 확인해주세요.")

else:
    st.warning("먼저 엑셀 파일을 업로드해주세요.")