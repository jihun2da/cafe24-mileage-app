import streamlit as st
import pandas as pd

# --- [페이지 설정] ---
st.set_page_config(page_title="카페24 적립금 관리 시스템", layout="wide")
st.title("💰 적립금 일괄 지급 - 1단계: 데이터 전처리")

st.markdown("""
### 📂 작업 가이드
1. 적립금 내역이 담긴 **엑셀 파일**을 업로드하세요. (파일명은 상관없습니다.)
2. 프로그램이 **아이디(D열)**와 **주문자명(E열)**이 일치하는 내역을 찾아 **적립금(L열)**을 자동으로 합산합니다.
""")

# --- [파일 업로드 섹션 (파일명 상관없이 .xlsx, .xls 모두 가능)] ---
uploaded_file = st.file_uploader("엑셀 파일을 선택하세요", type=["xlsx", "xls"])

if uploaded_file:
    try:
        # 엑셀 로드
        df = pd.read_excel(uploaded_file)

        # 파이썬 인덱스는 0부터 시작: D열(3), E열(4), F열(5), L열(11)
        target_df = df.iloc[:, [3, 4, 5, 11]] 
        target_df.columns = ['아이디', '주문자명', '고객명', '적립금']

        # 데이터 클리닝: 아이디가 없는 행 삭제 및 적립금을 숫자로 변환
        target_df = target_df.dropna(subset=['아이디'])
        target_df['적립금'] = pd.to_numeric(target_df['적립금'], errors='coerce').fillna(0)

        st.subheader("1. 엑셀 데이터 인식 확인 (상위 5건 미리보기)")
        st.write(target_df.head()) # 파일이 잘 읽혔는지 확인하는 용도

        # --- [수정된 핵심 로직: 아이디 & 주문자명 기준 합산] ---
        # 고객명은 조건에서 제외하고, 아이디와 주문자명만으로 그룹을 묶음.
        # 고객명 컬럼이 사라지면 아쉬우니, 첫 번째로 발견된 고객명을 표에 남김('first')
        summary_df = target_df.groupby(['아이디', '주문자명'], as_index=False).agg({
            '고객명': 'first',
            '적립금': 'sum'
        })

        # 보기 좋게 컬럼 순서 재배치
        summary_df = summary_df[['아이디', '주문자명', '고객명', '적립금']]

        st.divider()
        st.subheader("2. 아이디별 적립금 합산 결과")
        st.info(f"조회된 총 인원: {len(summary_df)}명 / 합산된 총 적립금: {summary_df['적립금'].sum():,.0f}원")
        
        # 합산 완료된 표 출력
        st.dataframe(summary_df, use_container_width=True)

        # 다음 단계를 위해 데이터 임시 저장
        st.session_state['summary_df'] = summary_df

    except Exception as e:
        st.error(f"엑셀 분석 중 오류가 발생했습니다: {e}")
        st.info("Tip: 엑셀 파일의 D, E, F, L열에 데이터가 정확히 있는지 확인해주세요.")

else:
    st.warning("먼저 엑셀 파일을 업로드해주세요.")
