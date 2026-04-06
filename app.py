import streamlit as st
import requests
import base64
import urllib.parse

st.title("🔑 카페24 연동 테스트 모드")

# 1. 설정 정보 (Secrets에서 가져옴)
try:
    cafe24_info = st.secrets["cafe24"]
    MALL_ID = cafe24_info["mall_id"]
    CLIENT_ID = cafe24_info["client_id"]
    CLIENT_SECRET = cafe24_info["client_secret"]
    REDIRECT_URI = "https://cafe24-mileage-app.streamlit.app"
    st.write(f"현재 설정된 몰 ID: `{MALL_ID}`")
except Exception as e:
    st.error(f"Secrets 설정 오류: {e}")
    st.stop()

# 2. 주소창 파라미터 확인 (디버깅용)
st.write("---")
st.subheader("📡 주소창 신호 감지")
params = st.query_params
st.write("현재 주소창 파라미터:", params)

# 3. 인증 로직
if "code" in params:
    auth_code = params["code"]
    st.info(f"✅ 인증 코드를 발견했습니다: `{auth_code[:10]}...` (발급 시도 중)")
    
    # 토큰 요청
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
    
    try:
        res = requests.post(url, headers=headers, data=data)
        if res.status_code == 200:
            st.success("🎉 [대성공] 카페24 서버와 연결되었습니다!")
            st.json(res.json()) # 토큰 정보 출력
        else:
            st.error(f"❌ 토큰 발급 실패 (상태코드: {res.status_code})")
            st.write("응답 내용:", res.text)
    except Exception as e:
        st.error(f"통신 에러: {e}")

else:
    # 로그인 버튼 표시
    st.warning("아직 연동 전입니다. 아래 버튼을 눌러보세요.")
    SCOPE = "mall.read_customer,mall.write_customer,mall.read_mileage,mall.write_mileage"
    auth_url = f"https://{MALL_ID}.cafe24api.com/api/v2/oauth/authorize?response_type=code&client_id={CLIENT_ID}&state=random&redirect_uri={urllib.parse.quote(REDIRECT_URI)}&scope={SCOPE}"
    st.link_button("🔐 카페24 로그인 시작하기", auth_url, type="primary")

if st.button("🔄 주소창 초기화 및 다시 시작"):
    st.query_params.clear()
    st.rerun()
