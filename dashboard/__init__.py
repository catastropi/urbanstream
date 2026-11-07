"""UrbanStream 대시보드 (Step 10). 데이터 입구는 FastAPI(X-API-Key) 하나뿐이다 — DB 에 직접 붙지 않는다 (ADR-0012).

이 패키지의 config·timefmt·contract·client·shape 는 streamlit 을 import 하지 않는다(순수 파이썬).
그래야 단위 시험이 화면 없이 돌고, scripts/export_tableau.py 가 같은 클라이언트를 쓸 수 있다.
"""
