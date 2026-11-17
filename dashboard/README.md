# dashboard

Streamlit 대시보드 (Step 10). **데이터 입구는 FastAPI(`X-API-Key`) 하나** — DB 에 직접 붙지 않는다(구조 시험이 막음). 결정 근거 `docs/adr/0012-step10-dashboard.md`, 실행 `docs/runbooks/step10-dashboard.md`.

- 순수 모듈(streamlit 을 import 하지 않음): `config.py` · `timefmt.py` · `contract.py`(쓰는 API 필드 선언, OpenAPI 스냅샷과 대조) · `client.py`(캐시·single-flight·분당 상한) · `freshness.py` · `shape.py` · `holidays.py`
- 화면: `app.py` + `views/` — 개요, 지도, 혼잡(A), 돌발 영향(B), 버스(C), 날씨(D), 이상·건강(E·F)
- 표시 원칙: 값이 null 이면 0·"원활"로 바꾸지 않고 "판정 불가(표본 부족)"로 말한다. 시각은 KST, "현재"는 최대 약 80분 지연.
- 실행: `scripts/stack.sh up-dashboard` → http://127.0.0.1:8501 · Tableau CSV: `scripts/export_tableau.py`
