"""화면(페이지). 각 모듈은 load(client, now) 로 데이터를 모으고 render(data, now) 로 그린다.
load 는 streamlit 을 쓰지 않는다 → 호출 수 시험(tests/dashboard/test_views_budget.py)이 화면 없이 센다.
(폴더 이름을 pages 로 하지 않는다: streamlit 이 pages/ 를 자동 페이지로 취급한다.)"""
