"""데이터 품질(DQ) 엔진 (Step 8b, ADR-0010 D8-9). 규칙은 config/dq_rules.yaml, 결과는 ops.dq_result.

이 패키지는 import 시점에 psycopg 를 부르지 않는다(DAG 파싱이 가볍게). 연결은 호출하는 쪽(batch/dq_jobs.py)이 넘긴다.
"""
