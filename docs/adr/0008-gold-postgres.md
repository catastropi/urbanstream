# ADR-0008: Step 6 — Gold(PostgreSQL) 설계 결정

- 상태: 채택 (2026-11-04, 사용자 "전부 승인 + 앞뒤 단계 호환 점검" 지시 후).
- 관련: ADR-0005(Bronze), ADR-0007(Silver), `docs/DEFERRED.md`, `sql/`, `batch/`.

## 1. 결정 요약

| # | 결정 | 이유 | 앞·뒤 단계 호환 |
|---|---|---|---|
| ① | **도로소통 수집 링크 4개, 450초 간격** (기존 1220003800 + 노선 753 위 3개: 1110014200, 1120005200, 1190000700). 하루 호출 4×86400/450 = **768회** | 서울 API 는 `link_id` 필수(없으면 ERROR-301) → 전체망 수집 불가. 일일 한도는 **미확인**(1,000 으로 가정, 80% 계획) | 수집기: 링크별 스케줄(기존 `link_ids` 지원). Silver: 변경 없음. 베이스라인(문제 A)은 링크×요일×시간대 |
| ② | 수집기는 당분간 Mac 에 둔다 | 잠자기 공백(D-09)은 감사 로그의 성공 호출로 구분 | Step 8 알림 대상 |
| ③ | Gold 는 Mac 의 PostgreSQL 17 + PostGIS. 스키마 `dim` `fact` `mart` `ops` | 포트폴리오 범위, 비용 0 | Step 9 API 는 `api_reader`(읽기 전용), Step 10 BI 는 `bi_reader`(mart 뷰만) |
| ④ | **파티션 보류(철회)** | 하루 ≈ 수만 행, 1년 후에도 수천만 이하. 선언적 파티션은 PK/ON CONFLICT 제약만 늘린다 | 필요해지면 새 마이그레이션 (D-70) |
| ⑤ | 적재 = Spark 배치(`spark-gold`, DB 비밀 없음) → `data/gold_stage/` Parquet → 호스트 Python 로더(psycopg COPY → 임시 표 → `INSERT … ON CONFLICT`) | Spark 컨테이너에 DB 비밀번호를 주지 않는다. JDBC 드라이버 불필요. 로더는 Airflow(Step 7)에서 그대로 호출 | 날짜 범위 인자 + 멱등(재실행 안전) |
| ⑥ | PK = Silver 자연키(`dedup_key`) | 재적재가 중복을 만들지 않는다(2차 방어선) | 재적재·백필 안전 |
| ⑦ | 마이그레이션 = `sql/NNN_*.sql` + `scripts/migrate.py`(체크섬 표). `initdb.d` 사용 안 함 | 이미 떠 있는 Postgres 에도 적용 가능, 변경 이력 추적 | CI(Step 11)에서 빈 DB 에 전체 적용 테스트 |
| ⑧ | 돌발 수명(발생~해제)은 **성공·비절단 호출 감사 로그** 기준. 벽시계 기준 금지 | Mac 잠자기 공백이 대량 해제로 오인됨. `expected_clear_at` 과 `absent_since` 분리 | D-38 미확인 유지 |

## 2. 같이 정한 수집 계약 수정 (6-0)

- 버스위치 `headerCd=4`(결과 없음)와 기상청 `resultCode 03`(NODATA)은 실패가 아니라 **`empty`**: 재시도·서킷 실패 집계에 넣지 않는다. 기상은 `empty` 일 때 `last_kma_base` 를 갱신하지 않아 다음 주기에 다시 조회한다.
- 두 응답의 원문은 **재구성한 픽스처**(`reconstructed`)이며 실제 원문과 다를 수 있다 → D-42.

## 3. 한계 (미확인)

- 서울 API 일일 호출 한도 1,000 은 **가정**이다(`budget_basis: unknown`).
- 개발 샌드박스는 PostgreSQL 16 / PostGIS 3.4, Mac 은 17 / 3.6 → 버전 차이는 Mac 에서 `migrate.py` 한 번으로 확인(검증 전).
- 링크 지오메트리는 LINK_VERTEX_2025(EPSG:5181)에서 오며 파일은 git 에 넣지 않는다(공공누리 1유형, `data/reference/`).
