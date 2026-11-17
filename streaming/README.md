# streaming

Spark Structured Streaming 잡 (Blueprint O절). 이미지는 `docker/spark-base/Dockerfile`, 서비스는 compose 의 `spark-bronze`(profile `streaming`).

**지금 있는 것 (Step 4)**
- `bronze_writer.py` — Kafka 토픽 7개 → `data/bronze/<이름>/event_date=YYYY-MM-DD/` (Parquet append). 한 JVM 안에 쿼리 7개.
- `bronze.py` — 변환(`to_bronze_typed`/`to_bronze_raw`)과 쿼리 조립. 읽을 수 없는 행도 버리지 않고 `_parse_ok`/`_parse_error` 로 표시.
- `bronze_config.py` — 설정(`BRONZE_*` 환경변수)과 토픽→표 목록. pyspark 없이 호스트에서 테스트된다.
- `verify_bronze.py` — Bronze 가 Kafka 와 일치하는지(중복·유실·행 수) 대조. `scripts/bronze_verify.sh`.
- `profile_bronze.py` — (Step 5-0) Bronze 를 **읽기만 하며** 워터마크·중복·0값/결측·돌발 처리의 근거 숫자를 뽑는다. `scripts/silver_profile.sh`, 절차 `docs/runbooks/step5-silver.md`.

**Step 5 (Silver)**
- `silver_writer.py` — Bronze 5개 표 → `data/silver/<표>/` (서비스 `spark-silver`, profile `silver`). 쿼리 5개.
- `silver.py` — 변환(`transform_*`, 상태 없음)과 중복 제거(`dedup`, 상태 있음), 쿼리 조립. 입력 스키마는 Bronze 변환 함수에서 얻는다.
- `silver_config.py` + `config/silver.yaml` — 워터마크·중복 키·장기 돌발 기준(7일). pyspark 없이 테스트된다.
- `geo_tm.py` — EPSG:5181 → WGS84 (표준 라이브러리, pyproj 대조 오차 1e-13도). 돌발 좌표용.
- `silver_verify.py` — Bronze→Silver 행 수 등식·유실·초과 대조. `scripts/silver_verify.sh`.

결정 근거 `docs/adr/0005-bronze-spark.md`, `0006`, `0007`. 실행·검증 `docs/runbooks/step4-bronze.md`, `step5-silver.md`.

**만들지 않기로 한 것**
- `traffic_stream.py`·`bus_stream.py`·`health_stream.py` 는 처음 계획 이름이었다. 집계·지연·이상 탐지는 Airflow 배치(`batch/`, `dags/`)로, 파이프라인 건강은 `monitor` 컨테이너(`src/urbanstream/monitor/`)로 구현했다(ADR-0009, ADR-0010).
