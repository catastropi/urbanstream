# Step 6 런북 — Gold (PostgreSQL)

결정 근거: `docs/adr/0008-gold-postgres.md`. 미확인·남은 일: `docs/DEFERRED.md` F장(D-70~D-80).
프로젝트 루트에서 `conda activate urbanstream`, Docker Desktop 켠 상태. 명령은 한 줄씩 복사하세요.
**검증 상태:** 개발 환경(PostgreSQL 16 / PostGIS 3.4, Java 21)에서만 테스트했습니다. 아래 Mac 실행 결과는 아직 **미확인**입니다.

## 0. 먼저 (3분) — git 에 data/ 가 올라갔는지 확인

`.gitignore` 의 기존 규칙이 `data/` 안쪽 파일을 막지 못하는 결함이 있었습니다(이번에 수정). 아래 결과가 **비어 있어야** 합니다.

```zsh
git ls-files data | head
```

나오면 알려 주세요(push 이력 정리 방법이 달라집니다). 로컬 추적만 풀 때는: `git rm -r --cached data` 후 커밋.

## 1. 준비

```zsh
pip install -r requirements.txt
```

```zsh
docker compose --profile core --profile streaming build spark-bronze
```

`batch/` 코드가 이미지에 들어가므로 빌드가 필요합니다(D-56). 돌고 있는 Bronze/Silver 는 재시작되지 않습니다.

`.env` 에 `POSTGRES_HOST=127.0.0.1` 은 기본값이라 안 적어도 됩니다. 포트는 `POSTGRES_PORT`(5433).

## 2. 마이그레이션

```zsh
python scripts/migrate.py --dry-run
```

```zsh
python scripts/migrate.py
```

기대: `적용함: 001_base.sql, … 005_roles.sql`, 다시 실행하면 `없음 (이미 최신)`. 이미 적용된 파일을 고치면 중단됩니다(새 번호 파일로).

## 3. 링크 시드

`LINK_VERTEX_2025.xlsx` 를 `data/reference/` 에 둔 뒤:

```zsh
python -m batch.seed_dim_link data/reference/LINK_VERTEX_2025.xlsx
```

기대: `{'loaded': 5093, 'skipped': 0, 'collected_missing': 0}` (로드 수는 파일에 따라 다를 수 있음, `collected_missing` 은 0).

## 4. 적재 (스테이징 → Postgres)

하루치(KST):

```zsh
scripts/gold_run.sh 2026-11-04 --refresh-mart
```

기대: Spark 가 `row_counts` JSON 을 출력하고 이어서 `{"run_id": 1, "row_counts": {...}}`. 같은 명령을 다시 돌려도 DB 행 수는 늘지 않습니다.
Spark 가 읽는 Silver 는 스트리밍이 쓰는 폴더라서(읽기 전용 마운트) 처음에는 `silver_verify` 의 Silver 행 수와 대조해 보세요(D-73).

확인:

```zsh
docker compose exec postgres sh -c 'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c "select count(*) from fact.traffic_speed"'
```

## 5. (선택) 최소 권한 로그인 역할

마이그레이션은 `NOLOGIN` 역할만 만듭니다. 쓰려면 비밀번호를 **직접** 정해 터미널에서만 지정하세요(채팅·git 에 붙이지 말 것).

```zsh
docker compose exec postgres sh -c 'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB"'
```

psql 안에서: `CREATE ROLE gold_loader LOGIN PASSWORD '…' IN ROLE gold_writer;` 그리고 `.env` 에 `GOLD_WRITER_USER=gold_loader`, `GOLD_WRITER_PASSWORD=…`. 설정하지 않으면 적재는 관리자 계정으로 접속합니다.

## 6. 테스트

```zsh
GOLD_TEST_DSN="host=127.0.0.1 port=5433 dbname=ut_test user=urbanstream password=…" python -m pytest tests/integration
```

**주의:** 이 테스트는 해당 DB 의 `dim/fact/mart/ops` 스키마를 지웁니다. 운영 DB(`urbanstream`)를 가리키지 말고 테스트용 DB 를 따로 만드세요. DSN 이 없으면 건너뜁니다.
Spark 포함 전체: `scripts/spark_test.sh` (컨테이너에는 psycopg 가 없어 Postgres 테스트는 건너뜁니다).

## 되돌리기
- 적재 되돌리기: fact 는 PK 기반이라 지우고 다시 적재해도 됩니다 (`TRUNCATE fact.traffic_speed` 등 후 같은 구간 재실행).
- 스키마 통째로: `DROP SCHEMA dim, fact, mart, ops CASCADE;` 후 `migrate.py` (**데이터 삭제**).
