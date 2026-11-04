# sql

PostgreSQL 17 + PostGIS DDL. **번호 순서 마이그레이션**이며 `scripts/migrate.py` 가 적용한다 (ADR-0008 ⑦).

| 파일 | 내용 |
|---|---|
| 001_base.sql | postgis, 스키마 dim/fact/mart/ops, `ops.load_run` |
| 002_dims.sql | `dim_link`(선, 4326), `dim_date`(공휴일은 전부 false — 데이터 미확보), `dim_weather_category` |
| 003_facts.sql | fact 6개(PK = Silver `dedup_key` 등 자연키) + `ops.call_audit` |
| 004_mart.sql | `mv_link_baseline`(링크×요일×KST시각), 읽기 뷰 3개, `mart.refresh_all()` |
| 005_roles.sql | gold_writer / api_reader / bi_reader (NOLOGIN — 비밀번호는 운영자가 지정) |

규칙: 이미 적용된 파일은 **수정하지 않는다**(체크섬 검사로 중단). 고칠 때는 다음 번호 파일을 추가한다.
`initdb.d` 는 쓰지 않는다. 이미 떠 있는 DB 에도 같은 명령으로 적용된다.

```zsh
python scripts/migrate.py --dry-run
```
