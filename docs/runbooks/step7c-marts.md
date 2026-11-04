# Step 7c 런북 — 돌발 영향(문제 B) · 날씨×교통(문제 D) · 창고 관리

결정 근거: `docs/adr/0009-step7-airflow.md` §6. 미확인·남은 일: `docs/DEFERRED.md` D-109~D-113.
**검증 상태.** 샌드박스(Postgres 16, Airflow 3.3.0)에서 통합 테스트·변이 시험·`airflow dags test` 통과. **Mac 의 실제 데이터에서 값이 나오는지는 미확인**입니다.

## 1. 반영

```zsh
unzip -o ~/Downloads/urbanstream_full.zip -d . && chmod +x scripts/*.sh && python scripts/migrate.py
```

`적용함: 007_mart_7c.sql`(이미 했다면 `008_grant_baseline.sql`)이 나와야 합니다. 008 은 기준선 읽기 권한입니다 — 없으면 문제 B·D DAG 가 `permission denied for materialized view mv_link_baseline` 로 재시도합니다(D-114). Airflow 컨테이너는 다시 만들 필요가 없습니다.

## 2. DAG 켜기 — 목록에 보인 뒤에 켭니다

새 DAG 를 올린 직후에는 1분쯤 걸립니다. 아래가 3줄을 보여 줄 때까지 기다린 뒤 켜세요(먼저 켜면 "No paused DAGs were found" 가 나오고 수동 실행이 `queued` 로 멈춥니다).

```zsh
docker exec urbanstream-airflow-scheduler airflow dags list 2>&1 | grep -E "incident_impact_finalize|weather_traffic_analysis|warehouse_maintenance"
```

```zsh
for d in incident_impact_finalize weather_traffic_analysis warehouse_maintenance; do docker exec urbanstream-airflow-scheduler airflow dags unpause $d; done
```

## 3. 수동 실행

`gold_daily` 가 성공한 적재를 가진 상태여야 합니다(최근 36시간). 순서대로 하나씩:

```zsh
docker exec urbanstream-airflow-scheduler airflow dags trigger weather_traffic_analysis
```

```zsh
docker exec urbanstream-airflow-scheduler airflow dags trigger incident_impact_finalize
```

```zsh
docker exec urbanstream-airflow-scheduler airflow dags trigger warehouse_maintenance
```

1~2분 뒤 상태 확인:

```zsh
for d in weather_traffic_analysis incident_impact_finalize warehouse_maintenance; do docker exec urbanstream-airflow-scheduler airflow dags list-runs $d 2>&1 | head -4; done
```

## 4. 결과 확인

```zsh
docker exec urbanstream-postgres psql -U urbanstream -d urbanstream -c "select (select count(*) from mart.weather_traffic_hourly) wx_rows, (select count(*) from mart.weather_traffic_hourly where sufficient) wx_ok, (select count(*) from mart.incident_impact) inc_rows, (select count(*) from mart.incident_impact where match_kind<>'no_link') inc_linked, (select count(*) from mart.incident_impact where sufficient) inc_ok, (select count(*) from ops.table_stats) stats_rows"
```

```zsh
docker exec urbanstream-postgres psql -U urbanstream -d urbanstream -c "select * from mart.v_weather_effect order by 1"
```

```zsh
docker exec urbanstream-postgres psql -U urbanstream -d urbanstream -c "select schema_name||'.'||table_name t, n_live_est, n_dead_est, pg_size_pretty(total_bytes) size from ops.table_stats where snapshot_date=(select max(snapshot_date) from ops.table_stats) order by total_bytes desc limit 8"
```

기대하는 모양:
- 날씨: 비가 거의 없으면 `none` 만 보이고 나머지 등급이 없는 것이 정상입니다(`insufficient` 가 true 이면 읽지 말 것).
- 돌발: 해제된 지 24시간 안 된 에피소드가 많으면 `inc_rows` 가 0 이어도 정상입니다. `inc_linked` 가 0 에 가까운 것도 수집 링크가 4개뿐이라 예상된 결과입니다.
- 표 통계: 가장 큰 표가 `fact.bus_position` 등이고 `n_dead_est` 가 `n_live_est` 에 비해 작아야 합니다.

## 5. 알려 주세요

위 3개 결과(숫자 한 줄, 날씨 뷰, 표 통계)를 새 채팅에 붙여 주세요. 첫 실행 때 "기준선이 아직 채워지지 않았습니다" 오류가 나면 `gold_daily` 를 한 번 수동 실행하면 됩니다.

## 6. 008 적용 후 다시 돌리기 (11-04 Mac 에서 권한 오류가 난 경우)

이미 재시도 중인 실행은 20분 간격 재시도에서 저절로 성공합니다. 바로 확인하려면 재시도 대기를 건너뛰고 새로 실행하세요. 표 통계의 옛 날짜 행(밀린 실행이 낡은 날짜로 남긴 것)은 지웁니다.

```zsh
docker exec urbanstream-postgres psql -U urbanstream -d urbanstream -c "delete from ops.table_stats where snapshot_date < (now() at time zone 'Asia/Seoul')::date"
```
