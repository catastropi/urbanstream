# Step 7b 런북 — 혼잡 단계 임계값(문제 A)·버스 도착 드리프트/구간 통과 시간(문제 C)

결정 근거: `docs/adr/0009-step7-airflow.md` §5. 미확인·남은 일: `docs/DEFERRED.md` D-103·D-105~D-108.
**검증 상태.** 샌드박스(Postgres 16, Airflow 3.3.0)에서 통합 테스트·변이 시험·`airflow dags test` 통과. **Mac 의 실제 데이터에서 값이 나오는지는 미확인**입니다.

## 1. 반영 (이미 Airflow 가 떠 있는 상태)

zip 을 프로젝트 위에 풀고(`unzip -o ~/Downloads/urbanstream_full.zip -d .`), 실행 권한을 되돌립니다.

```zsh
chmod +x scripts/*.sh
```

새 마이그레이션 006(표 3개·뷰 4개·권한)을 적용합니다. Airflow 컨테이너는 다시 만들 필요가 없습니다(`dags/`·`batch/` 가 마운트되어 있어 DAG 프로세서가 새 파일을 읽습니다).

```zsh
python scripts/migrate.py
```

`적용함: 006_mart_7b.sql` 이 나와야 합니다.

## 2. DAG 켜기

```zsh
docker exec urbanstream-airflow-scheduler airflow dags unpause bus_delay
```

```zsh
docker exec urbanstream-airflow-scheduler airflow dags unpause baseline_build
```

화면 Dags 목록에 5개(`backfill`, `baseline_build`, `bus_delay`, `gold_daily`, `gold_intraday`)가 보이는지 확인합니다. 새 DAG 는 1분 안에 나타납니다.

## 3. 수동 실행 (오늘 바로 확인하려면)

`bus_delay` 는 **어제~오늘을 덮는 성공한 적재**가 있어야 시작합니다. `gold_intraday` 는 오늘만 덮으므로 부족합니다. 그래서 `gold_daily` 를 먼저 한 번 수동으로 돌립니다(약 2~3분).

```zsh
docker exec urbanstream-airflow-scheduler airflow dags trigger gold_daily
```

끝난 뒤(`list-runs gold_daily` 가 `success`):

```zsh
docker exec urbanstream-airflow-scheduler airflow dags trigger bus_delay
```

```zsh
docker exec urbanstream-airflow-scheduler airflow dags trigger baseline_build
```

선행 조건이 없으면 `성공한 적재(gold_load)가 ... 없습니다` 로 실패하고 재시도합니다(정상 동작, 20분 간격 3회). 자동 실행은 매일 01:00 `gold_daily` → 02:00 `bus_delay`, 일요일 02:30 `baseline_build` 순서입니다.

## 4. 결과 확인

```zsh
docker exec urbanstream-postgres psql -U urbanstream -d urbanstream -c "select 'threshold' t, count(*), count(*) filter (where sufficient) ok from mart.congestion_threshold union all select 'drift', count(*), count(*) filter (where sufficient) from mart.bus_arrival_drift union all select 'section', count(*), 0 from mart.bus_section_time"
```

```zsh
docker exec urbanstream-postgres psql -U urbanstream -d urbanstream -c "select count(*), min(traversal_sec), max(traversal_sec) from mart.bus_section_time"
```

```zsh
docker exec urbanstream-postgres psql -U urbanstream -d urbanstream -c "select link_id, speed_kmh, level, threshold_provisional from mart.v_traffic_level"
```

- 지금은 데이터가 1.5일치라 `threshold` 의 `ok`(충분한 칸)가 0이어도 정상입니다. 같은 요일·시각에 20건이 모여야 칸이 채워지므로 며칠~몇 주가 걸립니다. `level` 이 비어 있는 것도 정상입니다(표본 부족은 계산하지 않음).
- **가장 알고 싶은 것**은 두 번째 명령입니다. 구간 통과 시간이 **0건**이거나 값이 비정상(몇 초 / 몇 시간)이면 `sect_ord` 가정(D-103)이 틀린 것이니 결과를 알려 주세요.
- 드리프트가 0건이면 `fact.bus_arrival` 에 `arrive_code` 가 2 가 아닌 행이 있는지 확인이 필요합니다.

## 5. 알려 주세요

1. `migrate.py` 출력, DAG 3개 수동 실행 상태(`list-runs`).
2. 4번의 세 결과.

## 6. Mac 실측 참고값과 주의 (11-04)

- 실측: 임계값 38칸(충분 14, 전부 잠정) · 드리프트 7,463건(충분 6,979, 평균 예측 15.1개, p10 −153 / p50 +32 / p90 +262초) · 구간 통과 4,815건(102개 구간, p50 100초, p90 257초).
- 드리프트는 중앙값이 +32초로 0에 가깝고 양의 쪽 꼬리가 더 길다(예측이 시간이 갈수록 늦어지는 경향). 시간표 대비 지연이 아니다(D-105).
- **새 DAG 를 올린 직후에는 `dags list` 에 보일 때까지 기다린 뒤 unpause 한다.** 그 전에 하면 "No paused DAGs were found" 가 나오고, 수동 실행이 paused DAG 위에서 `queued` 로 멈춘다.
- DAG 를 켜면 가장 최근에 놓친 구간 실행이 한 번 생긴다(예: `baseline_build` 2026-10-31T17:30 UTC). 업스트림 검사에서 실패할 수 있으나 무해하며 다음 정상 실행에서 수렴한다.
