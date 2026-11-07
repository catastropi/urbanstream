# tableau

Tableau 로 분석할 때 쓰는 **API 기반 CSV 추출** 폴더 (Step 10, 결정 D10-8: 옵션 A).

- 사용자는 Tableau Desktop 이 없고 **Tableau Public** 을 쓴다. Tableau Public 은 DB(PostGIS)에 직접 연결할 수 없으므로 CSV 파일로 가져온다.
- 데이터 입구는 대시보드와 같은 **FastAPI(X-API-Key)** 하나다. DB 에 직접 붙지 않는다.
- 생성물은 `tableau/export/` 에 쓰이며 git 에 올라가지 않는다(`.gitignore`).

## 만드는 법

```
scripts/stack.sh up-api
python scripts/export_tableau.py --preview
python scripts/export_tableau.py
```

`--preview` 는 파일을 쓰지 않고 행 수와 앞부분만 보여 준다. `--days N`(최대 28)으로 기간을 늘린다.
키는 `.env` 의 `DASHBOARD_API_KEY` 를 쓴다(값은 출력되지 않는다).

## 나오는 파일 (집계만)

| 파일 | 내용 | 단위 |
|---|---|---|
| `link_speed_hourly.csv` | 링크별 시간당 평균·최소·최대 속도 | 링크 × KST 날짜 × KST 시 |
| `congestion_threshold.csv` | 평소 속도 기준선(p10/p25/p50/p90) | 링크 × 요일 구분 × KST 시 |
| `incident_impact_by_type.csv` | 돌발 유형별 영향 요약 | 유형 |
| `weather_effect.csv` | 강수 구분별 속도 비율(평소=1.0) | 강수 구분 |
| `bus_arrival_drift_hourly.csv` | 버스 도착예측 변화(마지막−처음, 양수=늦어짐) | KST 날짜 × 시 × 노선 × 정류장 |
| `manifest.json` | 행 수·sha256·API 기준시각·불완전 여부·**게시 전 점검표** | |

모든 CSV 의 첫 열은 `snapshot_at`(UTC). `kst_date`·`kst_hour`·`event_date` 는 한국 시간.
차량번호·차량 ID·운영 지표(DQ·알림·소스 상태)는 **내지 않는다**.

## 해석 주의

- 기준선은 **잠정**(약 11월 말까지 표본 부족) — `provisional` 열을 필터로 남기고 제목에 "잠정"을 쓴다.
- 속도 `null` 은 빈 칸이다(0 아님). `n_points` 가 0 이면 그 시간은 값이 없는 것이다.
- `day_type_guess` 는 평일/토/일 근사(공휴일은 평일로 근사). 정확한 구분은 `congestion_threshold.day_type`.
- 날씨 효과는 **연관이지 인과가 아니다**. `insufficient=true` 구간은 해석하지 않는다.
- 버스 드리프트는 시간표 지연이 아니라 **예측 도착시각의 변화**다.

## 게시 (사용자가 직접)

이 스크립트는 **업로드하지 않는다.** Tableau Public 에 올리면 **누구나 보고 내려받을 수 있다.**
올리기 전에 `manifest.json` 의 `publish_checklist` 를 확인하고, 원천 데이터 이용 조건과 Tableau Public 약관을 직접 확인한다.
(이 프로젝트에서는 두 가지 모두 **미확인** 이다 — DEFERRED 참고.)

Blueprint 참고: R절(Tableau 구성)
