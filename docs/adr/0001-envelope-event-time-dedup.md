# ADR-0001 — envelope 필드, event_time 규칙, dedup_key 규칙

- 상태: 확정 (Step 1, 2026-11-02)
- 관련: `src/urbanstream/contracts/`, `docs/api-catalog/`

## 배경

인수인계 문서(2026-11-01)는 envelope 후보와 소스별 event_time/dedup_key 를 '제안'으로 적고 사용자 확인이 필요하다고 했다.
한편 폴더 뼈대의 `src/urbanstream/contracts/README.md`(Blueprint 기준)는 다른 이름의 envelope 를 정의하고 있었다.
두 목록을 합쳐 하나로 확정한다. 충돌 시 Blueprint 이름을 따른다는 인수인계 6장의 규칙을 적용했다.

## 결정 1 — envelope 필드 (11개 + payload)

| 필드 | 출처 | 비고 |
|---|---|---|
| `event_id` | Blueprint | `source_system` + `dedup_key` 에서 만든 결정적 UUID (uuid5) |
| `source_system` | Blueprint (인수인계의 `source`) | `seoul_traffic` 등 5개 |
| `schema_version` | Blueprint | `traffic_speed_v1` 처럼 스키마 이름+버전 |
| `event_time` | 공통 | 소스 규칙에 따름 (결정 2) |
| `fetched_at` | 공통 | 응답을 받은 시각 |
| `ingested_at` | Blueprint | 파서는 None, 수집기/Bronze 가 채움 |
| `ingest_id` | 인수인계 | API 호출 1회 = ID 1개 (lineage) |
| `raw_uri` | Blueprint (인수인계의 `raw_ref`) | 원본 응답 위치. 형식은 Step 3 에서 확정 |
| `dedup_key` | 공통 | 결정 3 |
| `http_status`, `latency_ms` | 인수인계 | 문제 F(파이프라인 감시)에 필요 |

이유
- `event_id` 를 무작위가 아니라 결정적으로 만든다: 같은 사건을 재수집·재처리해도 ID 가 같아서 멱등 처리와 중복 제거가 쉽다 (원칙 '재처리 가능').
- `ingest_id` 는 별도로 둔다: 'event 는 같은데 어느 호출에서 왔는지'는 lineage 에 필요한 정보다 (원칙 'lineage 추적').
- 모든 시각은 timezone-aware 이고 KST 오프셋(+09:00)으로 직렬화한다. 소스가 모두 KST 로 주고, 오프셋이 붙어 있으면 Spark 에서 UTC 변환이 안전하다. naive datetime 은 계약 위반으로 거부한다.

## 결정 2 — event_time (원칙 'event-time 중심')

| 소스 | event_time | 이유 |
|---|---|---|
| seoul_traffic | 수집 시각을 분 단위로 절삭 | 소스가 시각을 주지 않는다 (Step 0 확인). 분 단위로 절삭하면 같은 분 재호출이 같은 키가 된다 |
| seoul_incident | `occr_date`+`occr_time` | `HHMM`/`HHMMSS` 둘 다 받는다 (Step 1 에서 새로 확인) |
| bus_position | `dataTm` | 차량별로 다르다 (한 호출 안에서 약 20초 폭) |
| bus_arrival | `mkTm` | 초 단위(정정 2), 한 호출의 모든 행이 같다 |
| kma_ncst | `baseDate`+`baseTime` | 호출 시각이 아니라 발표 시각 |

## 결정 3 — dedup_key

| 소스 | dedup_key | 검증 |
|---|---|---|
| seoul_traffic | `link_id\|YYYYMMDDHHMM` | event_time 이 분 절삭이므로 같은 분 = 같은 키 |
| seoul_incident | `acc_id\|내용해시 8자` | 샘플 10건 `acc_id` 유일. 해제 시각·설명이 바뀌면 새 키가 되는 것을 테스트로 확인 |
| bus_position | `vehId\|dataTm` | 13/13 유일 |
| bus_arrival | `busRouteId\|stId\|staOrd\|slot\|YYYYMMDDHHMMSS` | 193/193 유일. 인수인계 제안에 `slot` 을 더했다. 초 단위로 정정(정정 2) |
| kma_ncst | `nx\|ny\|category\|YYYYMMDDHHMM` | 8/8 유일 |

인수인계 제안과 달라진 점
- 돌발: `acc_id` 단독이 아니라 `acc_id` + 내용 해시. 이유: 같은 돌발의 해제 예정 시각·설명이 바뀌어도 이력을 잃지 않으려고(원칙 '데이터는 틀릴 수 있다, 재처리 가능'). 최신 상태 선택은 Silver 의 일이다.
- 버스도착: `slot` 추가. 이유: 한 정류소 행을 버스 2대로 펼치기 때문 (결정 4).

## 결정 4 — 버스도착은 계약 단계에서 (정류소, 슬롯) 레코드로 펼친다

- 하류 단계(Silver 이후)가 모두 '버스 1대의 도착 예측' 단위로 일하고, 버스위치와 `vehId` 로 직접 조인된다 (13/13 일치).
- 빈 슬롯(`vehId` = 0)은 레코드가 아니다. reject 로 세면 DQ 지표가 오염되므로 `stats["empty_slots"]` 로 센다.
- 원본(Bronze)은 펼치기 전 그대로 보관한다.

## 결정 5 — 0 과 결측

계약은 값을 해석하지 않는다. `0` 을 결측으로 바꾸지 않는다 (`prcs_spd` 0, `traTime` 0 은 의미가 불확실). 공백뿐인 문자열(버스도착이 빈 칸을 `' '` 로 보냄)과 빈 문자열만 결측(None)으로 본다. 0 의 해석은 Silver 의 규칙이고 DQ 규칙으로 문서화한다.

## 결정 6 — 잘못된 행은 Reject 로 모으고 호출 전체를 버리지 않는다

필수 필드 누락·형식 오류인 행은 `Reject(reason, raw, index)` 로 남긴다. HTTP 오류·인증 오류·게이트웨이 오류·깨진 본문은 예외 없이 `ApiStatus` 로 돌려준다. 관측하지 못한 코드는 `unknown` 으로 두고 추측하지 않는다 (원칙 '확인 안 된 API 동작은 있다고 가정하지 않는다').

## 결과 / 되돌릴 때

- 스키마는 `tests/contract/snapshots/*.json` 으로 고정돼 있어서, 필드를 바꾸면 테스트가 실패한다. 호환되지 않는 변경은 `schema_version` 을 올리고 이 ADR 에 후속 번호로 남긴다.
- dedup 규칙을 바꾸면 Bronze 의 기존 데이터와 키가 달라진다. 데이터가 쌓이기 전(지금)이 바꾸기 가장 싼 시점이다.

## 정정 1 — `isArrive` 는 플래그가 아니라 코드값 (2026-11-02)

실제 연속 수집(12:31)에서 버스도착 슬롯 8개가 `플래그가 아님: '2'` 로 reject 됐다. `isArrive1` 이 노선 시작점 '출발대기' 행들에서 `2` 로 왔다 (샘플 2건에서는 0 뿐이었다). 조치:
- payload `is_arrive`(boolean) → `arrive_code`(int). 코드표는 미확인이라 값을 해석하지 않는다.
- 부가 플래그(`isLast`, 버스위치의 `isrunyn/islastyn/isFullFlag`)는 예상 밖 값이면 행을 버리지 않고 None 으로 두고 `stats.unexpected_flag_values` 로 센다. 이 통계는 collector 의 audit(`parse_stats`)에 남는다.
- **v1 안에서 정정한다 (schema_version 을 올리지 않음).** 이유: 아직 소비자가 없고(Bronze 미구현) Kafka 보관은 72시간이라, 정정 이전 메시지는 곧 사라진다. 소비자가 생긴 뒤의 같은 종류의 변경은 `.v2` 로 올린다.
- 교훈: 샘플 몇 건으로 값의 범위를 단정하지 않는다. 계약이 reject → DLQ → 원본 보존으로 이를 잡아냈고(설계가 의도한 동작), 연속 수집 로그의 `rejects` 를 계속 감시한다.

## 정정 2 (2026-11-03) — 버스도착 `mkTm` 은 분 단위가 아니라 초 단위다

- **관측:** 20초 간격으로 14번 호출했더니 `mkTm` 이 14번 모두 바뀌었고(저장 시각과의 차이 2~20초), 원본에서 값이 `14:40:02.0`, `14:40:19.0`, `14:40:39.0` 로 초가 달랐다. 서버가 도착 정보를 약 17~20초마다 새로 만들고 그때의 시각을 찍는다. 처음 샘플의 `21:53:00.0` 은 우연히 초가 0 이었다. 한 응답 안의 모든 행은 같은 값이다.
- **문제:** `dedup_key` 의 시각을 분까지만 썼다. 같은 분 안의 서로 다른 스냅샷이 같은 키가 되어 Silver 가 하나를 중복으로 버린다. 수집 간격이 90초일 때는 충돌이 없지만, 간격을 줄이면 조용히 데이터를 잃는다.
- **결정:** 키의 시각을 `YYYYMMDDHHMMSS`(초까지)로 바꾼다. 같은 스냅샷을 두 번 받으면 같은 키(중복 제거 유지), 다른 스냅샷은 다른 키. `event_time` 은 원래부터 초를 유지했다.
- **영향:** 이 정정 이전에 Kafka 에 들어간 `urban.bus.arrival.v1` 메시지(보관 72시간)는 옛 분 단위 키다. `event_id` 도 키에서 만들어지므로 같은 레코드의 ID 가 두 형식으로 존재할 수 있다. Silver 가 아직 없어서 소비자는 없고, 필요하면 `data/raw` 를 새 파서로 다시 발행하면 된다.
- **되돌리기:** `bus_arrival.py` 의 `second_key` 를 `minute_key` 로 되돌리고 `registry.py`·테스트·문서의 형식 표기를 원복한다.
- **같은 점검:** 버스위치는 처음부터 `vehId|dataTm`(초), 도로소통·기상청은 소스가 분/발표 단위라 변경 없음.
