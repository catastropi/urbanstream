# api-catalog

Step 0 에서 **실제로 호출해 확인한 것**을 소스별로 정리한 문서. 설계 문서가 아니라 관측 기록이다.
계약(`src/urbanstream/contracts/`)의 근거이며, 계약의 payload 필드는 이 문서에 모두 설명돼 있어야 한다
(`tests/contract/test_envelope_and_schema.py::test_catalog_documents_every_payload_field_and_rules` 가 검사한다).

## 소스 목록

| source_system | 문서 | 스키마 | 인증 키 | 형식 | 갱신 주기 | 한도 |
|---|---|---|---|---|---|---|
| `seoul_traffic` | [seoul_traffic.md](seoul_traffic.md) | traffic_speed_v1 | SEOUL_API_KEY | XML | 미확인 | 미확인 |
| `seoul_incident` | [seoul_incident.md](seoul_incident.md) | incident_v1 | SEOUL_API_KEY | XML | 미확인 | 미확인 |
| `bus_position` | [bus_position.md](bus_position.md) | bus_position_v1 | DATA_GO_KR_KEY | XML | 미확인 | 일 1,000건 가정 |
| `bus_arrival` | [bus_arrival.md](bus_arrival.md) | bus_arrival_v1 | DATA_GO_KR_KEY | XML | 약 20초 (mkTm 초 단위, 2026-11-03) | 일 1,000건 가정 |
| `kma_ncst` | [kma_ncst.md](kma_ncst.md) | weather_observation_v1 | DATA_GO_KR_KEY | JSON | 매시 정각 발표 | 일 10,000건 |

## 표기 규칙

- **미확인** = 이 프로젝트에서 직접 확인하지 못했다. 추측으로 채우지 않는다. 확인하면 근거(날짜, 샘플)와 함께 바꾼다.
- **가정** = 코드나 다른 자료가 그렇게 전제하지만 공식 확인은 안 됐다.
- **추정** = 관측한 값에서 합리적으로 짐작했지만 정의를 확인하지 못했다.
- 날짜·시각은 모두 KST. 샘플은 `tests/fixtures/recorded/` (호출 맥락은 `manifest.json`).

## 소스가 서로 다른 것 (한눈에)

| | 도로소통 | 돌발 | 버스위치 | 버스도착 | 기상 |
|---|---|---|---|---|---|
| 소스가 주는 event 시각 | 없음 | 발생 시각 | 차량별 GPS 시각 | 초 단위 생성 시각(mkTm) | 발표 시각 |
| 한 호출의 행 단위 | 링크 | 돌발 | 차량 | 정류소 (슬롯 2개) | 카테고리 |
| 오류 응답 모양 | `<RESULT>` / `<response>` | 같음 | HTTP 401 JSON, 게이트웨이 JSON | 같음 | JSON `resultCode` |
| 좌표 | 없음 (link_id) | TM(EPSG:5181) + link_id | 위경도 + TM | 없음 (정류소 ID) | 격자 nx, ny |

## 공통 계약 (envelope)

모든 레코드는 같은 envelope 를 가진다 (필드 정의: `src/urbanstream/contracts/envelope.py`, 결정 근거: [ADR-0001](../adr/0001-envelope-event-time-dedup.md)).

`event_id, source_system, schema_version, event_time, fetched_at, ingested_at, ingest_id, raw_uri, dedup_key, http_status, latency_ms` + `payload`

`ApiStatus.kind`: `ok`, `empty`, `auth`, `param`, `no_service`, `rate_limited`, `server_error`, `http_error`, `malformed`, `unknown`.
관측하지 못한 오류 코드는 추측해서 분류하지 않고 `unknown` 으로 두며 `code`/`message` 를 보존한다.

## 소스 사이의 연결 (검증한 것)

- 버스위치 `nextStId` ↔ 버스도착 `stId`: 12/12 일치
- 버스위치 `vehId` ↔ 버스도착 `vehId{n}`: 13/13 일치
- 버스위치 gps ↔ pos 좌표: EPSG:5181, 최대 오차 약 6 m (13점)
- 돌발 `grs80tm_x/y`: EPSG:5181 로 변환하면 설명문의 지명 위치와 일치 (10건)
- 도로소통 `link_id` ↔ 돌발 `link_id`: **미확인** (링크 지오메트리 확보 후 검증)
