# kma_ncst — 기상청 초단기실황

계약: `weather_observation_v1` · 파서: `src/urbanstream/contracts/weather.py` · 샘플: `tests/fixtures/recorded/kma_ncst/`

## 개요

| 항목 | 내용 | 근거 |
|---|---|---|
| 공식명 | 기상청_단기예보 조회서비스 (data.go.kr) — 오퍼레이션 `getUltraSrtNcst` | 승인 목록 |
| 쓰이는 문제 | D 기상-교통 결합 (상관과 인과를 구분) | Blueprint |
| 인증 | `DATA_GO_KR_KEY` | 호출 성공 |
| 호출 | `http://apis.data.go.kr/1360000/VilageFcstInfoService_2.0/getUltraSrtNcst` + `serviceKey, pageNo, numOfRows, dataType=JSON, base_date, base_time, nx, ny` | 2026-10-28 15:09 PASS |
| 응답 형식 | JSON (`response.header` / `response.body.items.item[]`) | 관측 |
| 호출 한도 | **일 10,000건** (응답 헤더에서 확인) | 관측 |
| 갱신 주기 | 매시 정각 기준 발표. **정각 후 5~6분에 조회 가능**(2026-11-03 16시 발표분, 1분 간격 폴링: +0~5분 `resultCode 03 NO_DATA`(HTTP 200), +6.0분 `00`·8건). 1회 측정이라 시간대별 변동은 미확인. 수집기는 여유를 두어 15분 뒤부터 부름(`kma_available_after_min`). 11-02 첫 측정은 Mac 잠자기로 무효 | 2026-11-03 |
| 호출 지연 | 113 ms | 리포트 |
| 기본 격자 | nx=60, ny=127 (서울시청 부근). `.env.example` 의 값이며 좌표변환 자료로 재확인 권장 | `.env.example` |

## 응답 구조 (관측)

격자 1곳, 발표 시각 1개당 카테고리마다 1행, 8행:

| category | 의미 | 단위 | 샘플 값 (2026-10-28 14:00 발표) |
|---|---|---|---|
| `T1H` | 기온 | ℃ | 14.1 |
| `RN1` | 1시간 강수량 | mm | 0 |
| `UUU` | 동서바람성분 | m/s | 2.9 |
| `VVV` | 남북바람성분 | m/s | 0.3 |
| `REH` | 습도 | % | 17 |
| `PTY` | 강수형태 | 코드 | 0 |
| `VEC` | 풍향 | deg | 264 |
| `WSD` | 풍속 | m/s | 2.9 |

카테고리 이름·단위는 기상청 활용가이드 기준이고, 8개 카테고리의 존재는 샘플로 확인했다. PTY 코드표(0 없음, 1 비, 2 비/눈, 3 눈, 5 빗방울, 6 빗방울눈날림, 7 눈날림)는 문서 기준이며 관측은 0 뿐이다.

## 시간·공간 정보

- 시간: `baseDate` + `baseTime` (예: `20261028` + `1400`, KST) = **발표(관측 기준) 시각**. 호출 시각이 아니다 (샘플은 15:09 에 호출해서 14:00 자료를 받았다).
- 공간: 격자 `nx, ny`. 위경도와의 대응은 기상청 격자 변환식으로 별도 확인한다.

## 오류·장애

| 상황 | 응답 | ApiStatus.kind |
|---|---|---|
| 정상 | `resultCode` = `00` | `ok` (item 없으면 `empty`) |
| 그 밖의 `resultCode` | **미관측** | `unknown` (code/message 보존) |
| 게이트웨이 오류 | `OpenAPI_ServiceResponse` JSON/XML | `no_service` 또는 `unknown` |
| JSON 이 아닌 본문 | — | `malformed` |

JSON 요청이어도 인증 오류가 XML 로 올 수 있다는 점은 알려져 있으나 이 프로젝트에서는 관측하지 못했다. 파서는 둘 다 처리한다.

## 계약 (weather_observation_v1)

- event_time: baseDate + baseTime (발표 시각, KST)
- dedup_key: nx | ny | category | YYYYMMDDHHMM
- source_system: `kma_ncst`
- 새 카테고리가 나타나도 reject 하지 않고 `category_name`/`unit` 만 None 으로 둔다.

| payload 필드 | 원본 필드 | 타입 | 설명 |
|---|---|---|---|
| `nx` | `nx` | long | 격자 x (필수) |
| `ny` | `ny` | long | 격자 y (필수) |
| `category` | `category` | string | 위 표의 카테고리 코드 (필수) |
| `value_raw` | `obsrValue` | string | 응답 문자열 그대로 |
| `value` | `obsrValue` | double? | 숫자로 읽을 수 있고 결측 표기(-999)가 아닐 때만 |
| `category_name` | (사전) | string? | 한글 이름. 모르는 카테고리면 None |
| `unit` | (사전) | string? | 단위. 모르는 카테고리면 None |

## 측정 예정 / 미확인

- [x] 정각 후 조회 가능 시각 — 5~6분 (2026-11-03). [ ] 다른 시간대·비 오는 날 재측정은 선택
- [ ] `resultCode 03`(NO_DATA)은 정시 직후의 정상 상태인데 파서는 `00` 이외를 `unknown` 으로 두어 수집기가 재시도 3회+DLQ+회로차단 실패로 처리한다 → 15분 여유로 회피 중. 개선 후보: 03 을 '아직 발표 전'(오류 아님)으로 분류
- [ ] `RN1` 이 비 올 때 보내는 값의 모양 (`'강수없음'` 같은 문자열 여부) — 비 오는 날 1회 관측 필요. 파서는 숫자가 아니면 `value=None`, `value_raw` 보존
- [ ] 수집할 격자 범위 (1차는 60,127 하나)
- [ ] 결측 표기 `-999` 가 실제로 오는지
