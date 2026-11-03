# contracts

API 응답 → envelope + payload 레코드로 이어지는 "계약(schema)"과 파서. **Step 1 에서 구현됨.**

| 파일 | 책임 |
|---|---|
| `envelope.py` | 모든 소스 공통 envelope(11개 필드) + `Record`(envelope + payload) |
| `base.py` | `CallContext`(호출 맥락), `ApiStatus`, `ParseResult`, `Reject`, HTTP/게이트웨이 오류 처리 |
| `common.py` | 시각·숫자·문자열 파싱, 결정적 `event_id`, 직렬화 |
| `seoul.py`, `seoulbus.py` | 서울 열린데이터광장 / 서울 버스 XML 응답 공통 처리 |
| `traffic.py` `incident.py` `bus_position.py` `bus_arrival.py` `weather.py` | 소스별 payload(v1)와 파서 |
| `schema.py`, `registry.py` | dataclass 에서 스키마 추출, 소스 목록과 event_time/dedup_key 규칙 |

규칙
- 파서는 네트워크를 쓰지 않는다: `parse(body, CallContext, **kw) -> ParseResult`. 호출은 Collector 의 일이다.
- 잘못된 행은 `Reject` 로 모으고 호출 전체를 버리지 않는다. 오류 응답은 예외가 아니라 `ApiStatus` 로 표현한다.
- 스키마는 `tests/contract/snapshots/*.json` 로 고정돼 있다. payload 를 바꾸면 테스트가 실패한다 → 의도한 변경이면 `python scripts/export_schemas.py` 로 스냅샷을 갱신하고, 호환되지 않으면 `schema_version` 을 올리고 ADR 을 쓴다.
- 새 응답 샘플(특히 새 오류 모양)을 관측하면 `tests/fixtures/recorded/` 에 넣고 `manifest.json` 에 등록한다.

결정 근거: `docs/adr/0001-envelope-event-time-dedup.md`, `docs/adr/0002-contracts-stdlib-dataclasses.md`
필드 설명과 미확인 목록: `docs/api-catalog/`

Blueprint 참고: I절(Bronze/Silver/Gold), 10-9(Schema Evolution)
