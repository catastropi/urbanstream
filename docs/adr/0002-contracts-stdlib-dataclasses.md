# ADR-0002 — contracts 는 표준 라이브러리 dataclass 로 만든다

- 상태: 확정 (Step 1, 2026-11-02)

## 결정

`src/urbanstream/contracts/` 는 표준 라이브러리(`dataclasses`, `xml.etree`, `json`, `datetime`)만 쓴다. pydantic 등 외부 검증 라이브러리를 쓰지 않는다.
테스트용 `pytest` 만 `requirements-dev.txt` 에 추가했다.

## 이유

1. **쓰이는 곳이 많다.** 이 코드는 Collector(상시 프로세스), Spark 잡, Airflow 태스크, 테스트에서 모두 import 된다. 외부 의존성이 없으면 Spark 이미지·Airflow 이미지에 따로 맞출 필요가 없다 (Step 2 Docker 이미지 단순화).
2. **검증이 단순하다.** 우리 검증은 '필수 필드 있는가, 숫자/시각으로 읽히는가' 정도이고, 행 단위로 실패를 Reject 로 모으는 것이 핵심이다. 라이브러리의 예외 모델에 맞추는 것보다 직접 쓰는 편이 짧고 읽기 쉽다.
3. **스키마가 한 곳에 있다.** dataclass 정의가 단일 출처이고, `schema.py` 가 거기서 필드 이름/타입/nullable 을 뽑아 스냅샷과 비교한다. 나중에 Spark `StructType` 이나 JSON Schema 도 같은 함수에서 파생시킬 수 있다.

## 대가

- 복잡한 검증(중첩 객체, 교차 필드 규칙)이 늘면 직접 쓰는 코드가 길어진다. 그때 pydantic 도입을 다시 검토한다.
- 신뢰할 수 없는 XML 을 `xml.etree` 로 읽는다. 대상은 공공 API 응답이고 크기가 작지만(최대 약 240 KB), 악의적 입력이 들어올 여지가 생기면 `defusedxml` 로 바꾼다.
