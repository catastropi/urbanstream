# logging (예약된 자리 — 코드 없음)

공통 구조화 로깅 모듈을 둘 계획이었으나 만들지 않았다. 이름이 표준 라이브러리 `logging` 과 겹쳐 혼동 위험이 있어
이 폴더는 패키지(`__init__.py`)로 만들지 않았다.

- 인증키 마스킹: `collector/` 의 마스킹, `urbanstream.notify` 의 `_mask.py`, API 의 JSON 로그(`api/main.py`)가 각자 처리한다.
- 공통화는 후속 과제로 `docs/DEFERRED.md` 에 둔다.
