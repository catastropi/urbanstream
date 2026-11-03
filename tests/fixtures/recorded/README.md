# recorded fixtures

실제 API 에서 받은 응답 원본 (키는 응답 본문에 들어 있지 않다). 계약 테스트의 입력이다.

- `manifest.json` — 파일별 소스, 수집 시각(KST), HTTP 상태, 기대하는 `ApiStatus.kind`, 파서에 넘길 인자.
- `err_*` 파일은 **장애 응답의 실제 모양**이다. 지우지 말고, 새로운 오류 모양을 관측하면 추가한다.
- 새 샘플 추가 방법: `data/smoke/raw/<api>/` 에서 복사 → `manifest.json` 등록 → `python -m pytest`.
- 샘플은 한 시점의 스냅샷이다. 막차 이후·심야·비 오는 날처럼 모양이 달라질 수 있는 상황의 샘플을 관측하면 추가한다.
