# dq (Data Quality)

Bronze/Silver 단계에서 실행하는 데이터 품질 규칙 엔진.

- 규칙 정의 로더 (config/dq_rules.yaml 을 읽음)
- 규칙 타입: 스키마, null, 중복, range, timestamp, freshness, 참조 무결성, 볼륨, 분포 드리프트
- 정책 레벨: hard(DLQ+알림) / soft(플래그만) / warn(기록만)
- 결과를 ops.dq_result 에 쓰는 함수

Blueprint 참고: S절(Data Quality 규칙)
