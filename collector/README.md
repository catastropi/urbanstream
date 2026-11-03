# collector

실제 운영 중인 Open API 를 폴링해서 계약(`src/urbanstream/contracts/`)으로 해석하고 Kafka 로 발행하는 상시 프로세스. **Step 3 에서 구현됨.**

```zsh
python scripts/create_topics.py        # 토픽 만들기 (자동 생성이 꺼져 있다). --plan 은 접속 없이 계획만 출력
python -m collector status             # 오늘 호출 예산 사용량
python -m collector once --dry-run     # 1사이클, Kafka 로 안 보냄 (실제 API 는 호출함)
python -m collector once               # 1사이클, Kafka 로 발행
python -m collector run                # 계속 수집 (소스별 스레드, Ctrl+C 로 종료)
python -m collector run --sequential   # 예전 방식(한 줄로 순서대로) — 문제 조사용
```

| 파일 | 책임 |
|---|---|
| `config.py` | `config/sources.yaml`, `config/topics.yaml` 로딩·검증. **계획 호출 수가 한도의 80% 를 넘으면 시작 거부** |
| `requests_spec.py` | 소스별 URL·파라미터 (smoke_test 로 검증한 엔드포인트), 기상청 발표 시각 계산 |
| `fetch.py` | HTTP 호출, 키 마스킹(`Redactor`) |
| `resilience.py` | 재시도(지수 백오프), 회로차단, 일일 호출 예산(파일에 저장) |
| `rawstore.py` | 원본 응답 gzip 보관 `data/raw/<source>/<날짜>/…` |
| `publisher.py` | Kafka 발행(`confluent-kafka`, 멱등·acks=all) / 테스트용 메모리 발행기 |
| `runner.py` | 한 사이클(호출→원본→파서→발행→DLQ→audit), 스케줄러 |
| `__main__.py` | CLI |

흐름: 예산 확인 → 호출(일시 실패는 재시도) → **원본 저장** → 계약 파서 → 토픽 발행(+DLQ) → audit 1건.
audit(`platform.audit`)은 호출 시도마다 1건씩 남는다 — 문제 F(파이프라인 감시)의 입력이다.

설계 근거: `docs/adr/0004-collector-topics.md` · 실행 절차: `docs/runbooks/step3-collector.md`
Blueprint 참고: F절(Architecture), U절(장애 대응), 10-10/10-11(Rate Limit/API 장애)
