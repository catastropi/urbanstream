# Step 12 증거 묶음 (Azure 최소 증거 배포)

런북 `docs/runbooks/step12-cloud.md` 를 따라 실행한 뒤 여기에 모은다. **비밀·구독 ID·이메일·내 공인 IP 는 가린다** (캡처는 모자이크, 로그는 아래 sed).

| 파일(예) | 내용 | 상태 |
|---|---|---|
| `01_plan.txt` · `02_apply.txt` · `03_destroy.txt` | `terraform plan/apply/destroy` 출력(가린 것) | [미확인] — 사용자 실행 전 |
| `04_ps.png` | `scripts/cloud.sh ps` (postgres·api·dashboard healthy) | [미확인] |
| `05_api_docs.png` · `06_dashboard_*.png` | SSH 터널로 본 API `/docs` 와 대시보드 화면(스냅샷 촬영 시각 포함) | [미확인] |
| `07_cost.png` | Cost Management 비용 그래프 | [미확인] |
| `08_summary.md` | 크기·가동 시간·비용·스냅샷 시각 표 | [미확인] |
| `09_destroyed.txt` | destroy 뒤 `az group exists` = false | [미확인] |

가림 명령(로그 파일에 적용):

```zsh
sed -E 's#/subscriptions/[0-9a-fA-F-]{36}#/subscriptions/<SUB>#g; s#[0-9]{1,3}(\.[0-9]{1,3}){3}#<IP>#g' 입력.txt > 출력.txt
```

README 에는 "서빙 계층(DB·API·대시보드)을 Azure VM 에 올려 **Mac 에서 모은 스냅샷 데이터**로 시연하고 destroy 했다"고 있는 그대로 쓴다. 스트리밍·배치는 Mac 에서 돈다.
