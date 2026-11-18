# Step 11 런북 — GitHub Actions CI/CD

결정 근거: `docs/adr/0013-step11-ci-cd.md`. 남은 것: `docs/DEFERRED.md` K장.
**사용자가 Mac 에서 실행하는 순서입니다.** 명령은 한 줄씩이고, 비밀 값은 채팅·git 어디에도 붙이지 않습니다(CI 는 비밀을 쓰지 않습니다).
상태 표기: 아래 "기대 결과"는 모두 **[GitHub 미확인]** 입니다(샌드박스에서는 워크플로를 실행할 수 없고, 도구·시험만 확인했습니다).

## 0. 압축 풀기

```zsh
cp ~/Downloads/urbanstream_full.zip .
```

```zsh
unzip -o urbanstream_full.zip -d .
```

```zsh
rm urbanstream_full.zip
```

zip 은 풀자마자 지웁니다(`.gitignore` 의 `*.zip` 이 git 에 올라가는 것은 막지만, 폴더에 남겨 두면 다음 압축 때 섞입니다). 11-09 에 `git add -A` 로 zip 이 추적된 적이 있습니다.

`.env` 와 `data/` 는 zip 에 없으므로 건드리지 않습니다. 이번에 바뀐 것: `.github/`(워크플로 3개·dependabot), `.gitleaks.toml`, `ruff.toml`, `pytest.ini`, `scripts/ci_*.py`·`pin_*.py`·`mutation_ci.py`, `tests/support/layers_plugin.py`, `tests/e2e/`, `tests/unit/test_{workflows,ci_checks,ci_tools,layers_plugin}.py`.

```zsh
python -m pytest -q
```

기대: 실패 0. 개수는 Mac 환경(Spark·Postgres 유무)에 따라 skip 이 달라 샌드박스와 다릅니다 — 실패 0 인지만 봅니다.

## 1. 핀 만들기 (push **전에**, 이 순서가 중요)

gitleaks 핀이 없으면 `secrets` job 이 일부러 실패합니다. 먼저 만듭니다.

```zsh
python scripts/pin_gitleaks.py
```

기대: `gitleaks <버전> 핀 기록: .github/gitleaks.pin`. 파일을 열어 `version=` 과 `sha256=`(64자 16진수)가 있는지 눈으로 봅니다. 실패하면(네트워크·GitHub 한도) 메시지를 그대로 알려 주세요.

```zsh
python scripts/pin_actions.py
```

액션을 `@v6` 같은 태그에서 40자 SHA(+ `# v6` 주석)로 바꿉니다. 끝나면 확인:

```zsh
python scripts/pin_actions.py --check
```

기대: 고정 안 된 항목 0. 못 고정한 항목이 있으면 목록이 나옵니다(그대로 두면 태그로 동작은 합니다).

```zsh
python -m pytest -q tests/unit/test_workflows.py
```

기대: 통과(시험은 40자 SHA 와 `vN` 둘 다 허용).

## 2. 커밋과 push

```zsh
git status -sb
```

`.env`·`data/` 가 목록에 **없어야** 합니다.

```zsh
git add -A
```

```zsh
git commit -m "feat(step11): CI/CD, test layers, e2e"
```

```zsh
git push
```

## 3. 첫 실행 확인 (GitHub 웹)

1. 저장소 → Actions → `ci` 의 최신 실행. job 10개(changes, hygiene, secrets, test-core, test-minimal, test-pg, mutation-api, dags, docker-build, ci-ok) 중 경로 필터로 건너뛴 것은 "skipped" 로 보입니다.
2. 빨간불이면 job 이름과 실패 단계의 로그 마지막 20줄을 알려 주세요. **로그에 비밀이 보이면 붙여넣지 말고** 알려 주세요(gitleaks 는 `--redact` 로 값을 가립니다).
3. 흔한 원인(예상, [추정]): ① `secrets` — 핀 파일을 커밋하지 않음 ② `test-pg` — 서비스 컨테이너 헬스체크 시간 초과 ③ `dags` — Airflow 설치/constraints URL ④ `docker-build` — 빌드 컨텍스트에서 파일 누락.
4. 초록불이 되면 사용량(Settings → Billing 의 Actions 분)을 한 번 보고 알려 주세요(D-188).

PG17/PostGIS 3.6.1 에서의 시험 확인은 `test-pg` 가 처음 초록불이 될 때 닫을 수 있습니다.

## 4. (선택) 릴리스 이미지 시험

```zsh
git tag v0.1.0
```

```zsh
git push origin v0.1.0
```

Actions → `release` 가 api·dashboard 이미지를 만들어 `ghcr.io/catastropi/urbanstream-api:0.1.0` 등으로 올립니다. 이미지는 private 패키지로 생깁니다. 되돌리기: GitHub → Packages 에서 버전 삭제, `git push origin :refs/tags/v0.1.0`.

## 5. (선택) 브랜치 보호

Settings → Branches → main → 필수 상태 체크에 **`ci-ok` 하나만** 지정. (private 저장소 Free 플랜에서 지원되는지는 [미확인].)

## 6. 되돌리기

- 워크플로 전체: `.github/workflows/` 삭제 후 push (앱 코드 영향 없음).
- 계층 마커만: `pytest.ini` 의 `-p layers_plugin` 제거.
- 이미 push 한 비밀이 gitleaks 에 걸렸다면: 키를 먼저 폐기·재발급하고, 이력 정리는 별도 판단(D-71).

## 7. 로컬에서 같은 검사를 돌리기

```zsh
python scripts/ci_checks.py tracked
```

```zsh
python scripts/ci_checks.py compose
```

```zsh
python scripts/export_openapi.py --check
```

```zsh
python scripts/mutation_ci.py
```

`compose` 는 Docker 가 필요하고, `mutation_ci.py` 의 e2e 1건은 `GOLD_TEST_DSN` 이 없으면 "건너뜀" 입니다.
