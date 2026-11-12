#!/usr/bin/env bash
# Mac 에서 Azure 최소 증거 배포를 다루는 도우미 (Step 12, ADR-0014). 런북: docs/runbooks/step12-cloud.md
#
#   scripts/cloud.sh dump              Mac 의 Postgres(마트)를 data/cloud/snapshot_<시각>.sql.gz 로 뜬다 (Mac 스택이 켜져 있어야 한다)
#   scripts/cloud.sh up [스냅샷]       VM 에 소스·스냅샷을 올리고 postgres·api·dashboard 를 기동한다 (terraform apply 뒤)
#   scripts/cloud.sh tunnel            SSH 터널: http://127.0.0.1:18010 (API), http://127.0.0.1:18501 (대시보드)
#   scripts/cloud.sh ssh [명령...]     VM 접속
#   scripts/cloud.sh ps                VM 의 컨테이너 상태
#   scripts/cloud.sh destroy           terraform destroy 후 리소스 그룹이 사라졌는지 확인 (Cloud Shell 배포는 CLOUD_RG 설정 → 포털 삭제 후 확인)
#
# - 터널의 Mac 쪽 포트는 18010·18501 이다: Mac 의 로컬 스택이 8010·8501 을 쓰고 있어도 겹치지 않는다.
# - 비밀(.env)·data/·state·tfvars 는 VM 으로 보내지 않는다 (.gitignore 에 걸린 파일은 목록에서 빠진다).
# - 비밀 값은 출력하지 않는다. set -x 를 쓰지 않는다.
set -euo pipefail
cd "$(dirname "$0")/.."

TF_DIR="infra/terraform"
KEY="${CLOUD_SSH_KEY:-$HOME/.ssh/urbanstream_azure}"
REMOTE_DIR="urbanstream"
LOCAL_API_PORT=18010
LOCAL_DASH_PORT=18501
SSH_OPTS=(-i "$KEY" -o IdentitiesOnly=yes -o StrictHostKeyChecking=accept-new -o ServerAliveInterval=30)

tf_out() { terraform -chdir="$TF_DIR" output -raw "$1"; }
# 보통은 terraform output 에서 읽는다. Cloud Shell 로 apply 한 경우(MFA 우회, 런북 '3-B')에는 Mac 에 state 가 없으므로 환경 변수로 준다:
#   export CLOUD_HOST=<공용 IP> CLOUD_USER=azureuser CLOUD_RG=urbanstream-step12-rg
target() { echo "${CLOUD_USER:-$(tf_out admin_username)}@${CLOUD_HOST:-$(tf_out public_ip)}"; }

cmd="${1:-help}"
shift || true

case "$cmd" in
  dump)
    command -v docker >/dev/null || { echo "docker 가 필요합니다"; exit 1; }
    docker ps --format '{{.Names}}' | grep -qx urbanstream-postgres || { echo "urbanstream-postgres 가 실행 중이 아닙니다. 먼저 scripts/stack.sh up-silver 등으로 스택을 켜세요 (docs/runbooks/mac-on-off.md)"; exit 1; }
    mkdir -p data/cloud
    out="data/cloud/snapshot_$(date -u +%Y%m%dT%H%M%SZ).sql.gz"
    # 컨테이너 안의 pg_dump 를 쓴다(서버와 같은 버전). 데이터만 뜬다: 스키마는 VM 에서 마이그레이션이 만든다.
    # ops.schema_migrations 의 데이터는 뺀다 — VM 이 자기 이력을 가진다.
    docker exec urbanstream-postgres sh -c 'pg_dump -U "$POSTGRES_USER" -d "$POSTGRES_DB" --data-only --disable-triggers --no-owner -n dim -n fact -n mart -n ops --exclude-table-data=ops.schema_migrations' | gzip > "$out"
    echo "스냅샷: $out ($(du -h "$out" | cut -f1))"
    echo "촬영 기준 시각(UTC): $(date -u +%FT%TZ) — 증거 문서에 '스냅샷' 이라고 적는다."
    ;;
  up)
    snap="${1:-}"
    if [ -z "$snap" ]; then snap="$(ls -1t data/cloud/snapshot_*.sql.gz 2>/dev/null | head -1 || true)"; fi
    [ -n "$snap" ] && [ -f "$snap" ] || { echo "스냅샷이 없습니다. 먼저: scripts/cloud.sh dump"; exit 1; }
    T="$(target)"
    echo "[ship] 대상 VM 준비 대기 (cloud-init)"
    ok=0
    for _ in $(seq 1 30); do   # 방금 만든 VM 은 SSH 가 열릴 때까지 최대 약 5분 걸릴 수 있다
      if ssh "${SSH_OPTS[@]}" -o ConnectTimeout=10 "$T" 'cloud-init status --wait >/dev/null 2>&1 || true; test -f /var/lib/urbanstream-cloud-init-done'; then ok=1; break; fi
      sleep 10
    done
    [ "$ok" = 1 ] || { echo "VM 준비를 확인하지 못했습니다(SSH 불가 또는 cloud-init 실패). 내 IP 가 바뀌었다면 tfvars 의 allowed_ssh_cidr 를 고쳐 apply 하세요."; exit 1; }
    echo "[ship] 소스 전송 (.gitignore 에 걸린 .env·data/·state·tfvars 는 제외)"
    list="$(mktemp)"; trap 'rm -f "$list"' EXIT
    git ls-files -z --cached --others --exclude-standard > "$list"
    if tr '\0' '\n' < "$list" | grep -v '/\.gitkeep$' | grep -Eq '(^|/)\.env($|\.[^e])|(^|/)\.env\.e[^x]|\.tfstate|\.tfvars$|^data/'; then
      echo "전송 목록에 비밀/상태 파일이 섞여 있어 중단합니다. .gitignore 를 확인하세요."; exit 1
    fi
    COPYFILE_DISABLE=1 tar --null -czf - -T "$list" | ssh "${SSH_OPTS[@]}" "$T" "mkdir -p $REMOTE_DIR && tar -xzf - -C $REMOTE_DIR"
    echo "[ship] 스냅샷 전송"
    scp "${SSH_OPTS[@]}" "$snap" "$T:$REMOTE_DIR/snapshot.sql.gz"
    echo "[up] VM 에서 기동 (이미지 빌드 포함, 첫 실행은 몇 분 걸립니다)"
    ssh "${SSH_OPTS[@]}" "$T" "cd $REMOTE_DIR && bash scripts/cloud_remote.sh snapshot.sql.gz"
    ;;
  tunnel)
    T="$(target)"
    echo "터널 시작: API http://127.0.0.1:${LOCAL_API_PORT}/docs , 대시보드 http://127.0.0.1:${LOCAL_DASH_PORT} (끝내려면 Ctrl+C)"
    exec ssh "${SSH_OPTS[@]}" -N -L "127.0.0.1:${LOCAL_API_PORT}:127.0.0.1:8010" -L "127.0.0.1:${LOCAL_DASH_PORT}:127.0.0.1:8501" "$T"
    ;;
  ssh)
    exec ssh "${SSH_OPTS[@]}" "$(target)" "$@"
    ;;
  ps)
    ssh "${SSH_OPTS[@]}" "$(target)" "cd $REMOTE_DIR && docker compose -f docker-compose.yml -f docker-compose.cloud.yml --profile core --profile api --profile dashboard ps postgres api dashboard"
    ;;
  destroy)
    if [ -n "${CLOUD_RG:-}" ]; then
      # Cloud Shell 로 만든 배포: Mac 에는 state 가 없다. 포털(MFA 로 로그인된 세션)에서 리소스 그룹을 지운다.
      rg="$CLOUD_RG"
      echo "포털에서 리소스 그룹 $rg 를 삭제하세요(리소스 그룹 → $rg → 삭제, 이름 입력). 삭제가 끝나면 Enter."
      read -r _
    else
      rg="$(tf_out resource_group)"
      terraform -chdir="$TF_DIR" destroy
    fi
    if command -v az >/dev/null; then
      if [ "$(az group exists --name "$rg")" = "false" ]; then
        echo "확인: 리소스 그룹 $rg 가 사라졌습니다."
      else
        echo "경고: 리소스 그룹 $rg 가 아직 있습니다. 남은 것:"; az resource list --resource-group "$rg" --query '[].{name:name,type:type}' -o table
        exit 1
      fi
    else
      echo "az 가 없어 삭제 확인을 건너뜁니다. 포털에서 리소스 그룹 $rg 가 없는지 직접 확인하세요."
    fi
    ;;
  *)
    sed -n '2,12p' "$0"
    ;;
esac
