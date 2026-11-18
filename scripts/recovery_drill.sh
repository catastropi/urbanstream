#!/usr/bin/env bash
# 복구 훈련 (Step 8, 완료 기준): "일부러 고장 내서 알림이 오는지, 고치면 회복 알림이 오는지" 확인한다.
# 이 스크립트는 고장을 내거나 되돌리는 명령만 실행한다. 알림이 왔는지는 웹훅 채널과 `status` 로 사람이 확인한다.
# 비밀 값(.env, 웹훅 URL)은 출력하지 않는다. 훈련 중 수집기가 돌고 있으면 아래 설명대로 먼저 멈춘다.
#
#   scripts/recovery_drill.sh status          파이프라인 상태 한눈에 (ops.v_pipeline_health, 최근 알림)
#   scripts/recovery_drill.sh webhook-test    웹훅이 실제로 닿는지 1건 보내기 (info → 아침 요약이 아니라 즉시 보내려고 error 로)
#   scripts/recovery_drill.sh bad-key         잘못된 API 키로 서울 돌발 소스를 1회 호출 → 소스 down 알림 (수집기를 먼저 멈출 것)
#   scripts/recovery_drill.sh kafka-down      kafka 컨테이너 중지 → 감시가 3분 안에 critical, 수집은 발행 실패를 audit 에 남김
#   scripts/recovery_drill.sh kafka-up        kafka 컨테이너 시작 → 회복 알림
#   scripts/recovery_drill.sh spark-down      spark-silver 중지 → 레이어 정체 알림(약 15~20분 뒤)
#   scripts/recovery_drill.sh spark-up        spark-silver 시작 → 회복 알림
#   scripts/recovery_drill.sh monitor-down    monitor 중지 → (알림 없음. 컨테이너 unhealthy/정지는 docker ps 로만 보인다 — D-126)
#   scripts/recovery_drill.sh monitor-up      monitor 시작
#   잠자기 훈련은 자동화하지 않는다: docs/runbooks/step8-recovery.md 의 "7. 잠자기" 를 따라 직접 덮개를 닫는다.
set -euo pipefail
cd "$(dirname "$0")/.."
PY="${PYTHON:-python3}"
cmd="${1:-help}"

dc() { COMPOSE_PROFILES="${COMPOSE_PROFILES_OVERRIDE:-core,streaming,silver,airflow,monitor}" docker compose "$@"; }
psql_ops() {
  docker exec urbanstream-postgres sh -c 'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -P pager=off -c "$0"' "$1"
}

case "$cmd" in
  status)
    psql_ops "SELECT component_kind, component, status, left(coalesce(reason,''),60) AS reason, age_sec::int FROM ops.v_pipeline_health ORDER BY 1,2"
    psql_ops "SELECT created_at AT TIME ZONE 'Asia/Seoul' AS kst, level, status, left(title,70) AS title FROM ops.v_alert_recent ORDER BY created_at DESC LIMIT 10"
    ;;
  webhook-test)
    # 알림 경로 전체(마스킹 → 전송)를 실제로 1회 통과시킨다. URL 은 출력하지 않는다.
    PYTHONPATH=src:. "$PY" - <<'PY'
import os
from dotenv import load_dotenv
load_dotenv(".env")
from urbanstream.notify.transport import config_from_env
enabled, kind, url = config_from_env()
print("ALERTS_ENABLED:", enabled, "| kind:", kind, "| URL 설정됨:", bool(url))
if not (enabled and url):
    raise SystemExit("ALERT_WEBHOOK_URL 이 .env 에 없습니다 (또는 ALERTS_ENABLED=false). 설정 후 다시 실행하세요.")
from urbanstream.notify.core import Notifier
from urbanstream.notify.policy import load_policy
from urbanstream.notify.store import MemoryStore
st = Notifier(MemoryStore(), load_policy(), enabled=True, kind=kind, url=url)
print("전송 결과:", st.emit("error", "웹훅 시험 (UrbanStream)", "이 메시지가 보이면 알림 경로가 정상입니다. password=hunter2 는 가려져야 합니다.", "drill:webhook"))
PY
    ;;
  bad-key)
    echo "먼저 수집기를 멈추세요(수집 터미널에서 Ctrl+C). 켜 두면 1~2분 안에 정상 호출이 상태를 덮어씁니다."
    echo "잘못된 키로 서울 돌발을 1회 호출합니다 (.env 의 실제 키는 쓰이지 않고 바뀌지도 않습니다)."
    SEOUL_API_KEY="drill-invalid-key" "$PY" -m collector once --source seoul_incident || true
    echo "→ 1~2분 뒤 'scripts/recovery_drill.sh status' 에서 seoul_incident 가 down(permanent_failure) 이고 웹훅에 [ERROR] 가 와야 합니다."
    echo "→ 수집기를 다시 켜면 다음 정상 호출 뒤 '해소' 알림이 와야 합니다."
    ;;
  kafka-down)  docker stop urbanstream-kafka; echo "→ 약 3분 안에 [CRITICAL] 'Kafka 감사 토픽을 읽지 못합니다'. 끝나면: scripts/recovery_drill.sh kafka-up" ;;
  kafka-up)    docker start urbanstream-kafka; echo "→ 1~2분 안에 'Kafka 감사 토픽 읽기 회복' 이 와야 합니다." ;;
  spark-down)  docker stop urbanstream-spark-silver; echo "→ 약 15~20분 뒤 '…적재 정체' [ERROR] (원천 소스가 정상일 때만). 끝나면: scripts/recovery_drill.sh spark-up" ;;
  spark-up)    docker start urbanstream-spark-silver; echo "→ 새 파일이 쌓이면 해소 알림." ;;
  monitor-down) docker stop urbanstream-monitor; echo "→ 알림이 오지 않는 것이 정상입니다(감시자가 멈췄으므로). 이 한계는 docs/DEFERRED.md D-126 에 있습니다." ;;
  monitor-up)  docker start urbanstream-monitor ;;
  *) sed -n 2,16p "$0" ;;
esac
