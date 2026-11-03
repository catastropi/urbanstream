#!/usr/bin/env bash
# UrbanStream 인프라 스택 도우미 (Docker Compose). 프로젝트 어디서 실행해도 루트로 이동해서 동작한다.
#
#   scripts/stack.sh init-env   .env 에 Compose 항목 채우기 (이미 있는 값은 건드리지 않음, 비밀 값 자동 생성)
#   scripts/stack.sh up         postgres + kafka 기동 (healthy 가 될 때까지 기다림)
#   scripts/stack.sh up-dev     위 + kafka-ui (http://127.0.0.1:8090)
#   scripts/stack.sh up-streaming  위 + spark-bronze (이미지를 만들고 Kafka → Bronze 쓰기 시작. 첫 빌드는 몇 분 걸린다)
#   scripts/stack.sh up-silver  위 + spark-bronze + spark-silver (Bronze → Silver. 메모리가 약 1.5 GiB 더 든다)
#   scripts/stack.sh check      상태/PostGIS/Kafka 왕복 점검
#   scripts/stack.sh stats      컨테이너별 메모리·CPU 와 Docker VM 총 메모리 (data/stack/ 에 저장)
#   scripts/stack.sh ps | logs [서비스]
#   scripts/stack.sh down       중지 (데이터 볼륨 유지)
#   scripts/stack.sh reset      중지 + 데이터 볼륨 삭제 (Kafka/Postgres 데이터가 사라진다. 확인 질문 있음)
set -euo pipefail
cd "$(dirname "$0")/.."

PY="${PYTHON:-python3}"
cmd="${1:-help}"
shift || true

compose() { docker compose --profile '*' "$@"; }

need_docker() {
  if ! docker info >/dev/null 2>&1; then
    echo "Docker 데몬에 연결할 수 없습니다. Docker Desktop 이 꺼져 있는 것 같습니다."
    echo "  1) 실행:  open -a Docker"
    echo "  2) 메뉴바 고래 아이콘이 'Docker Desktop is running' 이 될 때까지 기다리기 (약 30초~1분)"
    echo "  3) 확인:  docker info   (에러 없이 출력되면 준비 완료)"
    exit 1
  fi
}

case "$cmd" in
  init-env)
    "$PY" scripts/stack_env.py "$@"
    ;;
  up)
    need_docker
    docker compose up -d --wait
    docker compose ps
    ;;
  up-dev)
    need_docker
    COMPOSE_PROFILES="core,dev" docker compose up -d --wait
    COMPOSE_PROFILES="core,dev" docker compose ps
    ;;
  up-streaming)
    need_docker
    mkdir -p data/bronze data/checkpoints
    COMPOSE_PROFILES="core,streaming" docker compose up -d --build --wait
    COMPOSE_PROFILES="core,streaming" docker compose ps
    ;;
  up-silver)
    need_docker
    mkdir -p data/bronze data/silver data/checkpoints
    COMPOSE_PROFILES="core,streaming,silver" docker compose up -d --build --wait
    COMPOSE_PROFILES="core,streaming,silver" docker compose ps
    ;;
  ps)
    need_docker
    compose ps --all
    ;;
  logs)
    need_docker
    compose logs --tail=100 "$@"
    ;;
  check)
    need_docker
    "$PY" scripts/stack_check.py
    ;;
  stats)
    need_docker
    ids="$(compose ps -q)"
    if [ -z "$ids" ]; then echo "실행 중인 컨테이너가 없습니다. 먼저: scripts/stack.sh up"; exit 1; fi
    mkdir -p data/stack
    out="data/stack/stats_$(date +%Y%m%dT%H%M%S).txt"
    {
      echo "측정 시각: $(date '+%Y-%m-%d %H:%M:%S %Z')"
      echo "Docker VM 총 메모리: $(( $(docker info --format '{{.MemTotal}}') / 1024 / 1024 )) MiB / CPU: $(docker info --format '{{.NCPU}}')개"
      echo "아키텍처: $(docker info --format '{{.Architecture}}')"
      echo
      # shellcheck disable=SC2086
      docker stats --no-stream --format 'table {{.Name}}\t{{.MemUsage}}\t{{.MemPerc}}\t{{.CPUPerc}}' $ids
    } | tee "$out"
    echo
    echo "저장: $out"
    ;;
  down)
    need_docker
    compose down
    ;;
  reset)
    need_docker
    echo "Kafka 와 Postgres 의 데이터 볼륨을 모두 삭제합니다. 되돌릴 수 없습니다."
    read -r -p "계속하려면 'yes' 를 입력하세요: " ans
    if [ "$ans" = "yes" ]; then compose down -v; else echo "취소했습니다."; fi
    ;;
  help|*)
    sed -n '2,11p' "$0" | sed 's/^# \{0,1\}//'
    ;;
esac
