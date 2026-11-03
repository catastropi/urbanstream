"""config/topics.yaml 의 토픽을 Kafka 에 만든다 (이미 있으면 건너뜀). 호스트(Mac)에서 실행한다.

  python scripts/create_topics.py            # 만들기
  python scripts/create_topics.py --list     # 현재 토픽과 파티션 수 보기
  python scripts/create_topics.py --plan     # 만들 내용만 출력 (Kafka 접속 안 함)

접속 주소: 환경변수 KAFKA_BOOTSTRAP (기본 localhost:9092). 토픽 자동 생성이 꺼져 있으므로(ADR-0003) 먼저 이것을 실행해야 한다.
이미 있는 토픽의 파티션 수가 설정과 다르면 경고만 한다 (파티션은 줄일 수 없고, 늘리면 키 배치가 바뀌므로 자동 변경하지 않는다).
"""
from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "src"))

from collector.config import TopicSpec, load_topics  # noqa: E402


def topic_config(spec: TopicSpec) -> dict:
    return {"retention.ms": str(spec.retention_ms)}


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--bootstrap", default=os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092"))
    ap.add_argument("--list", action="store_true")
    ap.add_argument("--plan", action="store_true")
    args = ap.parse_args(argv)

    topics = load_topics().topics
    if args.plan:
        for t in topics.values():
            print(f"{t.name:<32} partitions={t.partitions} replication={t.replication_factor} "
                  f"retention={t.retention_ms // 3600000}h")
        return 0

    from confluent_kafka.admin import AdminClient, NewTopic
    admin = AdminClient({"bootstrap.servers": args.bootstrap, "broker.address.family": "v4"})
    try:
        meta = admin.list_topics(timeout=10)
    except Exception as e:  # 접속 실패
        print(f"Kafka({args.bootstrap}) 에 접속하지 못했습니다: {e}\n먼저 `scripts/stack.sh up` 으로 스택을 켜세요.")
        return 2
    existing = {n: len(m.partitions) for n, m in meta.topics.items() if not n.startswith("__")}

    if args.list:
        for n, p in sorted(existing.items()):
            print(f"{n:<32} partitions={p}")
        return 0

    new = [NewTopic(t.name, num_partitions=t.partitions, replication_factor=t.replication_factor,
                    config=topic_config(t)) for t in topics.values() if t.name not in existing]
    for name, spec in topics.items():
        if name in existing:
            note = "" if existing[name] == spec.partitions else f"  ⚠ 파티션 수가 설정({spec.partitions})과 다름"
            print(f"이미 있음  {name} (partitions={existing[name]}){note}")
    failed = 0
    for name, fut in admin.create_topics(new).items():
        try:
            fut.result()
            print(f"생성함    {name}")
        except Exception as e:
            failed += 1
            print(f"실패      {name}: {e}")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
