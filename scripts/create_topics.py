"""config/topics.yaml 의 토픽을 Kafka 에 만든다 (이미 있으면 건너뜀). 호스트(Mac)에서 실행한다.

  python scripts/create_topics.py            # 만들기
  python scripts/create_topics.py --list     # 현재 토픽과 파티션 수 보기
  python scripts/create_topics.py --plan     # 만들 내용만 출력 (Kafka 접속 안 함)
  python scripts/create_topics.py --check    # 이미 있는 토픽의 실제 설정(retention.ms)이 파일과 같은지 확인. 다르면 종료 코드 1
  python scripts/create_topics.py --apply-config   # 이미 있는 토픽에 파일의 설정을 반영 (파티션 수는 바꾸지 않는다)

접속 주소: 환경변수 KAFKA_BOOTSTRAP (기본 localhost:9092). 토픽 자동 생성이 꺼져 있으므로(ADR-0003) 먼저 이것을 실행해야 한다.
이미 있는 토픽의 파티션 수가 설정과 다르면 경고만 한다 (파티션은 줄일 수 없고, 늘리면 키 배치가 바뀌므로 자동 변경하지 않는다).
토픽을 만들 때 넣은 설정은 나중에 topics.yaml 을 고쳐도 바뀌지 않는다. 그래서 --check 로 실제 값을 확인하고 --apply-config 로 맞춘다.
"""
from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path
from typing import Optional

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "src"))

from collector.config import TopicSpec, load_topics  # noqa: E402


def topic_config(spec: TopicSpec) -> dict:
    return {"retention.ms": str(spec.retention_ms)}


def config_drift(specs: dict[str, TopicSpec], actual: dict[str, dict[str, str]]) -> list[tuple[str, str, str, Optional[str]]]:
    """파일의 설정과 브로커의 실제 설정 차이: (토픽, 키, 파일 값, 실제 값). 실제에 없는 토픽은 실제 값 None 으로 돌려준다."""
    out = []
    for name, spec in specs.items():
        got = actual.get(name)
        for key, want in topic_config(spec).items():
            have = None if got is None else got.get(key)
            if have != want:
                out.append((name, key, want, have))
    return out


def _describe(admin, names: list[str]) -> dict[str, dict[str, str]]:
    from confluent_kafka.admin import ConfigResource, ResourceType
    res = [ConfigResource(ResourceType.TOPIC, n) for n in names]
    out: dict[str, dict[str, str]] = {}
    for r, fut in admin.describe_configs(res).items():
        out[r.name] = {k: v.value for k, v in fut.result(timeout=10).items()}
    return out


def _apply(admin, specs: dict[str, TopicSpec], names: list[str]) -> int:
    from confluent_kafka.admin import AlterConfigOpType, ConfigEntry, ConfigResource, ResourceType
    res = [ConfigResource(ResourceType.TOPIC, n, incremental_configs=[
        ConfigEntry(k, v, incremental_operation=AlterConfigOpType.SET) for k, v in topic_config(specs[n]).items()])
        for n in names]
    failed = 0
    for r, fut in admin.incremental_alter_configs(res).items():
        try:
            fut.result(timeout=10)
            print(f"설정 반영  {r.name}  {topic_config(specs[r.name])}")
        except Exception as e:  # noqa: BLE001
            failed += 1
            print(f"반영 실패  {r.name}: {e}")
    return failed


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--bootstrap", default=os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092"))
    ap.add_argument("--list", action="store_true")
    ap.add_argument("--plan", action="store_true")
    mode = ap.add_mutually_exclusive_group()
    mode.add_argument("--check", action="store_true", help="실제 설정이 파일과 같은지 확인만 한다")
    mode.add_argument("--apply-config", action="store_true", help="이미 있는 토픽에 파일의 설정을 반영한다")
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

    if args.check or args.apply_config:
        present = [n for n in topics if n in existing]
        drift = config_drift(topics, _describe(admin, present) if present else {})
        for name, key, want, have in drift:
            print(f"다름      {name}  {key}: 파일={want} 실제={have if have is not None else '(토픽 없음)'}")
        if not drift:
            print(f"일치      토픽 {len(present)}개의 설정이 config/topics.yaml 과 같습니다")
            return 0
        if args.check:
            return 1
        targets = sorted({name for name, *_ in drift if name in existing})
        missing = sorted({name for name, *_ in drift if name not in existing})
        if missing:
            print(f"없는 토픽은 만들지 않았습니다(인자 없이 실행하면 만든다): {', '.join(missing)}")
        failed = _apply(admin, topics, targets) if targets else 0
        return 1 if failed or missing else 0

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
