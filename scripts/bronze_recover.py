"""Bronze 스트림이 Kafka 보존 기간을 넘겨 멈췄을 때(OffsetOutOfRange) 체크포인트를 고쳐 다시 시작하게 한다. 호스트(Mac)에서 실행한다.

  python scripts/bronze_recover.py status                  표마다 체크포인트 위치와 Kafka 에 남은 범위를 비교한다 (읽기만 함)
  python scripts/bronze_recover.py reset all               고칠 내용만 출력한다 (미리보기)
  python scripts/bronze_recover.py reset bus_arrival --apply

무엇을 고치나
  체크포인트의 마지막 batch 가 가리키는 offset 중 Kafka 에서 이미 지워진 것을 '지금 남아 있는 가장 이른 offset' 으로 올린다.
  batch 번호와 data/bronze/<표>/_spark_metadata 는 그대로 둔다. 그래서 이전에 쓴 Bronze 파일이 계속 보이고, 새 batch 도 건너뛰어지지 않는다.

왜 이 방식인가 (D-255)
  - 체크포인트만 지우면 새 쿼리의 batch 번호가 0 부터 다시 시작하고, 파일 sink 는 _spark_metadata 에 기록된 번호 이하의 batch 를
    '이미 썼다'고 보고 건너뛴다. 새 데이터가 조용히 버려진다.
  - 체크포인트와 _spark_metadata 를 함께 옮기면 다시 쓰기는 되지만, 옮기기 전 Bronze 파일이 sink 기록에서 빠져 읽히지 않는다
    (D-258 의 '초과' FAIL 원인).
  - 남은 offset 으로 옮기는 것은 지워진 구간을 잃었다는 사실을 바꾸지 않는다. 잃은 메시지 수를 출력하고, 바꾸기 전 파일은
    data/_recovery/<시각>/ 에 복사해 둔다.

반드시 spark-bronze 를 멈춘 뒤 --apply 한다(실행 중이면 체크포인트를 쓰는 중일 수 있다). 스크립트는 멈췄는지 확인하지 못하므로
`docker compose --profile streaming stop spark-bronze` 를 먼저 실행하라고 안내만 한다.
"""
from __future__ import annotations

import argparse
import json
import os
import shutil
import sys
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Callable, Optional

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "src"))

Watermarks = Callable[[str, int], tuple[int, int]]    # (topic, partition) -> (가장 이른 offset, 다음에 쓸 offset)


@dataclass(frozen=True)
class OffsetFile:
    path: Path
    batch: int
    header: str          # "v1"
    meta: str            # batchWatermarkMs 등 (그대로 둔다)
    sources: list[str]   # 소스별 offset JSON 한 줄 (Bronze 는 소스가 1개)


@dataclass(frozen=True)
class Plan:
    table: str
    batch: int
    path: Path
    changes: dict[str, dict[str, tuple[int, int]]]    # topic -> partition -> (저장된 값, 바꿀 값)

    @property
    def lost(self) -> int:
        return sum(new - old for parts in self.changes.values() for old, new in parts.values())


def _batch_ids(d: Path) -> list[int]:
    if not d.is_dir():
        return []
    return sorted(int(p.name) for p in d.iterdir() if p.is_file() and p.name.isdigit())


def read_offset_file(path: Path) -> OffsetFile:
    lines = path.read_text(encoding="utf-8").splitlines()
    if len(lines) < 3 or not lines[0].startswith("v"):
        raise ValueError(f"체크포인트 offset 파일 형식이 예상과 다릅니다: {path}")
    return OffsetFile(path, int(path.name), lines[0], lines[1], lines[2:])


def write_offset_file(f: OffsetFile, sources: list[str]) -> None:
    """같은 이름으로 다시 쓴다. Hadoop 로컬 파일시스템이 남긴 체크섬(.N.crc)은 내용과 맞지 않게 되므로 지운다(없으면 검사하지 않는다)."""
    tmp = f.path.with_name(f".{f.path.name}.recover.tmp")
    tmp.write_text("\n".join([f.header, f.meta, *sources]), encoding="utf-8")
    os.replace(tmp, f.path)
    crc = f.path.with_name(f".{f.path.name}.crc")
    if crc.exists():
        crc.unlink()


def batches_to_fix(ckpt: Path) -> list[int]:
    """재시작 때 Spark 가 시작 위치로 쓰는 batch: 마지막으로 커밋된 batch, 그리고 그 뒤에 계획만 되고 커밋되지 않은 batch."""
    offsets, commits = _batch_ids(ckpt / "offsets"), _batch_ids(ckpt / "commits")
    if not offsets:
        return []
    last_commit = commits[-1] if commits else None
    return [b for b in offsets if last_commit is None or b >= last_commit]


def plan_table(table: str, ckpt: Path, watermarks: Watermarks) -> list[Plan]:
    plans = []
    for b in batches_to_fix(ckpt):
        f = read_offset_file(ckpt / "offsets" / str(b))
        if not f.sources or f.sources[0] in ("", "-"):
            continue
        stored = json.loads(f.sources[0])
        changes: dict[str, dict[str, tuple[int, int]]] = {}
        for topic, parts in stored.items():
            for part, off in parts.items():
                low, _high = watermarks(topic, int(part))
                if int(off) < low:
                    changes.setdefault(topic, {})[part] = (int(off), low)
        if changes:
            plans.append(Plan(table, b, f.path, changes))
    return plans


def apply_plan(plan: Plan, backup_root: Path) -> None:
    f = read_offset_file(plan.path)
    dest = backup_root / plan.table / "offsets"
    dest.mkdir(parents=True, exist_ok=True)
    shutil.copy2(plan.path, dest / plan.path.name)
    stored = json.loads(f.sources[0])
    for topic, parts in plan.changes.items():
        for part, (_old, new) in parts.items():
            stored[topic][part] = new
    write_offset_file(f, [json.dumps(stored, separators=(",", ":")), *f.sources[1:]])


def kafka_watermarks(bootstrap: str) -> Watermarks:
    from confluent_kafka import Consumer, TopicPartition
    c = Consumer({"bootstrap.servers": bootstrap, "group.id": "urbanstream-bronze-recover", "enable.auto.commit": False,
                  "broker.address.family": "v4"})

    def get(topic: str, partition: int) -> tuple[int, int]:
        return c.get_watermark_offsets(TopicPartition(topic, partition), timeout=10)
    return get


def _tables(names: list[str]) -> list[str]:
    from streaming.bronze_config import load_tables
    known = [t.name for t in load_tables(ROOT / "config")]
    if names == ["all"]:
        return known
    bad = [n for n in names if n not in known]
    if bad:
        raise SystemExit(f"모르는 표: {', '.join(bad)} (가능: {', '.join(known)}, all)")
    return names


def main(argv: Optional[list[str]] = None, *, watermarks: Optional[Watermarks] = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("cmd", choices=["status", "reset"])
    ap.add_argument("tables", nargs="*", default=["all"])
    ap.add_argument("--apply", action="store_true", help="reset 을 실제로 적용한다 (없으면 미리보기)")
    ap.add_argument("--checkpoint-root", default=str(ROOT / "data" / "checkpoints" / "bronze"))
    ap.add_argument("--bootstrap", default=os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092"))
    ap.add_argument("--backup-root", default=str(ROOT / "data" / "_recovery"))
    a = ap.parse_args(argv)

    wm = watermarks or kafka_watermarks(a.bootstrap)
    root = Path(a.checkpoint_root)
    needs = 0
    stamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    for t in _tables(a.tables):
        ckpt = root / t
        if not (ckpt / "offsets").is_dir():
            print(f"{t:<16} 체크포인트 없음 (처음 시작하면 startingOffsets 를 따른다)")
            continue
        plans = plan_table(t, ckpt, wm)
        if not plans:
            print(f"{t:<16} 정상      저장된 offset 이 모두 Kafka 에 남아 있다")
            continue
        needs += 1
        for p in plans:
            detail = ", ".join(f"{tp}[{pt}] {o}→{n}" for tp, parts in p.changes.items() for pt, (o, n) in sorted(parts.items()))
            print(f"{t:<16} 지워진 구간  batch {p.batch}: {detail}  (잃은 메시지 {p.lost:,}건)")
        if a.cmd == "reset" and a.apply:
            backup = Path(a.backup_root) / stamp
            for p in plans:
                apply_plan(p, backup)
            print(f"{'':<16} 적용함. 바꾸기 전 파일: {backup / t}")
    if a.cmd == "reset" and not a.apply and needs:
        print("\n미리보기였다. spark-bronze 를 멈춘 뒤 --apply 를 붙여 실행한다:\n"
              "  docker compose --profile streaming stop spark-bronze\n"
              f"  python scripts/bronze_recover.py reset {' '.join(a.tables)} --apply\n"
              "  scripts/stack.sh up-silver")
    if a.cmd == "status":
        return 1 if needs else 0
    return 0


if __name__ == "__main__":
    sys.exit(main())
