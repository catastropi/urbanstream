"""Silver 읽기 (STEP 1). 스트리밍 루트(data/silver)와 재처리 버전(data/silver_rebuild)을 날짜 단위로 골라 한 DataFrame 으로 만든다.

날짜 D 에 포인터가 있으면 그 버전의 D 폴더만, 없으면 스트리밍 루트의 D 를 본다. 한 날짜는 정확히 한쪽에서만 온다(합치지 않는다).
포인터가 하나도 없으면 예전과 똑같이 스트리밍 루트만 읽는다(`read_bronze(silver_root/표)` 와 같은 결과).

버전 폴더는 파티션 폴더를 직접 나열해 읽는다. Spark 4.1.3 은 하위 파티션 폴더를 직접 읽을 때 부모의 `_spark_metadata` 를 쓰지 않는다
(tests/spark/test_silver_rebuild.py::test_reading_partition_dirs_ignores_parent_sink_log 로 고정). 버전 폴더에는 원래 sink 로그도 없다.
"""
from __future__ import annotations

from datetime import date
from pathlib import Path
from typing import Mapping, Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from streaming.silver_pointer import active_dates, version_dir, version_inputs
from streaming.verify_bronze import read_bronze


def read_silver(spark: SparkSession, silver_root: str, rebuild_root: Optional[str], table: str, partition_col: str,
                dates: Optional[Mapping[str, str]] = None) -> Optional[DataFrame]:
    """dates: 미리 읽은 포인터 스냅샷 {날짜: 버전}. None 이면 여기서 한 번 읽는다. 데이터가 하나도 없으면 None."""
    if dates is None:
        dates = active_dates(Path(rebuild_root) if rebuild_root else None, table)
    stream = read_bronze(spark, f"{silver_root}/{table}")
    if not dates:
        return stream
    override = sorted(dates)
    parts: list[DataFrame] = []
    if stream is not None:
        parts.append(stream.filter(~F.col(partition_col).isin([date.fromisoformat(d) for d in override])))
    for v, paths in version_inputs(Path(rebuild_root), table, dates).items():
        if paths:
            parts.append(spark.read.option("basePath", str(version_dir(Path(rebuild_root), table, v))).parquet(*paths))
    if not parts:
        return None
    out = parts[0]
    for p in parts[1:]:
        out = out.unionByName(p.select(*out.columns))
    return out
