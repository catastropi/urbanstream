"""verify_bronze 의 Kafka 대조 로직 시험. Kafka 없이 (partition, offset) DataFrame 으로 같은 함수를 돌린다.

첫 실제 실행(2026-11-03)에서 실패한 상황을 그대로 재현한다: 토픽은 파티션 3개인데 키가 한 파티션에만 몰려 Bronze 에는 그 파티션만 있다.
"""
from __future__ import annotations

import pytest

pytest.importorskip("pyspark")

from streaming.verify_bronze import compare_with_kafka  # noqa: E402


def kafka(spark, rows):
    return spark.createDataFrame(rows, "partition INT, offset LONG")


def test_topic_with_three_partitions_but_data_only_in_one(spark):
    # Kafka: 파티션 1 에만 offset 0..9. 파티션 0, 2 는 비어 있어 목록에도 없다. Bronze 도 파티션 1 의 0..9.
    k = compare_with_kafka(spark, kafka(spark, [(1, o) for o in range(10)]), {1: (0, 9)})
    assert k == {1: {"total": 10, "inside": 10, "before": 0, "after": 0}}


def test_before_inside_and_after_are_split_by_bronze_range(spark):
    # Bronze 는 offset 3..6 만 가짐: 앞 3건(0,1,2) / 안 4건 / 뒤 3건(7,8,9)
    k = compare_with_kafka(spark, kafka(spark, [(0, o) for o in range(10)]), {0: (3, 6)})
    assert k[0] == {"total": 10, "inside": 4, "before": 3, "after": 3}


def test_partition_missing_from_bronze_counts_as_lagging_not_inside(spark):
    rows = [(0, o) for o in range(5)] + [(2, o) for o in range(4)]
    k = compare_with_kafka(spark, kafka(spark, rows), {0: (0, 4)})
    assert k[0]["inside"] == 5 and k[0]["after"] == 0
    assert k[2] == {"total": 4, "inside": 0, "before": 0, "after": 4}


def test_a_lost_message_inside_the_range_is_visible(spark):
    # Kafka 에는 0..9 가 있는데 Bronze 가 [0,9] 를 주장한다 → inside 는 10. Bronze 쪽 행이 9개뿐이면 호출자가 10 != 9 로 FAIL 판정한다.
    k = compare_with_kafka(spark, kafka(spark, [(0, o) for o in range(10)]), {0: (0, 9)})
    assert k[0]["inside"] == 10


def test_empty_bronze_ranges_do_not_crash(spark):
    k = compare_with_kafka(spark, kafka(spark, [(0, 0), (0, 1)]), {})
    assert k == {0: {"total": 2, "inside": 0, "before": 0, "after": 2}}


# ---- 빈 표 (2026-11-03 두 번째 실패: 메시지가 없는 토픽의 폴더는 _spark_metadata 만 있고 Parquet 이 없다)

from streaming.verify_bronze import read_bronze  # noqa: E402


def test_empty_table_folder_with_only_sink_metadata_returns_none(spark, tmp_path):
    (tmp_path / "dlq" / "_spark_metadata").mkdir(parents=True)
    assert read_bronze(spark, str(tmp_path / "dlq")) is None


def test_missing_folder_returns_none(spark, tmp_path):
    assert read_bronze(spark, str(tmp_path / "nope")) is None


def test_table_with_parquet_returns_dataframe(spark, tmp_path):
    spark.createDataFrame([(0, 1)], "a INT, b INT").write.parquet(str(tmp_path / "t"))
    df = read_bronze(spark, str(tmp_path / "t"))
    assert df is not None and df.count() == 1
