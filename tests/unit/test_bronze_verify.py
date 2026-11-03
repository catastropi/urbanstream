"""verify_bronze 의 중복/유실 계산 (pyspark 가 필요한 부분은 import 하지 않도록 계산만 따로 시험)."""
import pytest

pytest.importorskip("pyspark", reason="verify_bronze 는 pyspark 를 import 한다 — scripts/spark_test.sh 에서 실행")

from streaming.verify_bronze import PartitionCheck  # noqa: E402


def test_clean_partition_has_no_duplicates_or_gaps():
    c = PartitionCheck(rows=100, distinct=100, lo=50, hi=149)
    assert (c.duplicates, c.gaps) == (0, 0)


def test_duplicates_and_gaps_are_detected_separately():
    assert PartitionCheck(rows=105, distinct=100, lo=0, hi=99).duplicates == 5      # 같은 offset 이 5번 더 쓰임
    assert PartitionCheck(rows=97, distinct=97, lo=0, hi=99).gaps == 3              # 3개 offset 이 비어 있음
