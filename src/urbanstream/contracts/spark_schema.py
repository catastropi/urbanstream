"""계약 스키마(schema_of)에서 Spark 스키마를 만든다. 스키마는 contracts 한 곳에서만 정의하고 여기서는 변환만 한다.

- `ddl_of(source)`        : Spark 가 읽는 DDL 문자열. **순수 Python** 이라 pyspark/Java 없이도 호스트에서 테스트할 수 있다.
                            `from_json(value, ddl_of(source))` 에 그대로 쓴다.
- `struct_type_of(source)` : 같은 내용의 `StructType` (pyspark 가 있을 때만; 지연 import).
- `required_paths(source)` : 계약상 NULL 이 아니어야 하는 필드 경로 (Bronze 의 `_parse_ok` 판단에 쓴다).

구조: envelope 필드는 최상위에 펼치고, 소스별 내용은 `payload STRUCT<...>` 로 둔다 (Kafka 메시지의 JSON 모양과 같다).
주의: Spark 는 JSON 을 읽을 때 NOT NULL 을 강제하지 않는다(모든 필드가 nullable). 그래서 NOT NULL 판단은 `required_paths` 로 따로 한다.
"""
from __future__ import annotations

from .registry import schema_of

# schema.py 의 타입 이름 → Spark SQL 타입
SPARK_TYPES = {
    "string": "STRING",
    "long": "BIGINT",
    "double": "DOUBLE",
    "boolean": "BOOLEAN",
    "timestamp": "TIMESTAMP",
}


def _spark_type(name: str) -> str:
    try:
        return SPARK_TYPES[name]
    except KeyError:  # 새 타입을 계약에 넣고 여기를 안 고치면 조용히 틀리지 않고 바로 실패한다
        raise TypeError(f"Spark 타입 매핑이 없는 계약 타입: {name!r} (SPARK_TYPES 에 추가하세요)") from None


def ddl_of(source_system: str) -> str:
    s = schema_of(source_system)
    env = ", ".join(f"{f['name']} {_spark_type(f['type'])}" for f in s["envelope"])
    payload = ", ".join(f"{f['name']}: {_spark_type(f['type'])}" for f in s["payload"])
    return f"{env}, payload STRUCT<{payload}>"


def required_paths(source_system: str) -> list[str]:
    """NULL 이면 안 되는 필드의 경로. 예: ['event_id', ..., 'payload.link_id']"""
    s = schema_of(source_system)
    return [f["name"] for f in s["envelope"] if not f["nullable"]] + [
        f"payload.{f['name']}" for f in s["payload"] if not f["nullable"]
    ]


def struct_type_of(source_system: str):
    from pyspark.sql import types as T  # 지연 import: 호스트(pyspark 없음)에서도 이 모듈을 가져올 수 있다

    ctor = {"string": T.StringType, "long": T.LongType, "double": T.DoubleType,
            "boolean": T.BooleanType, "timestamp": T.TimestampType}
    s = schema_of(source_system)

    def fields(items):
        return [T.StructField(f["name"], ctor[f["type"]](), True) for f in items]

    return T.StructType(fields(s["envelope"]) + [T.StructField("payload", T.StructType(fields(s["payload"])), True)])
