"""dataclass 정의에서 스키마(필드 이름/타입/null 허용)를 뽑는다.

목적: 스키마를 한 곳(코드)에만 두고, 다른 곳(문서 스냅샷, Spark StructType 등)은 여기서 파생시킨다.
tests/contract/snapshots/*.json 은 이 결과를 저장한 것이고, 코드가 바뀌어 스냅샷과 달라지면 테스트가 실패한다.
= 스키마를 바꾸려면 의도적으로 스냅샷(과 schema_version)을 같이 갱신해야 한다.
"""
from __future__ import annotations

import dataclasses
import typing
from datetime import datetime
from typing import Any, Optional

_TYPE_NAMES = {str: "string", int: "long", float: "double", bool: "boolean", datetime: "timestamp"}


def _describe_type(tp: Any) -> tuple[str, bool]:
    """(타입 이름, nullable)"""
    if typing.get_origin(tp) is typing.Union:
        args = [a for a in typing.get_args(tp) if a is not type(None)]
        nullable = len(args) != len(typing.get_args(tp))
        if len(args) != 1:
            raise TypeError(f"지원하지 않는 Union: {tp}")
        return _TYPE_NAMES[args[0]], nullable
    return _TYPE_NAMES[tp], False


def describe(cls: type) -> list[dict]:
    hints = typing.get_type_hints(cls)
    out = []
    for f in dataclasses.fields(cls):
        type_name, nullable = _describe_type(hints[f.name])
        out.append({"name": f.name, "type": type_name, "nullable": nullable})
    return out
