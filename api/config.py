"""환경 변수 → Settings. 키가 없거나 약하면 시작을 거부한다(안전한 쪽으로 실패, D9-2).

비밀(API 키, DB 비밀번호)은 repr 에 나오지 않는다. 컨테이너는 .env 전체가 아니라 compose 가 넘긴 값만 받는다.
"""
from __future__ import annotations

import os
from dataclasses import dataclass, field
from typing import Mapping, Optional

API_VERSION = "1.0.0"
MIN_KEY_LEN = 24

# 연결마다 거는 안전 옵션. 역할(api_service)에도 같은 값이 있지만 역할 설정은 SET ROLE 로 접속하는 경로(시험·점검)에는 적용되지 않는다.
DB_OPTIONS = ("-c statement_timeout=5000 -c lock_timeout=1000 -c idle_in_transaction_session_timeout=10000 "
              "-c default_transaction_read_only=on -c TimeZone=UTC")


class ConfigError(ValueError):
    pass


def _int(env: Mapping[str, str], name: str, default: int, lo: int = 1, hi: int = 10**9) -> int:
    raw = (env.get(name) or "").strip()
    if not raw:
        return default
    try:
        v = int(raw)
    except ValueError as e:
        raise ConfigError(f"{name} 는 정수여야 합니다") from e
    if not lo <= v <= hi:
        raise ConfigError(f"{name} 는 {lo}~{hi} 사이여야 합니다")
    return v


def _bool(env: Mapping[str, str], name: str, default: bool) -> bool:
    raw = (env.get(name) or "").strip().lower()
    if not raw:
        return default
    if raw in ("1", "true", "yes", "on"):
        return True
    if raw in ("0", "false", "no", "off"):
        return False
    raise ConfigError(f"{name} 는 true/false 여야 합니다")


@dataclass(frozen=True)
class Settings:
    api_keys: tuple = field(repr=False)
    db_host: str = "postgres"
    db_port: int = 5432
    db_name: str = "urbanstream"
    db_user: str = "api_service"
    db_password: str = field(default="", repr=False)
    db_dsn: str = field(default="", repr=False)          # 시험용 전체 접속 문자열 (API_DSN). 있으면 host/user/password 보다 우선
    db_extra_options: str = ""                           # 시험용: "-c role=api_service" 처럼 SET ROLE 경로를 쓸 때
    pool_max: int = 5
    rate_per_min: int = 120
    rate_burst: int = 40
    cors_origins: tuple = ()
    docs_enabled: bool = True
    stale_after_sec: int = 10800
    cache_ttl_scale: float = 1.0                         # 0 이면 캐시 끔 (시험·점검용)


def load_settings(env: Optional[Mapping[str, str]] = None) -> Settings:
    e = os.environ if env is None else env
    keys = tuple(k.strip() for k in (e.get("API_KEYS") or "").split(",") if k.strip())
    if not keys:
        raise ConfigError("API_KEYS 가 비어 있습니다. scripts/stack.sh init-env 로 만든 뒤 다시 시작하세요 (키가 없으면 API 는 시작하지 않습니다)")
    for k in keys:
        if len(k) < MIN_KEY_LEN or any(c.isspace() for c in k):
            raise ConfigError(f"API_KEYS 의 각 키는 공백 없이 {MIN_KEY_LEN}자 이상이어야 합니다")
    if len(set(keys)) != len(keys):
        raise ConfigError("API_KEYS 에 같은 키가 중복되어 있습니다")
    dsn = (e.get("API_DSN") or "").strip()
    password = e.get("API_DB_PASSWORD") or ""
    if not dsn and not password:
        raise ConfigError("API_DB_PASSWORD 가 비어 있습니다. scripts/stack.sh init-env 와 db_bootstrap.py --api 를 먼저 실행하세요")
    origins = tuple(o.strip() for o in (e.get("API_CORS_ORIGINS") or "").split(",") if o.strip())
    if "*" in origins:
        raise ConfigError("API_CORS_ORIGINS 에 '*' 는 쓸 수 없습니다 (출처를 명시하세요)")
    try:
        scale = float((e.get("API_CACHE_TTL_SCALE") or "1").strip())
    except ValueError as ex:
        raise ConfigError("API_CACHE_TTL_SCALE 는 숫자여야 합니다") from ex
    if scale < 0:
        raise ConfigError("API_CACHE_TTL_SCALE 는 0 이상이어야 합니다")
    return Settings(
        api_keys=keys,
        db_host=e.get("API_DB_HOST", "postgres"), db_port=_int(e, "API_DB_PORT", 5432, 1, 65535),
        db_name=e.get("API_DB_NAME", "urbanstream"), db_user=e.get("API_DB_USER", "api_service"),
        db_password=password, db_dsn=dsn, db_extra_options=(e.get("API_DB_EXTRA_OPTIONS") or "").strip(),
        pool_max=_int(e, "API_POOL_MAX", 5, 1, 8),                 # 역할 CONNECTION LIMIT 8 보다 작게
        rate_per_min=_int(e, "API_RATE_LIMIT_PER_MIN", 120, 1, 100000), rate_burst=_int(e, "API_RATE_BURST", 40, 1, 100000),
        cors_origins=origins, docs_enabled=_bool(e, "API_DOCS_ENABLED", True),
        stale_after_sec=_int(e, "API_STALE_AFTER_SEC", 10800, 60, 10**7), cache_ttl_scale=scale,
    )
