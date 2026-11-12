"""运行配置：端口、跨域、运行环境与共享缓存后端位置。

缓存数据库位置由环境变量驱动，本地开发与容器部署可以指向不同路径：
- ``QUERY_CACHE_DB``：SQLite 文件路径；缺省放在后端运行目录下的 ``data/query_cache.sqlite3``。
  容器里把它指到挂载卷（见 docker-compose.yml），重启后仍可读取。
- ``QUERY_CACHE_LEASE_MS``：填充租约时长（毫秒），租约持有者退出后其它进程可在此时长后接管。
- ``QUERY_CACHE_WAIT_MS``：同键已有租约时，本进程最多等待多久再转为直接计算。
"""
from __future__ import annotations

import os
from dataclasses import dataclass, field


def _cache_db_path() -> str:
    configured = os.getenv("QUERY_CACHE_DB", "").strip()
    if configured:
        return configured
    here = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    return os.path.join(here, "data", "query_cache.sqlite3")


def _int_env(name: str, default: int) -> int:
    raw = os.getenv(name, "").strip()
    if not raw:
        return default
    try:
        value = int(raw)
    except ValueError:
        return default
    return value if value > 0 else default


@dataclass(frozen=True)
class Settings:
    app_name: str = "数据洞察分析工作台"
    env: str = os.getenv("APP_ENV", "local")
    port: int = 8000
    allowed_origins: list[str] = field(
        default_factory=lambda: [
            "http://127.0.0.1:5173",
            "http://localhost:5173",
        ]
    )
    page_size_default: int = 20
    page_size_max: int = 200
    # 共享查询缓存（SQLite）相关配置
    query_cache_db: str = field(default_factory=_cache_db_path)
    query_cache_lease_ms: int = field(default_factory=lambda: _int_env("QUERY_CACHE_LEASE_MS", 30_000))
    query_cache_wait_ms: int = field(default_factory=lambda: _int_env("QUERY_CACHE_WAIT_MS", 1_500))
    query_cache_wait_step_ms: int = 100


settings = Settings()
