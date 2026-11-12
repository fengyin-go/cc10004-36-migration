"""数据洞察分析工作台 后端服务入口。

启动：uvicorn app.main:app --host 127.0.0.1 --port 8000
健康检查：GET /api/health
"""
from __future__ import annotations

import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.config import settings
from app.routers import ROUTERS
from app.services.cache_runtime import query_cache
from app.store import store

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
logger = logging.getLogger("app.query_cache")


@asynccontextmanager
async def lifespan(_app: FastAPI):
    # 启动时把缓存策略迁移到 SQLite 共享后端（保留策略编号），失败不阻断服务启动
    try:
        synced = query_cache.policy_service.sync_all_strategies()
        stats = query_cache.cache_stats()
        logger.info(
            "查询缓存共享后端就绪 db=%s 迁移策略=%s 条 现有缓存条目=%s owner=%s",
            stats.get("db_path"), synced, stats.get("total"), query_cache.owner,
        )
    except Exception as exc:  # pragma: no cover - 防御性兜底
        logger.error("缓存策略迁移失败，服务继续运行并绕过缓存：%s", exc)
    yield


app = FastAPI(title="数据洞察分析工作台", version="1.0.0", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.allowed_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

for module in ROUTERS:
    app.include_router(module.router)


@app.get("/api/health")
def health() -> dict[str, object]:
    """健康检查：确认服务已经监听、示例数据已经就绪、共享缓存后端状态可见。"""
    return {
        "ok": True,
        "app": settings.app_name,
        "modules": len(store.module_names()),
        "query_cache": query_cache.cache_stats(),
    }


@app.get("/api/overview")
def overview() -> dict[str, object]:
    """运营概览：把各业务模块的待处理量汇总成看板卡片。"""
    return store.overview()
