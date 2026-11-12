"""查询缓存接口：维护缓存策略，覆盖提交校验、启用缓存、停用缓存等动作。

策略管理接口（列表/明细/登记/动作/导出）保持原有路径与出入参不变；另增共享缓存运行时
只读观察接口（``/entries``、``/stats``）与单键清理接口，用于排查多进程共享缓存状态。
"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException, Query

from app.schemas import ActionResult, EntryPayload, PageResult
from app.services.cache import CacheService
from app.services.cache_runtime import query_cache

router = APIRouter(prefix="/api/cache", tags=["查询缓存"])

service = CacheService(backend=query_cache.backend)

LIST_FIELDS = ["策略编号", "目标查询", "缓存键规则", "过期秒数", "刷新触发", "命中率阈值", "最大条目数", "淘汰算法"]
STATUSES = ["草稿", "待校验", "已启用", "已停用"]


# ------------------------------------------------------------- 共享缓存运行时（字面量路由先行）
@router.get("/entries")
def list_cache_entries(
    state: str | None = Query(default=None, description="ready、filling、quarantined"),
) -> dict[str, Any]:
    """查看 SQLite 共享后端里的真实缓存条目（含租约、fence、过期时点与隔离原因）。"""
    if state is not None and state not in ("ready", "filling", "quarantined"):
        raise HTTPException(status_code=400, detail="state 仅支持 ready、filling、quarantined")
    items = query_cache.list_cache_entries(state=state)
    return {"module": "cache", "total": len(items), "items": items, "owner": query_cache.owner}


@router.get("/stats")
def cache_stats() -> dict[str, Any]:
    """共享缓存运行统计：就绪/填充中/隔离/已过期条目数与数据库位置。"""
    return query_cache.cache_stats()


@router.delete("/entries/{cache_key:path}")
def delete_cache_entry(cache_key: str) -> ActionResult:
    """清理单个共享缓存键，便于隔离后人工触发重新计算。"""
    deleted = query_cache.delete_cache_entry(cache_key)
    if not deleted:
        return ActionResult(ok=False, message=f"缓存键 {cache_key} 不存在或后端不可用")
    return ActionResult(ok=True, message=f"缓存键 {cache_key} 已清理，下次查询将重新计算")


# ------------------------------------------------------------- 原有策略管理接口（保持兼容）
@router.get("", response_model=PageResult[dict])
def list_entries(
    keyword: str | None = Query(default=None, description="按策略编号检索"),
    status: str | None = Query(default=None, description="草稿、待校验、已启用、已停用"),
    page: int = 1,
    size: int = 20,
) -> PageResult[dict]:
    """按策略编号与状态过滤查询缓存列表；没有数据时返回空页，不报错。"""
    if size > 200:
        raise HTTPException(status_code=400, detail="每页最多 200 条，请缩小分页范围")
    items, total = service.list_entries(keyword=keyword, status=status, page=page, size=size)
    return PageResult(items=items, total=total, page=page, size=size)


@router.get("/export")
def export_entries() -> dict[str, Any]:
    """导出查询缓存清单：返回当前过滤条件下的全量数据。"""
    items, total = service.list_entries(page=1, size=10000)
    return {"module": "cache", "total": total, "items": items}


@router.get("/{entry_id}", response_model=dict)
def get_entry(entry_id: int) -> dict:
    """读取单条缓存策略明细；不存在时给出可读的错误说明。"""
    entry = service.get_entry(entry_id)
    if entry is None:
        raise HTTPException(status_code=404, detail=f"缓存策略 {entry_id} 不存在或已归档")
    return entry


@router.post("", response_model=ActionResult)
def create_entry(payload: EntryPayload) -> ActionResult:
    """登记一条缓存策略，缺字段时说明原因而不是静默丢弃。"""
    entry, missing = service.create_entry(payload.values)
    if missing:
        return ActionResult(ok=False, message=f"缺少必填字段：{'、'.join(missing)}")
    return ActionResult(ok=True, message="缓存策略已登记", entry=entry)


@router.post("/{entry_id}/actions", response_model=ActionResult)
def run_action(entry_id: int, payload: EntryPayload) -> ActionResult:
    """对单条缓存策略执行提交校验、启用缓存、停用缓存；不允许的动作会被拦下并说明原因。"""
    action = str(payload.values.get("action") or "").strip()
    entry, message = service.run_action(entry_id, action)
    if entry is None:
        return ActionResult(ok=False, message=message)
    return ActionResult(ok=True, message=message, entry=entry)
