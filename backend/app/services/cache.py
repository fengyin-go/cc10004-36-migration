"""查询缓存业务规则：状态流转、字段校验与筛选口径都收在这里。

策略本身仍通过原有内存仓库与既有接口维护（保持策略页面合法接口兼容），同时把策略
同步到 SQLite 共享后端：策略编号原样保留，无法解析的过期秒数标记为「待确认」。
旧样例数据只是策略样例，不会被当成真实缓存命中条目。
"""
from __future__ import annotations

import logging
from typing import Any

from app.query_cache import CacheBackendError, QueryCacheStore
from app.store import store

logger = logging.getLogger("app.query_cache")

MODULE = "cache"
REQUIRED_FIELDS = ["策略编号", "目标查询", "缓存键规则"]
STATUS_ORDER = ["草稿", "待校验", "已启用", "已停用"]
ACTION_RULES = {"提交校验": "待校验", "启用缓存": "已启用", "停用缓存": "已停用"}
NEGATIVE_ACTIONS = ["停用缓存"]


class CacheService:
    def __init__(self, backend: QueryCacheStore | None = None) -> None:
        # 后端不可用时为 None，所有缓存能力静默降级，策略接口照常工作
        self.backend = backend

    # ------------------------------------------------------------ 策略同步
    def sync_strategy(self, entry: dict[str, Any]) -> bool:
        """把一条策略同步到共享后端，返回是否成功。失败只记日志，不影响策略管理。"""
        if self.backend is None or not self.backend.available:
            return False
        try:
            self.backend.upsert_strategy(entry)
            return True
        except CacheBackendError as exc:
            logger.warning("缓存策略 %s 同步到 SQLite 失败，本次绕过缓存后端：%s",
                           entry.get("策略编号"), exc)
            return False

    def sync_all_strategies(self) -> int:
        """启动时把现有策略全量迁移到共享后端，策略编号保持不变。"""
        if self.backend is None or not self.backend.available:
            return 0
        synced = 0
        for entry in store.rows(MODULE):
            if self.sync_strategy(entry):
                synced += 1
        return synced

    def enabled_strategies(self) -> list[dict[str, Any]]:
        return [row for row in store.rows(MODULE) if row.get("status") == "已启用"]

    def strategies_for_target(self, query_entry: dict[str, Any]) -> list[dict[str, Any]]:
        """匹配与某条查询语句相关的已启用策略。

        策略的「目标查询」可能填写语句编号，也可能填写目标数据集名称，两者都纳入匹配；
        字段为空的策略不参与自动匹配。
        """
        candidates = {
            str(query_entry.get("语句编号") or "").strip(),
            str(query_entry.get("目标数据集") or "").strip(),
            str(query_entry.get("查询表达式") or "").strip(),
        }
        candidates.discard("")
        if not candidates:
            return []
        return [
            row for row in self.enabled_strategies()
            if str(row.get("目标查询") or "").strip() in candidates
        ]

    # ------------------------------------------------------------ 原有策略接口（签名保持不变）
    def list_entries(
        self,
        *,
        keyword: str | None = None,
        status: str | None = None,
        page: int = 1,
        size: int = 20,
    ) -> tuple[list[dict[str, Any]], int]:
        rows = store.rows(MODULE)
        if keyword:
            rows = [row for row in rows if keyword in str(row.get("策略编号", ""))]
        if status:
            rows = [row for row in rows if row.get("status") == status]
        total = len(rows)
        start = max(page - 1, 0) * size
        return rows[start:start + size], total

    def get_entry(self, entry_id: int) -> dict[str, Any] | None:
        return store.find(MODULE, entry_id)

    def create_entry(self, values: dict[str, Any]) -> tuple[dict[str, Any] | None, list[str]]:
        missing = [field for field in REQUIRED_FIELDS if not str(values.get(field) or "").strip()]
        if missing:
            return None, missing
        rows = store.rows(MODULE)
        entry = {"id": max((int(row.get("id", 0)) for row in rows), default=0) + 1}
        entry.update({field: values.get(field) for field in REQUIRED_FIELDS})
        # 迁移期允许补充过期秒数等可选字段；未提供则留空，落库时标记为待确认
        for optional in ("过期秒数", "刷新触发", "命中率阈值", "最大条目数", "淘汰算法"):
            if optional in values:
                entry[optional] = values.get(optional)
        entry["status"] = STATUS_ORDER[0]
        entry["pending"] = True
        entry["abnormal"] = False
        rows.append(entry)
        self.sync_strategy(entry)
        return entry, []

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        entry = store.find(MODULE, entry_id)
        if entry is None:
            return None, f"缓存策略 {entry_id} 不存在或已归档"
        if action not in ACTION_RULES:
            return None, f"动作「{action}」不属于查询缓存可执行范围"
        target = ACTION_RULES[action]
        if target not in STATUS_ORDER:
            return None, f"目标状态「{target}」不在允许的状态序列里"
        entry["status"] = target
        entry["pending"] = target != STATUS_ORDER[-1]
        entry["abnormal"] = action in NEGATIVE_ACTIONS
        self.sync_strategy(entry)
        return entry, f"缓存策略已{action}"
