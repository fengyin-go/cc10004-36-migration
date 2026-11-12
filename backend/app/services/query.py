"""交互查询业务规则：状态流转、字段校验与筛选口径都收在这里。

``execute_entry`` 是跨模块的查询执行入口：命中共享缓存时直接返回共享事实，未命中时
计算并把「完整身份 + 绝对过期时点 + 结果」写回 SQLite 共享缓存；缓存不可用则明确绕过，
查询结果不受影响。
"""
from __future__ import annotations

import hashlib
from typing import Any

from app.services import cache_runtime
from app.store import store

MODULE = "query"
REQUIRED_FIELDS = ["语句编号", "查询表达式", "目标数据集"]
STATUS_ORDER = ["草稿", "待执行", "执行中", "已完成", "已超时"]
ACTION_RULES = {"提交执行": "待执行", "启动查询": "执行中", "终止查询": "已超时", "保存结果": "已完成"}
NEGATIVE_ACTIONS = []


class QueryService:
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
            rows = [row for row in rows if keyword in str(row.get("语句编号", ""))]
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
        entry["status"] = STATUS_ORDER[0]
        entry["pending"] = True
        entry["abnormal"] = False
        rows.append(entry)
        return entry, []

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        entry = store.find(MODULE, entry_id)
        if entry is None:
            return None, f"查询语句 {entry_id} 不存在或已归档"
        if action not in ACTION_RULES:
            return None, f"动作「{action}」不属于交互查询可执行范围"
        target = ACTION_RULES[action]
        if target not in STATUS_ORDER:
            return None, f"目标状态「{target}」不在允许的状态序列里"
        entry["status"] = target
        entry["pending"] = target != STATUS_ORDER[-1]
        entry["abnormal"] = action in NEGATIVE_ACTIONS
        return entry, f"查询语句已{action}"

    # ---------------------------------------------------------- 共享缓存执行
    def execute_entry(
        self,
        entry_id: int,
        params: dict[str, Any] | None = None,
    ) -> tuple[dict[str, Any] | None, str | None]:
        """执行查询并接入共享缓存。

        返回 ``(payload, error)``：成功时 payload 包含查询语句、结果与缓存决策；
        查询语句不存在时返回错误说明。缓存层任何失败都不会让该接口失败。
        """
        entry = store.find(MODULE, entry_id)
        if entry is None:
            return None, f"查询语句 {entry_id} 不存在或已归档"

        params = params or {}
        strategies = cache_runtime.query_cache.policy_service.strategies_for_target(entry)

        def calculate() -> dict[str, Any]:
            return self._calculate(entry, params)

        outcome = cache_runtime.query_cache.execute_with_cache(
            query_entry=entry,
            params=params,
            strategies=strategies,
            calculator=calculate,
        )
        return {
            "语句编号": entry.get("语句编号"),
            "查询表达式": entry.get("查询表达式"),
            "目标数据集": entry.get("目标数据集"),
            "params": params,
            "result": outcome["result"],
            "cache": outcome["cache"],
        }, None

    @staticmethod
    def _calculate(entry: dict[str, Any], params: dict[str, Any]) -> dict[str, Any]:
        """本工作台没有真实数据源，这里给出确定性的合成结果，便于验证缓存一致性。"""
        basis = "|".join(
            str(entry.get(field) or "") for field in ("语句编号", "查询表达式", "目标数据集")
        )
        digest = hashlib.sha256(f"{basis}|{params}".encode("utf-8")).hexdigest()
        return {
            "rows": [
                {"序号": 1, "指纹": digest[:12], "来源": entry.get("目标数据集")},
                {"序号": 2, "指纹": digest[12:24], "来源": entry.get("目标数据集")},
            ],
            "row_count": 2,
            "fingerprint": digest,
            "computed_by": cache_runtime.query_cache.owner,
        }
