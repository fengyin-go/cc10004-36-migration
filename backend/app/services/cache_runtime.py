"""跨模块共享的查询缓存运行时（外观层）。

交互查询等模块通过这里访问 SQLite 共享缓存，而不直接操作后端：

* 缓存键由「策略 + 查询完整身份」决定，同一目标查询在多个 FastAPI 进程间共享同一份事实；
* 每条读写都携带完整身份（``identity``）与绝对过期时点（``expires_at_epoch_ms``），
  过期时点在取得租约时固化，进程重启不会重新计算，因此不会延长生存期；
* 同键并发只有一个有效写入者，其它进程短暂等待新结果；租约持有者退出后，租约到期
  即可被其它进程接管，接管会推进 fence，晚到的旧写入无法覆盖新结果；
* 后端任何落库失败都被吞掉并明确降级为「绕过缓存」，查询结果始终照常返回，
  返回结构里用 ``cache.bypassed`` / ``cache.reason`` 说明绕过原因。
"""
from __future__ import annotations

import hashlib
import json
import logging
import time
from typing import Any, Callable

from app.config import settings
from app.query_cache import (
    CacheBackendError,
    QueryCacheStore,
    now_ms,
    parse_ttl,
    process_identity,
)
from app.services.cache import CacheService

logger = logging.getLogger("app.query_cache")

PROCESS_IDENTITY = process_identity()


def stable_hash(payload: Any) -> str:
    """对任意可 JSON 化结构计算稳定指纹，用作身份哈希与缓存键后缀。"""
    blob = json.dumps(payload, ensure_ascii=False, sort_keys=True, default=str)
    return hashlib.sha256(blob.encode("utf-8")).hexdigest()


class QueryCacheRuntime:
    """共享缓存外观；后端初始化失败时整体降级为直算模式。"""

    def __init__(self) -> None:
        self.owner = PROCESS_IDENTITY
        self.policy_service = CacheService()
        try:
            self.backend: QueryCacheStore | None = QueryCacheStore()
        except CacheBackendError as exc:
            logger.error("SQLite 缓存后端初始化失败，进程将全程绕过缓存：%s", exc)
            self.backend = None
        self.policy_service.backend = self.backend

    # --------------------------------------------------------------- 键与身份
    def build_cache_key(self, strategy: dict[str, Any], identity: dict[str, Any]) -> str:
        # 键只由稳定的查询身份决定（语句、表达式、数据集、入参），不能混入 source_process、
        # created_at_ms 这类每次请求/每个进程都变化的字段，否则同一查询在不同进程会得到不同键。
        stable_identity = {
            "语句编号": identity.get("语句编号"),
            "查询表达式": identity.get("查询表达式"),
            "目标数据集": identity.get("目标数据集"),
            "参数": identity.get("参数"),
        }
        rule = str(strategy.get("缓存键规则") or "").strip() or "sha256(identity)"
        basis = {
            "策略编号": strategy.get("策略编号"),
            "目标查询": strategy.get("目标查询"),
            "缓存键规则": rule,
            "identity": stable_identity,
        }
        return f"{strategy.get('策略编号') or 'CACH'}:{stable_hash(basis)[:32]}"

    @staticmethod
    def build_identity(*, query_entry: dict[str, Any], params: dict[str, Any]) -> dict[str, Any]:
        """组装完整查询身份：语句编号、表达式、数据集、入参与发起进程都纳入指纹。"""
        core = {
            "语句编号": query_entry.get("语句编号"),
            "查询表达式": query_entry.get("查询表达式"),
            "目标数据集": query_entry.get("目标数据集"),
            "参数": params or {},
        }
        return {
            "identity_hash": stable_hash(core),
            "语句编号": core["语句编号"],
            "查询表达式": core["查询表达式"],
            "目标数据集": core["目标数据集"],
            "参数": core["参数"],
            "source_process": PROCESS_IDENTITY,
            "created_at_ms": now_ms(),
        }

    # --------------------------------------------------------------- 主入口
    def execute_with_cache(
        self,
        *,
        query_entry: dict[str, Any],
        params: dict[str, Any],
        strategies: list[dict[str, Any]],
        calculator: Callable[[], Any],
    ) -> dict[str, Any]:
        """带缓存执行一次查询，返回结果与缓存决策说明。

        输出形如::

            {"result": ..., "cache": {"status": "hit|miss|bypassed", ...}}
        """
        identity = self.build_identity(query_entry=query_entry, params=params)

        if self.backend is None or not self.backend.available:
            return self._bypass(calculator, identity, reason="缓存后端不可用，已绕过缓存直接计算")

        try:
            # 一个目标查询可能挂多条策略；选第一条可解析过期值的已启用策略，其余仅记录
            chosen: dict[str, Any] | None = None
            ttl_seconds: int | None = None
            for strategy in strategies:
                record = self.backend.get_strategy(int(strategy["id"]))
                if record is None:
                    # 策略尚未落库（例如同步失败过）：先补同步，再读；仍失败则就地解析，
                    # 保证缓存能力不会因为策略表暂时不可写而整体失效
                    self.policy_service.sync_strategy(strategy)
                    record = self.backend.get_strategy(int(strategy["id"]))
                if record is not None:
                    unknown = int(record.get("ttl_unknown") or 0) == 1
                    candidate_ttl = record.get("ttl_seconds")
                else:
                    candidate_ttl, unknown, _raw = parse_ttl(strategy.get("过期秒数"))
                if unknown:
                    logger.info("策略 %s 的过期秒数待确认（原值=%r），不自动缓存",
                                strategy.get("策略编号"), strategy.get("过期秒数"))
                    continue
                chosen = strategy
                ttl_seconds = candidate_ttl
                break

            if chosen is None:
                return self._bypass(
                    calculator, identity,
                    reason="没有可确认过期时点的启用策略（过期秒数未知或待确认），绕过缓存",
                )

            cache_key = self.build_cache_key(chosen, identity)
            return self._run_locked_path(
                cache_key=cache_key,
                identity=identity,
                strategy=chosen,
                ttl_seconds=ttl_seconds,
                calculator=calculator,
            )
        except CacheBackendError as exc:
            logger.warning("缓存读写失败，本次绕过缓存：%s", exc)
            return self._bypass(calculator, identity, reason=f"缓存落库失败，已绕过：{exc}")

    # --------------------------------------------------------------- 内部流程
    def _run_locked_path(
        self,
        *,
        cache_key: str,
        identity: dict[str, Any],
        strategy: dict[str, Any],
        ttl_seconds: int | None,
        calculator: Callable[[], Any],
    ) -> dict[str, Any]:
        backend = self.backend
        assert backend is not None

        # 1) 先读共享事实
        ready = backend.get_ready_entry(cache_key)
        if ready is not None:
            return self._hit(ready)

        # 2) 尝试成为唯一写入者
        outcome = backend.acquire_lease(
            cache_key=cache_key,
            identity=identity,
            strategy=strategy,
            owner=self.owner,
            ttl_seconds=ttl_seconds,
        )

        if not outcome["acquired"]:
            if outcome.get("entry") is not None:
                # 竞争窗口里别人刚好写完
                return self._hit(outcome["entry"])
            # 其它进程正在填充：有限等待，读不到就绕过缓存自行计算（不抢租约）
            waited = self._wait_for_ready(cache_key)
            if waited is not None:
                return self._hit(waited)
            result = calculator()
            return {
                "result": result,
                "cache": {
                    "status": "bypassed",
                    "cache_key": cache_key,
                    "reason": "同键缓存正在由其它进程填充，等待超时后绕过缓存直接计算",
                    "identity_hash": identity["identity_hash"],
                    "identity": identity,
                    "bypassed": True,
                },
            }

        lease = outcome["lease"]
        fence_token = int(lease["fence_token"])
        try:
            result = calculator()
        except Exception:
            backend.release_lease(cache_key=cache_key, fence_token=fence_token, owner=self.owner)
            raise

        committed = backend.commit_fill(
            cache_key=cache_key,
            fence_token=fence_token,
            owner=self.owner,
            result=result,
        )
        if not committed["committed"]:
            # 典型场景：本进程在填充期间被接管，fence 已推进，旧写入被拒绝
            logger.info("缓存写入被拒绝（%s），查询结果照常返回", committed["reason"])
            latest = backend.get_ready_entry(cache_key)
            return {
                "result": result,
                "cache": {
                    "status": "bypassed",
                    "cache_key": cache_key,
                    "reason": committed["reason"],
                    "identity_hash": identity["identity_hash"],
                    "identity": identity,
                    "bypassed": True,
                    "superseded_by": latest["identity_hash"] if latest else None,
                },
            }

        entry = committed["entry"]
        return {
            "result": result,
            "cache": {
                "status": "miss",
                "cache_key": cache_key,
                "filled_by": self.owner,
                "fence_token": fence_token,
                "identity_hash": identity["identity_hash"],
                "identity": identity,
                "expires_at_epoch_ms": entry["expires_at_epoch_ms"] if entry else lease.get("expires_at_epoch_ms"),
                "lease_expires_ms": lease.get("lease_expires_ms"),
                "bypassed": False,
            },
        }

    def _wait_for_ready(self, cache_key: str) -> dict[str, Any] | None:
        backend = self.backend
        assert backend is not None
        deadline = now_ms() + settings.query_cache_wait_ms
        step = max(settings.query_cache_wait_step_ms, 10) / 1000.0
        while now_ms() < deadline:
            time.sleep(step)
            try:
                ready = backend.get_ready_entry(cache_key)
            except CacheBackendError as exc:
                logger.warning("等待共享缓存结果时读取失败：%s", exc)
                return None
            if ready is not None:
                return ready
        return None

    @staticmethod
    def _hit(entry: dict[str, Any]) -> dict[str, Any]:
        identity = entry["identity"]
        return {
            "result": entry["result"],
            "cache": {
                "status": "hit",
                "cache_key": entry["cache_key"],
                "identity_hash": entry["identity_hash"],
                "identity": identity,
                "created_at_ms": entry["created_at_ms"],
                "updated_at_ms": entry["updated_at_ms"],
                # 绝对过期时点原样回传，不重新计算
                "expires_at_epoch_ms": entry["expires_at_epoch_ms"],
                "fence_token": entry["fence_token"],
                "bypassed": False,
            },
        }

    def _bypass(
        self,
        calculator: Callable[[], Any],
        identity: dict[str, Any],
        *,
        reason: str,
    ) -> dict[str, Any]:
        result = calculator()
        return {
            "result": result,
            "cache": {
                "status": "bypassed",
                "reason": reason,
                "identity_hash": identity["identity_hash"],
                "identity": identity,
                "bypassed": True,
            },
        }

    # --------------------------------------------------------------- 运维辅助
    def list_cache_entries(self, state: str | None = None) -> list[dict[str, Any]]:
        if self.backend is None:
            return []
        return self.backend.list_entries(state=state)

    def cache_stats(self) -> dict[str, Any]:
        if self.backend is None:
            return {"available": False, "owner": self.owner}
        try:
            stats = self.backend.stats()
        except CacheBackendError as exc:
            # 运行期间数据库变得不可用也要如实反映，而不是让健康检查 500
            return {"available": False, "owner": self.owner, "reason": str(exc)}
        stats["available"] = True
        stats["owner"] = self.owner
        return stats

    def delete_cache_entry(self, cache_key: str) -> bool:
        if self.backend is None:
            return False
        return self.backend.delete_entry(cache_key)


# 单例：进程内共享；跨进程则通过同一个 SQLite 文件共享
query_cache = QueryCacheRuntime()


def reset_query_cache(db_path: str | None = None, lease_ms: int | None = None) -> QueryCacheRuntime:
    """测试/运维用途：用指定数据库位置重建单例，返回新实例。"""
    global query_cache
    runtime = QueryCacheRuntime.__new__(QueryCacheRuntime)
    runtime.owner = PROCESS_IDENTITY
    try:
        runtime.backend = QueryCacheStore(db_path=db_path, lease_ms=lease_ms)
    except CacheBackendError as exc:
        logger.error("重建 SQLite 缓存后端失败，将绕过缓存：%s", exc)
        runtime.backend = None
    runtime.policy_service = CacheService(backend=runtime.backend)
    query_cache = runtime
    return runtime
