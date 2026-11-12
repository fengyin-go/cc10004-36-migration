"""共享查询缓存端到端测试。

用真实多进程（``multiprocessing``）访问同一个 SQLite 文件，验证：

1. 多进程共享同一份缓存事实，命中带回完整身份与绝对过期时点；
2. 进程重启不会延长过期时点（过期时间在首次填充时固化）；
3. 同键并发只有一个有效写入者；
4. 租用者退出后其它进程可接管，且晚到的旧写入（旧 fence）不能覆盖新结果；
5. 损坏条目被隔离，同键可重新计算，其它键不受影响；
6. 未知过期值标记为待确认，不产生真实缓存命中；旧样例内容不会被当成缓存命中；
7. 落库失败时明确绕过缓存并保留查询结果；
8. 策略迁移保留原策略编号；
9. 策略页面原有接口保持兼容。

直接运行：``.venv/bin/python -m unittest app.tests.test_query_cache -v``
"""
from __future__ import annotations

import json
import os
import sqlite3
import tempfile
import time
import unittest
import uuid
from multiprocessing import get_context

from app.config import settings
from app.query_cache import (
    STATE_FILLING,
    STATE_QUARANTINED,
    STATE_READY,
    CacheBackendError,
    QueryCacheStore,
    now_ms,
    parse_ttl,
)
from app.services.cache import CacheService
from app.services.cache_runtime import QueryCacheRuntime, reset_query_cache
from app.services.query import QueryService
from app.store import store

MP_CTX = get_context("spawn")


def _new_db_path() -> str:
    tmp_dir = tempfile.mkdtemp(prefix="qcache-test-")
    return os.path.join(tmp_dir, "cache.sqlite3")


def _identity(tag: str) -> dict:
    return QueryCacheRuntime.build_identity(
        query_entry={"语句编号": f"QUER-{tag}", "查询表达式": "expr", "目标数据集": "set-A"},
        params={"page": 1},
    )


def _strategy(strategy_id: int = 3, ttl: object = 300) -> dict:
    return {
        "id": strategy_id,
        "策略编号": f"CACH-{strategy_id:04d}",
        "目标查询": "set-A",
        "缓存键规则": "sha256(语句编号+参数)",
        "过期秒数": ttl,
        "status": "已启用",
    }


# ---------------------------------------------------------------- 多进程 worker
def _worker_fill(db_path: str, cache_key: str, tag: str, ready_token: str, barrier_token: str) -> None:
    """子进程：获取租约，等同步事件后再提交，用于制造同键并发。"""
    backend = QueryCacheStore(db_path=db_path, lease_ms=30_000)
    runtime_owner = f"worker-{tag}-{os.getpid()}"
    identity = _identity(tag)
    strategy = _strategy()
    outcome = backend.acquire_lease(
        cache_key=cache_key, identity=identity, strategy=strategy,
        owner=runtime_owner, ttl_seconds=300,
    )
    with open(ready_token, "w", encoding="utf-8") as fh:
        fh.write(json.dumps({"acquired": outcome["acquired"], "fence": outcome["lease"]["fence_token"] if outcome["lease"] else None}))
    # 等到主进程放行
    deadline = time.time() + 20
    while not os.path.exists(barrier_token) and time.time() < deadline:
        time.sleep(0.02)
    if outcome["acquired"]:
        backend.commit_fill(
            cache_key=cache_key, fence_token=outcome["lease"]["fence_token"],
            owner=runtime_owner, result={"by": runtime_owner, "tag": tag},
        )


def _worker_takeover_commit(db_path: str, cache_key: str, old_fence: int, token: str) -> None:
    """子进程：接管后用旧 fence 提交，必须被拒绝；再用新 fence 正常写入。"""
    backend = QueryCacheStore(db_path=db_path, lease_ms=500)
    owner = f"takeover-{os.getpid()}"
    identity = _identity("takeover")
    stale = backend.commit_fill(
        cache_key=cache_key, fence_token=old_fence, owner=owner, result={"stale": True},
    )
    outcome = backend.acquire_lease(
        cache_key=cache_key, identity=identity, strategy=_strategy(),
        owner=owner, ttl_seconds=300,
    )
    new_fence = outcome["lease"]["fence_token"] if outcome["acquired"] else None
    committed = {"committed": False}
    if outcome["acquired"]:
        committed = backend.commit_fill(
            cache_key=cache_key, fence_token=new_fence, owner=owner,
            result={"by": owner, "fresh": True},
        )
    with open(token, "w", encoding="utf-8") as fh:
        json.dump({"stale_rejected": not stale["committed"], "acquired": outcome["acquired"],
                   "new_fence": new_fence, "committed": committed["committed"]}, fh, ensure_ascii=False)


class QueryCacheTests(unittest.TestCase):
    def setUp(self) -> None:
        self.db_path = _new_db_path()
        self.backend = QueryCacheStore(db_path=self.db_path, lease_ms=800)
        self.runtime = QueryCacheRuntime.__new__(QueryCacheRuntime)
        # 手工装配，避免单例占用默认路径
        from app.services.cache_runtime import PROCESS_IDENTITY
        self.runtime.owner = PROCESS_IDENTITY
        self.runtime.backend = self.backend
        self.runtime.policy_service = CacheService(backend=self.backend)

    def tearDown(self) -> None:
        import shutil

        parent = os.path.dirname(self.db_path)
        for name in os.listdir(parent) if os.path.isdir(parent) else []:
            os.remove(os.path.join(parent, name))
        if os.path.isdir(parent):
            shutil.rmtree(parent, ignore_errors=True)

    # ------------------------------------------------------------ 1. 基础读写与身份/过期
    def test_lease_then_commit_carries_identity_and_absolute_expiry(self) -> None:
        key = "key-basic"
        identity = _identity("basic")
        before = now_ms()
        outcome = self.backend.acquire_lease(
            cache_key=key, identity=identity, strategy=_strategy(),
            owner=self.runtime.owner, ttl_seconds=300,
        )
        self.assertTrue(outcome["acquired"])
        lease = outcome["lease"]
        expected_expiry_before = before + 300_000
        self.assertIsNotNone(lease["expires_at_epoch_ms"])
        committed = self.backend.commit_fill(
            cache_key=key, fence_token=lease["fence_token"],
            owner=self.runtime.owner, result={"rows": [1, 2]},
        )
        self.assertTrue(committed["committed"])
        entry = self.backend.get_ready_entry(key)
        self.assertIsNotNone(entry)
        assert entry is not None
        self.assertEqual(entry["state"], STATE_READY)
        self.assertEqual(entry["identity_hash"], identity["identity_hash"])
        self.assertEqual(entry["identity"]["语句编号"], "QUER-basic")
        self.assertEqual(entry["identity"]["source_process"], identity["source_process"])
        self.assertEqual(entry["expires_at_epoch_ms"], lease["expires_at_epoch_ms"])
        self.assertLessEqual(abs(entry["expires_at_epoch_ms"] - expected_expiry_before), 5_000)
        self.assertEqual(entry["result"], {"rows": [1, 2]})

    def test_expiry_absolute_not_extended_by_reopen(self) -> None:
        """重新打开连接（模拟进程重启）后，过期时点保持原值，不重新计算。"""
        key = "key-restart"
        identity = _identity("restart")
        outcome = self.backend.acquire_lease(
            cache_key=key, identity=identity, strategy=_strategy(ttl=1),
            owner=self.runtime.owner, ttl_seconds=1,
        )
        lease = outcome["lease"]
        self.backend.commit_fill(
            cache_key=key, fence_token=lease["fence_token"],
            owner=self.runtime.owner, result={"v": 1},
        )
        first_expiry = lease["expires_at_epoch_ms"]
        time.sleep(0.2)
        reopened = QueryCacheStore(db_path=self.db_path, lease_ms=800)
        entry = reopened.get_ready_entry(key)
        self.assertIsNotNone(entry)
        assert entry is not None
        self.assertEqual(entry["expires_at_epoch_ms"], first_expiry)
        # 1 秒后到期，按未命中处理（但行仍在，身份与过期时点未被改写）
        time.sleep(1.0)
        self.assertIsNone(reopened.get_ready_entry(key))
        row = reopened._fetch_row(key)  # noqa: SLF001
        self.assertEqual(row["expires_at_epoch_ms"], first_expiry)

    # ------------------------------------------------------------ 2. 并发单写入者（真实多进程）
    def test_concurrent_same_key_single_writer_across_processes(self) -> None:
        key = "key-concurrent"
        ready_token = os.path.join(os.path.dirname(self.db_path), "ready.json")
        barrier = os.path.join(os.path.dirname(self.db_path), "barrier")
        proc = MP_CTX.Process(
            target=_worker_fill,
            args=(self.db_path, key, "concurrent", ready_token, barrier),
        )
        proc.start()
        deadline = time.time() + 10
        while not os.path.exists(ready_token) and time.time() < deadline:
            time.sleep(0.02)
        with open(ready_token, encoding="utf-8") as fh:
            worker_state = json.load(fh)
        self.assertTrue(worker_state["acquired"])

        # 主进程立刻竞争同一把租约：必须被拒
        outcome = self.backend.acquire_lease(
            cache_key=key, identity=_identity("concurrent"), strategy=_strategy(),
            owner=self.runtime.owner, ttl_seconds=300,
        )
        self.assertFalse(outcome["acquired"])
        self.assertIsNone(outcome["entry"])

        # 放行 worker 提交
        open(barrier, "w").close()
        proc.join(15)
        self.assertEqual(proc.exitcode, 0)

        entry = self.backend.get_ready_entry(key)
        self.assertIsNotNone(entry)
        assert entry is not None
        self.assertEqual(entry["result"]["tag"], "concurrent")

    def test_second_process_reads_shared_fact(self) -> None:
        """进程 A 填充后，新进程 B 对同一身份读取应命中同一份事实。"""
        key = "key-shared"
        identity = _identity("shared")
        outcome = self.backend.acquire_lease(
            cache_key=key, identity=identity, strategy=_strategy(),
            owner="owner-A", ttl_seconds=300,
        )
        self.backend.commit_fill(
            cache_key=key, fence_token=outcome["lease"]["fence_token"],
            owner="owner-A", result={"fact": "shared"},
        )
        other = QueryCacheStore(db_path=self.db_path)
        hit = other.get_ready_entry(key)
        self.assertIsNotNone(hit)
        assert hit is not None
        self.assertEqual(hit["result"], {"fact": "shared"})
        self.assertEqual(hit["identity_hash"], identity["identity_hash"])

    # ------------------------------------------------------------ 3. 接管与旧 fence 拒绝
    def test_takeover_after_lease_exit_and_stale_write_rejected(self) -> None:
        key = "key-takeover"
        first = self.backend.acquire_lease(
            cache_key=key, identity=_identity("old"), strategy=_strategy(),
            owner="dead-owner", ttl_seconds=300,
        )
        old_fence = first["lease"]["fence_token"]
        # 租用者未提交即退出；等待租约到期
        time.sleep(0.9)

        token = os.path.join(os.path.dirname(self.db_path), "takeover.json")
        proc = MP_CTX.Process(target=_worker_takeover_commit, args=(self.db_path, key, old_fence, token))
        proc.start()
        proc.join(15)
        self.assertEqual(proc.exitcode, 0)
        with open(token, encoding="utf-8") as fh:
            state = json.load(fh)
        self.assertTrue(state["stale_rejected"], "晚到的旧 fence 写入必须被拒绝")
        self.assertTrue(state["acquired"])
        self.assertGreater(state["new_fence"], old_fence)
        self.assertTrue(state["committed"])

        entry = self.backend.get_ready_entry(key)
        self.assertIsNotNone(entry)
        assert entry is not None
        self.assertTrue(entry["result"]["fresh"])
        self.assertEqual(entry["fence_token"], state["new_fence"])

        # 原租用者晚到的提交仍会被拒
        stale = self.backend.commit_fill(
            cache_key=key, fence_token=old_fence, owner="dead-owner", result={"stale": True},
        )
        self.assertFalse(stale["committed"])
        entry2 = self.backend.get_ready_entry(key)
        assert entry2 is not None
        self.assertTrue(entry2["result"]["fresh"])

    def test_commit_with_wrong_owner_rejected(self) -> None:
        key = "key-owner"
        outcome = self.backend.acquire_lease(
            cache_key=key, identity=_identity("owner-x"), strategy=_strategy(),
            owner="owner-x", ttl_seconds=300,
        )
        fence = outcome["lease"]["fence_token"]
        wrong = self.backend.commit_fill(
            cache_key=key, fence_token=fence, owner="owner-y", result={"x": 1},
        )
        self.assertFalse(wrong["committed"])

    # ------------------------------------------------------------ 4. 损坏隔离
    def test_corrupt_entry_quarantined_and_recomputable_others_unaffected(self) -> None:
        good_key = "key-good"
        bad_key = "key-bad"
        for key, tag in ((good_key, "good"), (bad_key, "bad")):
            outcome = self.backend.acquire_lease(
                cache_key=key, identity=_identity(tag), strategy=_strategy(),
                owner=self.runtime.owner, ttl_seconds=300,
            )
            self.backend.commit_fill(
                cache_key=key, fence_token=outcome["lease"]["fence_token"],
                owner=self.runtime.owner, result={"tag": tag},
            )
        # 直接破坏坏键的结果负载
        conn = sqlite3.connect(self.db_path)
        try:
            conn.execute("UPDATE cache_entries SET result_payload = ? WHERE cache_key = ?",
                         ("{不是合法json", bad_key))
            conn.commit()
        finally:
            conn.close()

        self.assertIsNone(self.backend.get_ready_entry(bad_key))
        rows = {r["cache_key"]: r for r in self.backend.list_entries()}
        self.assertEqual(rows[bad_key]["state"], STATE_QUARANTINED)
        self.assertTrue(rows[bad_key]["corrupt_reason"])
        # 好键完全不受影响
        good = self.backend.get_ready_entry(good_key)
        self.assertIsNotNone(good)
        assert good is not None
        self.assertEqual(good["result"]["tag"], "good")

        # 坏键允许重新计算
        outcome = self.backend.acquire_lease(
            cache_key=bad_key, identity=_identity("bad"), strategy=_strategy(),
            owner=self.runtime.owner, ttl_seconds=300,
        )
        self.assertTrue(outcome["acquired"])
        self.backend.commit_fill(
            cache_key=bad_key, fence_token=outcome["lease"]["fence_token"],
            owner=self.runtime.owner, result={"tag": "recovered"},
        )
        recovered = self.backend.get_ready_entry(bad_key)
        self.assertIsNotNone(recovered)
        assert recovered is not None
        self.assertEqual(recovered["result"]["tag"], "recovered")

    # ------------------------------------------------------------ 5. TTL 未知/样例不当命中
    def test_unknown_ttl_marked_and_runtime_bypasses(self) -> None:
        self.assertEqual(parse_ttl("查询缓存样例1"), (None, True, "查询缓存样例1"))
        self.assertEqual(parse_ttl(""), (None, True, ""))
        self.assertEqual(parse_ttl(None), (None, True, None))
        self.assertEqual(parse_ttl("300"), (300, False, "300"))
        self.assertEqual(parse_ttl(60), (60, False, "60"))
        self.assertEqual(parse_ttl("-1"), (None, True, "-1"))

        # 策略落库后，未知 TTL 被标记 ttl_unknown=1
        unknown_strategy = _strategy(strategy_id=9, ttl="查询缓存样例1")
        self.backend.upsert_strategy(unknown_strategy)
        record = self.backend.get_strategy(9)
        assert record is not None
        self.assertEqual(record["ttl_unknown"], 1)
        self.assertIsNone(record["ttl_seconds"])

        # 运行时对未知 TTL 策略绕过缓存，且不产生任何缓存条目
        query_entry = {"语句编号": "QUER-X", "查询表达式": "expr", "目标数据集": "set-A"}
        calls = {"n": 0}

        def calculator() -> dict:
            calls["n"] += 1
            return {"v": calls["n"]}

        outcome = self.runtime.execute_with_cache(
            query_entry=query_entry, params={}, strategies=[unknown_strategy], calculator=calculator,
        )
        self.assertEqual(outcome["cache"]["status"], "bypassed")
        self.assertTrue(outcome["cache"]["bypassed"])
        self.assertEqual(outcome["result"], {"v": 1})
        self.assertEqual(self.backend.list_entries(), [])

    def test_seed_samples_are_not_cache_hits(self) -> None:
        """全新后端的缓存条目表必须为空：旧样例内容只是策略，不是真实缓存命中。"""
        self.assertEqual(self.backend.list_entries(), [])
        self.assertEqual(self.backend.stats()["total"], 0)

    # ------------------------------------------------------------ 6. 落库失败绕过
    def test_backend_failure_bypasses_and_keeps_result(self) -> None:
        broken = QueryCacheRuntime.__new__(QueryCacheRuntime)
        broken.owner = self.runtime.owner
        broken.backend = None  # 模拟后端不可用
        broken.policy_service = CacheService(backend=None)

        outcome = broken.execute_with_cache(
            query_entry={"语句编号": "QUER-Z", "查询表达式": "e", "目标数据集": "set-A"},
            params={}, strategies=[_strategy()], calculator=lambda: {"ok": True},
        )
        self.assertEqual(outcome["result"], {"ok": True})
        self.assertEqual(outcome["cache"]["status"], "bypassed")
        self.assertIn("不可用", outcome["cache"]["reason"])

    def test_operational_error_on_commit_is_surfaced_as_backend_error(self) -> None:
        backend = QueryCacheStore(db_path=self.db_path)
        outcome = backend.acquire_lease(
            cache_key="k-err", identity=_identity("err"), strategy=_strategy(),
            owner="o", ttl_seconds=300,
        )
        # 关闭底层连接后继续操作，应转成 CacheBackendError 而不是裸 sqlite3.Error
        backend._conn().close()  # noqa: SLF001
        with self.assertRaises(CacheBackendError):
            backend.commit_fill(
                cache_key="k-err", fence_token=outcome["lease"]["fence_token"],
                owner="o", result={"v": 1},
            )

    # ------------------------------------------------------------ 7. 策略迁移与编号保留
    def test_strategy_migration_preserves_ids(self) -> None:
        cache_rows = [row for row in store.rows("cache")]
        self.assertGreaterEqual(len(cache_rows), 3)
        service = CacheService(backend=self.backend)
        synced = service.sync_all_strategies()
        self.assertEqual(synced, len(cache_rows))
        for row in cache_rows:
            record = self.backend.get_strategy(int(row["id"]))
            self.assertIsNotNone(record, f"策略 id={row['id']} 迁移后丢失")
            assert record is not None
            self.assertEqual(record["strategy_id"], row["id"])
            self.assertEqual(record["strategy_code"], row["策略编号"])
        # 样例 1/2 的文本 TTL 必须标为待确认；样例 3 的 300 秒可确认
        self.assertEqual(self.backend.get_strategy(1)["ttl_unknown"], 1)
        self.assertEqual(self.backend.get_strategy(2)["ttl_unknown"], 1)
        record3 = self.backend.get_strategy(3)
        self.assertEqual(record3["ttl_unknown"], 0)
        self.assertEqual(record3["ttl_seconds"], 300)

    # ------------------------------------------------------------ 8. 外观层命中/绕过语义
    def test_runtime_hit_returns_identity_and_expiry(self) -> None:
        runtime = reset_query_cache(db_path=self.db_path, lease_ms=800)
        query_entry = {"语句编号": "QUER-RT", "查询表达式": "expr", "目标数据集": "set-A"}
        strategy = _strategy()
        runtime.backend.upsert_strategy(strategy)

        first = runtime.execute_with_cache(
            query_entry=query_entry, params={"q": 1}, strategies=[strategy],
            calculator=lambda: {"rows": [1]},
        )
        self.assertEqual(first["cache"]["status"], "miss")
        self.assertFalse(first["cache"]["bypassed"])
        self.assertIsNotNone(first["cache"]["expires_at_epoch_ms"])

        second = runtime.execute_with_cache(
            query_entry=query_entry, params={"q": 1}, strategies=[strategy],
            calculator=lambda: {"rows": ["SHOULD_NOT_BE_USED"]},
        )
        self.assertEqual(second["cache"]["status"], "hit")
        self.assertEqual(second["result"], {"rows": [1]})
        self.assertEqual(second["cache"]["identity_hash"], first["cache"]["identity_hash"])
        self.assertEqual(
            second["cache"]["expires_at_epoch_ms"], first["cache"]["expires_at_epoch_ms"],
        )

        # 不同参数 = 不同身份 = 不同缓存键，重新计算
        other = runtime.execute_with_cache(
            query_entry=query_entry, params={"q": 2}, strategies=[strategy],
            calculator=lambda: {"rows": [2]},
        )
        self.assertEqual(other["cache"]["status"], "miss")

    # ------------------------------------------------------------ 9. QueryService 接线
    def test_query_service_execute_uses_shared_cache(self) -> None:
        reset_query_cache(db_path=self.db_path, lease_ms=800)
        # 让 seed 里的查询语句 3（目标数据集=交互查询样例3）与策略 3 对齐
        q3 = store.find("query", 3)
        self.assertEqual(q3["目标数据集"], "交互查询样例3")
        service = QueryService()
        payload, error = service.execute_entry(3, {"page": 1})
        self.assertIsNone(error)
        assert payload is not None
        self.assertEqual(payload["cache"]["status"], "miss")
        payload2, _ = service.execute_entry(3, {"page": 1})
        assert payload2 is not None
        self.assertEqual(payload2["cache"]["status"], "hit")
        self.assertEqual(payload2["result"]["fingerprint"], payload["result"]["fingerprint"])

        missing, err = service.execute_entry(999_999)
        self.assertIsNone(missing)
        self.assertIn("不存在", err)

    # ------------------------------------------------------------ 10. 配置
    def test_config_exposes_db_location(self) -> None:
        self.assertTrue(settings.query_cache_db)
        self.assertGreater(settings.query_cache_lease_ms, 0)
        self.assertGreater(settings.query_cache_wait_ms, 0)


if __name__ == "__main__":
    unittest.main(verbosity=2)
