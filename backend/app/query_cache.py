"""查询缓存的 SQLite 共享后端。

设计目标（对应迁移需求）：

* 多个 FastAPI 进程共享同一份缓存事实：所有缓存条目落在一个 SQLite 文件里，
  开启 WAL，多个进程可同时读，写入串行化但不阻塞读。
* 读写都携带完整身份与绝对过期时点（``expires_at_epoch_ms``），过期时点在首次写入时
  固化，进程重启不会重新计算，因此不会因重启延长生存期。
* 同键并发填充只有一个有效写入者：``cache_entries`` 对 ``cache_key`` 建唯一索引，
  租约通过插入/更新唯一行获取（``ON CONFLICT``），配合 ``fence_token`` 单调递增，
  晚到的旧写入（fence 已落后）无法覆盖新结果。
* 租约持有者退出（崩溃/重启）后，租约到期时其它进程可以接管，接管会推进 fence，
  使旧持有者之后的任何提交都失效。
* 损坏条目（结果 JSON 无法解析、关键字段缺失）被隔离到 ``quarantined`` 状态，
  并允许同键重新计算；隔离只影响该键，不波及其它条目。
* 落库失败不抛出到业务层之外：统一转成 :class:`CacheBackendError`，由上层决定绕过缓存，
  保证查询结果照常返回。

本模块只用标准库（``sqlite3``、``json``、``time``、``uuid`` 等）。
"""
from __future__ import annotations

import json
import logging
import os
import sqlite3
import threading
import time
import uuid
from contextlib import contextmanager
from typing import Any, Iterator

from app.config import settings

logger = logging.getLogger("app.query_cache")

# 条目生命周期状态
STATE_READY = "ready"
STATE_FILLING = "filling"
STATE_QUARANTINED = "quarantined"

VALID_STATES = {STATE_READY, STATE_FILLING, STATE_QUARANTINED}

# 租约接管的宽限系数：超过 lease_ms * 该系数仍未提交，视为租用者已退出
LEASE_TAKEOVER_FACTOR = 1.0


class CacheBackendError(RuntimeError):
    """缓存后端不可用或落库失败；上层应绕过缓存而不是中断查询。"""


def now_ms() -> int:
    return int(time.time() * 1000)


def process_identity() -> str:
    """当前进程的稳定身份标识，写进每条租约/结果，便于跨进程排查。"""
    return f"{os.uname().nodename}:{os.getpid()}:{uuid.uuid4().hex[:8]}"


_SCHEMA = """
CREATE TABLE IF NOT EXISTS cache_strategies (
    strategy_id      INTEGER PRIMARY KEY,
    strategy_code    TEXT NOT NULL,
    target_query     TEXT NOT NULL,
    key_rule         TEXT NOT NULL,
    ttl_seconds      INTEGER,
    ttl_unknown      INTEGER NOT NULL DEFAULT 0,
    ttl_raw          TEXT,
    updated_at_ms    INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS cache_entries (
    cache_key            TEXT PRIMARY KEY,
    strategy_id          INTEGER,
    strategy_code        TEXT,
    target_query         TEXT,
    state                TEXT NOT NULL,
    identity_hash        TEXT NOT NULL,
    identity_payload     TEXT NOT NULL,
    result_payload       TEXT,
    created_at_ms        INTEGER NOT NULL,
    updated_at_ms        INTEGER NOT NULL,
    expires_at_epoch_ms  INTEGER,
    lease_owner          TEXT,
    lease_expires_ms     INTEGER,
    fence_token          INTEGER NOT NULL DEFAULT 1,
    corrupt_reason       TEXT
);

CREATE INDEX IF NOT EXISTS idx_cache_entries_state
    ON cache_entries(state);
CREATE INDEX IF NOT EXISTS idx_cache_entries_expires
    ON cache_entries(expires_at_epoch_ms);

CREATE TABLE IF NOT EXISTS cache_events (
    seq          INTEGER PRIMARY KEY AUTOINCREMENT,
    event_at_ms  INTEGER NOT NULL,
    cache_key    TEXT NOT NULL,
    event        TEXT NOT NULL,
    actor        TEXT,
    detail       TEXT
);
"""


class QueryCacheStore:
    """线程安全的 SQLite 缓存仓库；每个线程持有独立连接（SQLite 连接不能跨线程共享）。"""

    def __init__(self, db_path: str | None = None, lease_ms: int | None = None) -> None:
        self.db_path = db_path or settings.query_cache_db
        self.lease_ms = lease_ms or settings.query_cache_lease_ms
        self._local = threading.local()
        self._init_lock = threading.Lock()
        self._available = True
        self._ensure_parent_dir()
        self._init_schema()

    # ------------------------------------------------------------------ 基础连接
    def _ensure_parent_dir(self) -> None:
        parent = os.path.dirname(os.path.abspath(self.db_path))
        if parent:
            os.makedirs(parent, exist_ok=True)

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(
            self.db_path,
            timeout=10.0,
            isolation_level=None,  # 显式事务，自动提交关闭
            check_same_thread=False,
        )
        conn.row_factory = sqlite3.Row
        # WAL 让多进程并发读写互不长期阻塞；NORMAL 在 WAL 下安全且更快
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.execute("PRAGMA busy_timeout=10000")
        conn.execute("PRAGMA foreign_keys=ON")
        return conn

    def _conn(self) -> sqlite3.Connection:
        conn = getattr(self._local, "conn", None)
        if conn is None:
            conn = self._connect()
            self._local.conn = conn
        return conn

    def _init_schema(self) -> None:
        try:
            # executescript 会先隐式提交，不能放在显式事务里；DDL 均带 IF NOT EXISTS，可重复执行
            conn = self._conn()
            conn.execute("PRAGMA journal_mode=WAL")
            conn.executescript(_SCHEMA)
        except sqlite3.Error as exc:  # pragma: no cover - 极端环境（只读目录等）
            self._available = False
            raise CacheBackendError(f"缓存数据库初始化失败：{exc}") from exc

    @property
    def available(self) -> bool:
        return self._available

    @contextmanager
    def _tx(self) -> Iterator[sqlite3.Connection]:
        conn = self._conn()
        try:
            conn.execute("BEGIN IMMEDIATE")
            yield conn
            conn.execute("COMMIT")
        except Exception:
            try:
                conn.execute("ROLLBACK")
            except sqlite3.Error:
                pass
            raise

    # ------------------------------------------------------------------ 工具
    def _record_event(
        self,
        conn: sqlite3.Connection,
        cache_key: str,
        event: str,
        actor: str | None,
        detail: str | None = None,
    ) -> None:
        conn.execute(
            "INSERT INTO cache_events(event_at_ms, cache_key, event, actor, detail) "
            "VALUES (?, ?, ?, ?, ?)",
            (now_ms(), cache_key, event, actor, detail),
        )

    @staticmethod
    def _identity_hash(identity: dict[str, Any]) -> str:
        return str(identity.get("identity_hash") or "").strip()

    # ------------------------------------------------------------------ 策略同步
    def upsert_strategy(self, strategy: dict[str, Any]) -> None:
        """迁移/同步一条缓存策略。保留原策略编号；无法解析的过期值标记为待确认。"""
        ttl_seconds, ttl_unknown, ttl_raw = _parse_ttl(strategy.get("过期秒数"))
        try:
            with self._tx() as conn:
                conn.execute(
                    "INSERT INTO cache_strategies(strategy_id, strategy_code, target_query, "
                    "key_rule, ttl_seconds, ttl_unknown, ttl_raw, updated_at_ms) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?) "
                    "ON CONFLICT(strategy_id) DO UPDATE SET "
                    "strategy_code=excluded.strategy_code, target_query=excluded.target_query, "
                    "key_rule=excluded.key_rule, ttl_seconds=excluded.ttl_seconds, "
                    "ttl_unknown=excluded.ttl_unknown, ttl_raw=excluded.ttl_raw, "
                    "updated_at_ms=excluded.updated_at_ms",
                    (
                        int(strategy["id"]),
                        str(strategy.get("策略编号") or ""),
                        str(strategy.get("目标查询") or ""),
                        str(strategy.get("缓存键规则") or ""),
                        ttl_seconds,
                        1 if ttl_unknown else 0,
                        ttl_raw,
                        now_ms(),
                    ),
                )
        except sqlite3.Error as exc:
            raise CacheBackendError(f"缓存策略同步失败：{exc}") from exc

    def get_strategy(self, strategy_id: int) -> dict[str, Any] | None:
        try:
            cur = self._conn().execute(
                "SELECT * FROM cache_strategies WHERE strategy_id = ?", (strategy_id,)
            )
            row = cur.fetchone()
        except sqlite3.Error as exc:
            raise CacheBackendError(f"缓存策略读取失败：{exc}") from exc
        return dict(row) if row else None

    # ------------------------------------------------------------------ 读取
    def get_ready_entry(self, cache_key: str) -> dict[str, Any] | None:
        """读取一个未过期、可解析的就绪条目；损坏或过期均按未命中处理并隔离损坏项。"""
        try:
            row = self._fetch_row(cache_key)
        except sqlite3.Error as exc:
            raise CacheBackendError(f"缓存读取失败：{exc}") from exc
        if row is None:
            return None
        if row["state"] != STATE_READY:
            return None
        expires_at = row["expires_at_epoch_ms"]
        if expires_at is not None and expires_at <= now_ms():
            # 到期条目不删，留给填充流程原子替换；身份与过期时点保持原值直到被替换
            return None
        entry = self._decode_ready_row(row)
        if entry is None:
            self._quarantine(cache_key, "结果负载无法解析或缺少身份字段")
            return None
        return entry

    def _fetch_row(self, cache_key: str) -> sqlite3.Row | None:
        cur = self._conn().execute(
            "SELECT * FROM cache_entries WHERE cache_key = ?", (cache_key,)
        )
        return cur.fetchone()

    def _decode_ready_row(self, row: sqlite3.Row) -> dict[str, Any] | None:
        try:
            result = json.loads(row["result_payload"]) if row["result_payload"] else None
            identity = json.loads(row["identity_payload"])
        except (json.JSONDecodeError, TypeError):
            return None
        if not isinstance(identity, dict) or not self._identity_hash(identity):
            return None
        return {
            "cache_key": row["cache_key"],
            "state": STATE_READY,
            "identity": identity,
            "identity_hash": self._identity_hash(identity),
            "result": result,
            "strategy_id": row["strategy_id"],
            "strategy_code": row["strategy_code"],
            "target_query": row["target_query"],
            "created_at_ms": row["created_at_ms"],
            "updated_at_ms": row["updated_at_ms"],
            "expires_at_epoch_ms": row["expires_at_epoch_ms"],
            "fence_token": row["fence_token"],
        }

    def _quarantine(self, cache_key: str, reason: str) -> None:
        try:
            with self._tx() as conn:
                conn.execute(
                    "UPDATE cache_entries SET state = ?, corrupt_reason = ?, updated_at_ms = ? "
                    "WHERE cache_key = ?",
                    (STATE_QUARANTINED, reason, now_ms(), cache_key),
                )
                self._record_event(conn, cache_key, "quarantine", None, reason)
        except sqlite3.Error as exc:
            # 隔离失败不影响调用方的绕过路径，只记日志
            logger.warning("隔离损坏缓存条目失败 key=%s err=%s", cache_key, exc)

    # ------------------------------------------------------------------ 租约
    def acquire_lease(
        self,
        *,
        cache_key: str,
        identity: dict[str, Any],
        strategy: dict[str, Any],
        owner: str,
        ttl_seconds: int | None,
    ) -> dict[str, Any]:
        """尝试获取同键填充租约。

        返回 ``{"acquired": bool, "lease": {...} | None, "entry": {...} | None}``：

        * 已有就绪且未过期条目 → ``entry`` 带回缓存事实，不发租约；
        * 无条目 / 到期 / 已隔离 / 租约已失效 → 当前调用者拿到新租约（fence 递增）；
        * 其它进程持有有效租约 → ``acquired=False``，调用者应短暂等待或绕过。
        """
        identity_hash = self._identity_hash(identity)
        if not identity_hash:
            raise CacheBackendError("缓存身份缺少 identity_hash，拒绝写入")
        now = now_ms()
        try:
            with self._tx() as conn:
                row = conn.execute(
                    "SELECT * FROM cache_entries WHERE cache_key = ?", (cache_key,)
                ).fetchone()

                if row is not None and row["state"] == STATE_READY:
                    expires_at = row["expires_at_epoch_ms"]
                    if expires_at is None or expires_at > now:
                        decoded = self._decode_ready_row(row)
                        if decoded is not None:
                            return {"acquired": False, "lease": None, "entry": decoded}
                        # 就绪但负载损坏：落到下面的接管/重填分支并隔离
                        conn.execute(
                            "UPDATE cache_entries SET state = ?, corrupt_reason = ?, "
                            "updated_at_ms = ? WHERE cache_key = ?",
                            (STATE_QUARANTINED, "结果负载无法解析或缺少身份字段", now, cache_key),
                        )
                        self._record_event(conn, cache_key, "quarantine", owner, "acquire 时发现损坏")
                        row = conn.execute(
                            "SELECT * FROM cache_entries WHERE cache_key = ?", (cache_key,)
                        ).fetchone()

                if (
                    row is not None
                    and row["state"] == STATE_FILLING
                    and row["lease_expires_ms"] is not None
                    and row["lease_expires_ms"] > now
                    and row["lease_owner"] != owner
                ):
                    # 同键已有有效写入者，本进程不能成为第二个写入者
                    self._record_event(conn, cache_key, "lease_busy", owner, row["lease_owner"])
                    return {
                        "acquired": False,
                        "lease": {"fence_token": row["fence_token"], "lease_owner": row["lease_owner"]},
                        "entry": None,
                    }

                takeover = row is not None
                next_fence = (int(row["fence_token"]) if row is not None else 0) + 1
                lease_expires = now + self.lease_ms
                expires_at = (now + ttl_seconds * 1000) if ttl_seconds is not None else None
                identity_payload = json.dumps(identity, ensure_ascii=False, sort_keys=True)

                conn.execute(
                    "INSERT INTO cache_entries(cache_key, strategy_id, strategy_code, target_query, "
                    "state, identity_hash, identity_payload, result_payload, created_at_ms, "
                    "updated_at_ms, expires_at_epoch_ms, lease_owner, lease_expires_ms, fence_token, "
                    "corrupt_reason) "
                    "VALUES (:cache_key, :strategy_id, :strategy_code, :target_query, :state, "
                    ":identity_hash, :identity_payload, NULL, :created_at_ms, :updated_at_ms, "
                    ":expires_at_epoch_ms, :lease_owner, :lease_expires_ms, :fence_token, NULL) "
                    "ON CONFLICT(cache_key) DO UPDATE SET "
                    "state=:state, identity_hash=:identity_hash, identity_payload=:identity_payload, "
                    "result_payload=NULL, updated_at_ms=:updated_at_ms, "
                    "expires_at_epoch_ms=:expires_at_epoch_ms, lease_owner=:lease_owner, "
                    "lease_expires_ms=:lease_expires_ms, fence_token=:fence_token, "
                    "corrupt_reason=NULL",
                    {
                        "cache_key": cache_key,
                        "strategy_id": strategy.get("id"),
                        "strategy_code": strategy.get("策略编号"),
                        "target_query": strategy.get("目标查询"),
                        "state": STATE_FILLING,
                        "identity_hash": identity_hash,
                        "identity_payload": identity_payload,
                        "created_at_ms": now,
                        "updated_at_ms": now,
                        "expires_at_epoch_ms": expires_at,
                        "lease_owner": owner,
                        "lease_expires_ms": lease_expires,
                        "fence_token": next_fence,
                    },
                )
                self._record_event(
                    conn,
                    cache_key,
                    "lease_takeover" if takeover else "lease_acquired",
                    owner,
                    f"fence={next_fence}",
                )
                return {
                    "acquired": True,
                    "lease": {
                        "fence_token": next_fence,
                        "lease_owner": owner,
                        "lease_expires_ms": lease_expires,
                        "expires_at_epoch_ms": expires_at,
                    },
                    "entry": None,
                }
        except sqlite3.Error as exc:
            raise CacheBackendError(f"缓存租约获取失败：{exc}") from exc

    def commit_fill(
        self,
        *,
        cache_key: str,
        fence_token: int,
        owner: str,
        result: Any,
    ) -> dict[str, Any]:
        """提交填充结果。只有 fence 与当前行一致的租用者能写入；晚到的旧写入被拒绝。

        返回 ``{"committed": bool, "reason": str | None, "entry": dict | None}``。
        """
        now = now_ms()
        try:
            with self._tx() as conn:
                row = conn.execute(
                    "SELECT * FROM cache_entries WHERE cache_key = ?", (cache_key,)
                ).fetchone()
                if row is None:
                    return {"committed": False, "reason": "缓存条目不存在，租约已失效", "entry": None}
                if int(row["fence_token"]) != int(fence_token):
                    self._record_event(
                        conn, cache_key, "commit_rejected_stale_fence", owner,
                        f"held={fence_token} current={row['fence_token']}",
                    )
                    return {
                        "committed": False,
                        "reason": f"fence {fence_token} 已过期，当前为 {row['fence_token']}，旧写入被拒绝",
                        "entry": None,
                    }
                if row["lease_owner"] != owner:
                    self._record_event(conn, cache_key, "commit_rejected_owner", owner, row["lease_owner"])
                    return {"committed": False, "reason": "租约已被其它进程接管", "entry": None}

                result_payload = json.dumps(result, ensure_ascii=False, default=str)
                # 过期时点沿用租约获取时固化的值，绝不因提交时刻而后移
                conn.execute(
                    "UPDATE cache_entries SET state = ?, result_payload = ?, updated_at_ms = ?, "
                    "lease_owner = NULL, lease_expires_ms = NULL, corrupt_reason = NULL "
                    "WHERE cache_key = ? AND fence_token = ?",
                    (STATE_READY, result_payload, now, cache_key, fence_token),
                )
                self._record_event(conn, cache_key, "fill_committed", owner, f"fence={fence_token}")
                refreshed = conn.execute(
                    "SELECT * FROM cache_entries WHERE cache_key = ?", (cache_key,)
                ).fetchone()
                decoded = self._decode_ready_row(refreshed)
                return {"committed": True, "reason": None, "entry": decoded}
        except sqlite3.Error as exc:
            raise CacheBackendError(f"缓存填充提交失败：{exc}") from exc

    def release_lease(self, *, cache_key: str, fence_token: int, owner: str) -> None:
        """主动放弃租约（计算失败等）。仅当前 fence 的持有者可释放，避免误删新租用者。"""
        try:
            with self._tx() as conn:
                row = conn.execute(
                    "SELECT fence_token, lease_owner, state FROM cache_entries WHERE cache_key = ?",
                    (cache_key,),
                ).fetchone()
                if row is None:
                    return
                if int(row["fence_token"]) != int(fence_token) or row["lease_owner"] != owner:
                    return
                # 仍处于填充态才删除空壳，让后来者干净地重新申请；已是就绪态则不动
                if row["state"] == STATE_FILLING:
                    conn.execute("DELETE FROM cache_entries WHERE cache_key = ?", (cache_key,))
                    self._record_event(conn, cache_key, "lease_released", owner, f"fence={fence_token}")
        except sqlite3.Error as exc:
            logger.warning("释放缓存租约失败 key=%s err=%s", cache_key, exc)

    # ------------------------------------------------------------------ 运维查询
    def list_entries(self, *, state: str | None = None) -> list[dict[str, Any]]:
        sql = "SELECT * FROM cache_entries"
        params: tuple[Any, ...] = ()
        if state:
            sql += " WHERE state = ?"
            params = (state,)
        sql += " ORDER BY updated_at_ms DESC"
        try:
            rows = self._conn().execute(sql, params).fetchall()
        except sqlite3.Error as exc:
            raise CacheBackendError(f"缓存条目列表读取失败：{exc}") from exc
        items: list[dict[str, Any]] = []
        for row in rows:
            item = {
                "cache_key": row["cache_key"],
                "state": row["state"],
                "identity_hash": row["identity_hash"],
                "strategy_id": row["strategy_id"],
                "strategy_code": row["strategy_code"],
                "target_query": row["target_query"],
                "created_at_ms": row["created_at_ms"],
                "updated_at_ms": row["updated_at_ms"],
                "expires_at_epoch_ms": row["expires_at_epoch_ms"],
                "lease_owner": row["lease_owner"],
                "lease_expires_ms": row["lease_expires_ms"],
                "fence_token": row["fence_token"],
                "corrupt_reason": row["corrupt_reason"],
            }
            items.append(item)
        return items

    def stats(self) -> dict[str, Any]:
        try:
            rows = self._conn().execute(
                "SELECT state, COUNT(*) AS n FROM cache_entries GROUP BY state"
            ).fetchall()
        except sqlite3.Error as exc:
            raise CacheBackendError(f"缓存统计读取失败：{exc}") from exc
        counts = {STATE_READY: 0, STATE_FILLING: 0, STATE_QUARANTINED: 0}
        for row in rows:
            counts[row["state"]] = row["n"]
        now = now_ms()
        expired = self._conn().execute(
            "SELECT COUNT(*) FROM cache_entries WHERE state = ? AND expires_at_epoch_ms IS NOT NULL "
            "AND expires_at_epoch_ms <= ?",
            (STATE_READY, now),
        ).fetchone()[0]
        return {
            "db_path": os.path.abspath(self.db_path),
            "lease_ms": self.lease_ms,
            "total": sum(counts.values()),
            "ready": counts[STATE_READY],
            "filling": counts[STATE_FILLING],
            "quarantined": counts[STATE_QUARANTINED],
            "expired_ready": expired,
        }

    def delete_entry(self, cache_key: str) -> bool:
        try:
            with self._tx() as conn:
                cur = conn.execute("DELETE FROM cache_entries WHERE cache_key = ?", (cache_key,))
                deleted = cur.rowcount > 0
                if deleted:
                    self._record_event(conn, cache_key, "entry_deleted", None, None)
                return deleted
        except sqlite3.Error as exc:
            raise CacheBackendError(f"缓存条目删除失败：{exc}") from exc


def _parse_ttl(raw: Any) -> tuple[int | None, bool, str | None]:
    """解析策略里的过期秒数。

    返回 ``(ttl_seconds, unknown, raw_text)``：
    * 合法非负整数 → ``(n, False, 原文)``；
    * 空值、非数字（旧样例里的占位文本等）→ ``(None, True, 原文)``，标记为待确认，
      这类策略不会被当成可直接命中的真实缓存。
    """
    if raw is None:
        return None, True, None
    text = str(raw).strip()
    if not text:
        return None, True, text
    try:
        value = int(float(text))
    except ValueError:
        return None, True, text
    if value < 0:
        return None, True, text
    return value, False, text


# 对外语义化别名：其它模块需要解析策略过期值时使用
parse_ttl = _parse_ttl
