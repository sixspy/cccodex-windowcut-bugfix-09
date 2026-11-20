"""BUGFIX-09 同代物化去重 —— 幽灵金额源切断。

红灯先行（两红）：
1. test_double_click_same_generation_leaves_single_success
   同一关闭窗连点两次物化：修复前台账出现两条「成功」幽灵行。
2. test_concurrent_same_generation_lock_contention_is_recorded
   两人同插同代物化，后到只记争用：修复前争用条随 409 事务回滚而丢失。

五处同态（同一不变量在五个面上各断言一次）：
台账列表 / 作废标 / 重复审计 / 对账取值 / 争用条。

夹具全部本地、SQLite 临时库；登录与 data/app.db 均不改动。
"""
from __future__ import annotations

import json
import sqlite3

import pytest
from fastapi.testclient import TestClient

from app import app
import db


@pytest.fixture(autouse=True)
def _db(tmp_path, monkeypatch):
    monkeypatch.setattr("db.DB_PATH", tmp_path / "bugfix09.db")
    db.init_db()
    yield


def _client() -> TestClient:
    return TestClient(app)


def _auth(c: TestClient, username: str = "admin", password: str = "123456") -> dict:
    tok = c.post(
        "/api/login", json={"username": username, "password": password}
    ).json()["token"]
    return {"Authorization": "Bearer " + tok}


def _plant_ghost_success(conn: sqlite3.Connection, window_id: int, generation: int,
                         row_count: int, total_amount: float, partition: str) -> int:
    """直接种入一条同代「成功」幽灵行，模拟历史连点遗留。

    现行库有部分唯一索引守护，正常插入路径不可能再造幽灵；因此夹具临时
    摘下索引、按旧世界方式写入脏数据，保持「迁移前脏库」状态，交给
    init_db() 的幽灵切断迁移或对账逻辑证明切断（迁移会负责重建索引）。
    """
    conn.execute("DROP INDEX ux_materializations_success_per_generation")
    cur = conn.execute(
        """INSERT INTO materializations(window_id,status,row_count,total_amount,
                                        partition_key,created_at,generation)
           VALUES (?,?,?,?,?,?,?)""",
        (window_id, "成功", row_count, total_amount, partition,
         "2026-11-01T09:00:00", generation),
    )
    conn.commit()
    return cur.lastrowid


def _success_rows(c: TestClient, h: dict, wid: int) -> list[dict]:
    items = c.get("/api/materializations", headers=h).json()["items"]
    return [m for m in items if m["window_id"] == wid and m["status"] == "成功"]


def _audit_actions(c: TestClient, h: dict) -> list[dict]:
    return c.get("/api/audit/actions", headers=h).json()["items"]


# ---------------------------------------------------------------- 红灯 1
def test_double_click_same_generation_leaves_single_success():
    """连点复现：同窗同代有效物化只能留一条（甲路径：二次插入被拒绝）。"""
    c = _client()
    h = _auth(c)
    assert c.post("/api/windows/2/close", headers=h).status_code == 200

    first = c.post("/api/windows/2/materialize", headers=h)
    assert first.status_code == 200, first.text
    second = c.post("/api/windows/2/materialize", headers=h)

    # 甲路径：已有成功物化则拒绝再插，台账零增
    assert second.status_code == 409, second.text

    # 同态面 1：台账列表只有一条有效成功行
    ok_rows = _success_rows(c, h, 2)
    assert len(ok_rows) == 1
    with db.connect() as conn:
        n = conn.execute(
            "SELECT COUNT(*) c FROM materializations WHERE window_id=2 AND status='成功'"
        ).fetchone()["c"]
        assert n == 1

    # 同态面 3：重复审计已落账（不是静默拒绝）
    trail = c.get("/api/audit?limit=500", headers=h).json()["items"]
    dup = [a for a in trail if a["action"] == "materialize_duplicate_reject"]
    assert len(dup) == 1
    payload = json.loads(dup[0]["payload"])
    assert payload["window_id"] == 2
    assert payload["generation"] == 1
    actions = {a["action"]: a["c"] for a in _audit_actions(c, h)}
    assert actions.get("materialize_duplicate_reject") == 1
    # 成功审计仍只有一次
    assert actions.get("materialize_ok") == 1


def test_ghost_rows_never_enter_reconcile_and_are_cut_off():
    """同态面 4：幽灵旧成功行不得进入对账取值；幽灵须被切断。"""
    c = _client()
    h = _auth(c)
    # 窗 1 种子已带一条成功物化（1 笔 / 12.5）。再种一条同代幽灵，金额是毒值。
    with db.connect() as conn:
        ghost_id = _plant_ghost_success(
            conn, window_id=1, generation=1, row_count=99,
            total_amount=9999.99, partition="W-1101-g1-ghost-dup",
        )

    # 一次性幽灵切断：保留最早真行，幽灵标作废并审计留痕
    db.init_db()
    with db.connect() as conn:
        assert conn.execute(
            "SELECT status FROM materializations WHERE id=?", (ghost_id,)
        ).fetchone()["status"] == "作废"
        cutoff = conn.execute(
            "SELECT COUNT(*) c FROM audit_trail WHERE action='materialize_ghost_cutoff'"
        ).fetchone()["c"]
        assert cutoff == 1

    # 对该窗再次物化必须被拒绝，毒值行不能成为新的有效来源
    rejected = c.post("/api/windows/1/materialize", headers=h)
    assert rejected.status_code == 409
    assert len(_success_rows(c, h, 1)) == 1

    recon = c.get("/api/reconcile/1", headers=h).json()
    # 对账取值只认真行（1 笔 / 12.5），幽灵 99 笔/9999.99 不得进入
    assert recon["window_events"] == 1
    assert recon["window_amount"] == 12.5
    assert recon["materialized"]["row_count"] == 1
    assert recon["materialized"]["total_amount"] == 12.5
    assert recon["aligned"] is True
    assert recon["materialized"]["id"] != ghost_id

    all_recon = c.get("/api/reconcile", headers=h).json()["items"]
    w1 = next(r for r in all_recon if r["window_id"] == 1)
    assert w1["aligned"] is True


def test_legacy_ghost_rows_are_marked_void_not_deleted_silently():
    """同态面 2：历史幽灵行打作废标，不物理删除，且作废行不进对账。"""
    c = _client()
    h = _auth(c)
    with db.connect() as conn:
        ghost_id = _plant_ghost_success(
            conn, window_id=1, generation=1, row_count=99,
            total_amount=9999.99, partition="W-1101-g1-ghost-dup",
        )
    # 重新初始化触发一次性幽灵切断（同一 SQLite 文件，不换仓）
    db.init_db()

    with db.connect() as conn:
        ghost = conn.execute(
            "SELECT * FROM materializations WHERE id=?", (ghost_id,)
        ).fetchone()
        assert ghost is not None  # 行还在
        assert ghost["status"] == "作废"  # 作废标
        valid = conn.execute(
            "SELECT COUNT(*) c FROM materializations WHERE window_id=1 AND status='成功'"
        ).fetchone()["c"]
        assert valid == 1

    items = c.get("/api/materializations", headers=h).json()["items"]
    listed = next(m for m in items if m["id"] == ghost_id)
    assert listed["status"] == "作废"
    recon = c.get("/api/reconcile/1", headers=h).json()
    assert recon["aligned"] is True
    assert recon["materialized"]["id"] != ghost_id


# ---------------------------------------------------------------- 红灯 2
def test_concurrent_same_generation_lock_contention_is_recorded():
    """两人同插同代物化：台账锁后到只记争用，争用条必须落库。"""
    c = _client()
    h_admin = _auth(c, "admin", "123456")
    h_closer = _auth(c, "closer", "close123")
    assert c.post("/api/windows/2/close", headers=h_admin).status_code == 200

    # closer 先占住物化台账锁
    got = c.post(
        "/api/locks/acquire", headers=h_closer,
        json={"lock_key": "materialize:2"},
    )
    assert got.status_code == 200

    # admin 后到同代物化：409 且不写业务
    blocked = c.post("/api/windows/2/materialize", headers=h_admin)
    assert blocked.status_code == 409
    with db.connect() as conn:
        # 同态面 5：争用条落库（修复前随回滚丢失）
        rows = conn.execute(
            "SELECT * FROM contention_log WHERE lock_key='materialize:2'"
        ).fetchall()
        assert len(rows) == 1
        assert rows[0]["actor"] == "admin"
        assert rows[0]["action"] == "争用"
        # 台账零增
        n = conn.execute(
            "SELECT COUNT(*) c FROM materializations WHERE window_id=2"
        ).fetchone()["c"]
        assert n == 0
        # 无成功/重复审计混写
        audits = conn.execute(
            "SELECT action FROM audit_trail WHERE action IN ('materialize_ok','materialize_duplicate_reject')"
        ).fetchall()
        assert audits == []

    # 争用在争用接口可见
    items = c.get("/api/contention", headers=h_admin).json()["items"]
    assert any(x["lock_key"] == "materialize:2" and x["actor"] == "admin" for x in items)

    # 锁释放后，首个物化成功且仅一条
    assert c.post(
        "/api/locks/release", headers=h_closer, json={"lock_key": "materialize:2"}
    ).status_code == 200
    ok = c.post("/api/windows/2/materialize", headers=h_admin)
    assert ok.status_code == 200
    assert len(_success_rows(c, h_admin, 2)) == 1


def test_unique_database_guard_rejects_second_success_under_same_generation():
    """库级二次插入拒绝（甲路径的硬保证）：同窗同代只允许一条成功。"""
    with db.connect() as conn:
        # 窗 3 无任何物化行，避开种子数据
        now = "2026-11-03T00:10:00"
        conn.execute(
            """INSERT INTO materializations(window_id,status,row_count,total_amount,
                                            partition_key,created_at,generation)
               VALUES (3,'成功',1,12.5,'W-1103-g1',?,1)""",
            (now,),
        )
        conn.commit()
        with pytest.raises(sqlite3.IntegrityError):
            conn.execute(
                """INSERT INTO materializations(window_id,status,row_count,total_amount,
                                                partition_key,created_at,generation)
                   VALUES (3,'成功',2,20.0,'W-1103-g1-dup',?,1)""",
                (now,),
            )
            conn.commit()
        conn.rollback()
    # 作废行不受唯一约束限制（历史切断可标废多行）
    with db.connect() as conn:
        conn.execute(
            """INSERT INTO materializations(window_id,status,row_count,total_amount,
                                            partition_key,created_at,generation)
               VALUES (3,'作废',99,9999.99,'W-1103-g1-ghost',?,1)""",
            ("2026-11-03T00:11:00",),
        )
        conn.commit()
    with db.connect() as conn:
        n_ok = conn.execute(
            "SELECT COUNT(*) c FROM materializations WHERE window_id=3 AND status='成功'"
        ).fetchone()["c"]
        n_void = conn.execute(
            "SELECT COUNT(*) c FROM materializations WHERE window_id=3 AND status='作废'"
        ).fetchone()["c"]
        assert n_ok == 1
        assert n_void == 1


def test_dashboard_counts_only_effective_success():
    """统计同态：物化单计数只算有效成功，幽灵/作废不充数。"""
    c = _client()
    h = _auth(c)
    with db.connect() as conn:
        _plant_ghost_success(
            conn, 1, 1, 99, 9999.99, "W-1101-g1-ghost-dup",
        )
    db.init_db()  # 触发幽灵切断
    dash = c.get("/api/dashboard", headers=h).json()
    # 种子有效行 1 条；幽灵被作废，不计入
    assert dash["materializations"] == 1


class _NoneResult:
    def fetchone(self):
        return None


class _IntegrityOnInsert:
    """连接替身：锁表读为空、INSERT 必抛 IntegrityError，模拟提交交错竞态。"""

    def __init__(self, real_conn):
        self._real = real_conn

class _IntegrityOnInsert:
    """连接替身：锁表读为空、锁键 INSERT 必抛 IntegrityError，模拟提交交错竞态。"""

    def __init__(self, real_conn):
        self._real = real_conn
        self._select_calls = 0

    def execute(self, sql, params=()):
        compact = " ".join(sql.split())
        if compact.startswith("INSERT INTO window_locks"):
            raise sqlite3.IntegrityError("UNIQUE constraint failed: window_locks.lock_key")
        # 首次锁表查询返回空（过期视图）；回退后的重新查询走真连接读到真持有者
        if compact.startswith("SELECT holder FROM window_locks") and self._select_calls == 0:
            self._select_calls += 1
            return _NoneResult()
        return self._real.execute(sql, params)

    def rollback(self):
        self._real.rollback()

    def commit(self):
        self._real.commit()

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


def test_try_acquire_insert_race_is_treated_as_contention():
    """锁键 INSERT 撞主键时：try_acquire 自行回退为争用并落条，不向外抛 IntegrityError。"""
    import locks

    with db.connect() as conn:
        # 真表里先放一个持有者，供回退后的重新查询读到
        conn.execute(
            "INSERT INTO window_locks(lock_key,holder,acquired_at) VALUES (?,?,?)",
            ("materialize:9", "admin", "2026-11-21T09:00:00"),
        )
        conn.commit()

        fake = _IntegrityOnInsert(conn)
        assert locks.try_acquire(fake, "materialize:9", "closer") is False
        rows = conn.execute(
            "SELECT * FROM contention_log WHERE lock_key='materialize:9'"
        ).fetchall()
        assert len(rows) == 1
        assert rows[0]["actor"] == "closer"
        assert rows[0]["action"] == "争用"
        assert "admin" in rows[0]["detail"]
        # 真表锁未被动，仍归 admin
        assert conn.execute(
            "SELECT holder FROM window_locks WHERE lock_key='materialize:9'"
        ).fetchone()["holder"] == "admin"


def test_real_thread_race_two_workers_one_success_one_contention():
    """两个真实连接、两个身份同时同插同代物化：一单成功，后到记争用，台账仅一条。"""
    import threading

    c = _client()
    h = _auth(c)
    assert c.post("/api/windows/2/close", headers=h).status_code == 200
    # 两个不同关账员（同用户会被锁面判为持锁者重入，不是争用）
    h_admin = _auth(c, "admin", "123456")
    h_closer = _auth(c, "closer", "close123")

    results: list[int] = []
    errors: list[Exception] = []
    barrier = threading.Barrier(2)

    def worker(headers):
        try:
            local = _client()
            barrier.wait()
            r = local.post("/api/windows/2/materialize", headers=headers)
            results.append(r.status_code)
        except Exception as exc:  # noqa: BLE001
            errors.append(exc)

    t1 = threading.Thread(target=worker, args=(h_admin,))
    t2 = threading.Thread(target=worker, args=(h_closer,))
    t1.start()
    t2.start()
    t1.join(timeout=40)
    t2.join(timeout=40)
    assert errors == []
    assert sorted(results) == [200, 409]

    assert len(_success_rows(c, h, 2)) == 1
    with db.connect() as conn:
        # 后到者必被挡：要么撞台账锁记争用，要么锁释放后撞同代查重拒绝；
        # 两条 409 路径都保证台账零增，二者恰有其一落痕。
        contention = conn.execute(
            "SELECT COUNT(*) c FROM contention_log WHERE lock_key='materialize:2'"
        ).fetchone()["c"]
        dup_reject = conn.execute(
            "SELECT COUNT(*) c FROM audit_trail WHERE action='materialize_duplicate_reject'"
        ).fetchone()["c"]
        assert contention + dup_reject == 1
        ok_audit = conn.execute(
            "SELECT COUNT(*) c FROM audit_trail WHERE action='materialize_ok'"
        ).fetchone()["c"]
        assert ok_audit == 1
        # 锁已释放，无残留持锁
        left = conn.execute(
            "SELECT COUNT(*) c FROM window_locks WHERE lock_key='materialize:2'"
        ).fetchone()["c"]
        assert left == 0
