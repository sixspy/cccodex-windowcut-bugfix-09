"""关窗后物化（BUGFIX-09 同代去重，甲路径：二次插入拒绝）。

口径封存见 docs/BUGFIX-09-幽灵切断说明.md：
- 同窗同代 (window_id, generation) 仅允许一条 status='成功'；
- 重复物化：台账零增、409 拒绝、写 materialize_duplicate_reject 审计；
- 库级部分唯一索引兜底，IntegrityError 同事务回滚后补记重复审计；
- 两人同插争用：先提交 contention_log 争用条再抛 409，争用不随回滚丢失。
空桩预留：分区失败回滚 vs 保留成功分区。
"""
from __future__ import annotations

import sqlite3
from datetime import datetime

from fastapi import APIRouter, HTTPException

from auth import AuthHeader, require_user
from db import connect, write_audit
from locks import release, try_acquire
from models import MaterializeIn

router = APIRouter(tags=["materialize"])


@router.post("/api/windows/{wid}/materialize")
def materialize(
    wid: int,
    authorization: AuthHeader = None,
    body: MaterializeIn = MaterializeIn(),
):
    user = require_user(authorization)
    lock_key = f"materialize:{wid}"
    with connect() as conn:
        # 台账锁：后到者争用条先落库（独立提交），再拒绝，不写业务。
        if not try_acquire(conn, lock_key, user):
            conn.commit()
            raise HTTPException(409, "争用：后到只记争用，不写业务")
        try:
            w = conn.execute("SELECT * FROM windows WHERE id=?", (wid,)).fetchone()
            if not w:
                raise HTTPException(404, "窗口不存在")
            if w["status"] != "已关闭":
                raise HTTPException(409, "未关窗不能物化")
            generation = int(w["generation"])
            now = datetime.now().isoformat(timespec="seconds")
            rows = conn.execute(
                """SELECT COUNT(*) c, COALESCE(SUM(amount),0) s
                   FROM events WHERE window_id=? AND disposition='窗内'""",
                (wid,),
            ).fetchone()
            partition = body.partition_key or f"{w['name']}-g{generation}"

            # 甲路径查重：同代已有有效成功物化则拒绝再插，台账零增。
            existing = conn.execute(
                """SELECT id FROM materializations
                   WHERE window_id=? AND generation=? AND status='成功'
                   ORDER BY id ASC LIMIT 1""",
                (wid, generation),
            ).fetchone()
            if existing:
                write_audit(
                    conn,
                    user,
                    "materialize_duplicate_reject",
                    str(wid),
                    {
                        "window_id": wid,
                        "generation": generation,
                        "existing_id": existing["id"],
                        "partition": partition,
                        "reason": "幽灵偏了：同代已有成功物化，甲路径拒绝二次插入",
                        "path": "甲-二次插入拒绝",
                    },
                )
                conn.commit()
                raise HTTPException(409, "同代已存在成功物化：拒绝重复插入（台账零增）")

            if body.fail_simulate:
                write_audit(
                    conn,
                    user,
                    "materialize_fail_sim",
                    str(wid),
                    {"partition": partition, "generation": generation},
                )
                conn.commit()
                raise HTTPException(500, "模拟物化失败（夹具用）")

            try:
                # 分区键不再拼 -dup-时间戳；同代分区键稳定，唯一索引兜底。
                conn.execute(
                    """INSERT INTO materializations(window_id,status,row_count,total_amount,
                                                    partition_key,created_at,generation)
                       VALUES (?,?,?,?,?,?,?)""",
                    (wid, "成功", rows["c"], rows["s"], partition, now, generation),
                )
            except sqlite3.IntegrityError:
                # 咨询锁竞态兜底：同代成功行已存在，整事务回滚后只记重复审计。
                conn.rollback()
                keeper = conn.execute(
                    """SELECT id FROM materializations
                       WHERE window_id=? AND generation=? AND status='成功'
                       ORDER BY id ASC LIMIT 1""",
                    (wid, generation),
                ).fetchone()
                write_audit(
                    conn,
                    user,
                    "materialize_duplicate_reject",
                    str(wid),
                    {
                        "window_id": wid,
                        "generation": generation,
                        "existing_id": keeper["id"] if keeper else None,
                        "partition": partition,
                        "reason": "幽灵偏了：唯一索引拒绝同代二次成功插入",
                        "path": "甲-二次插入拒绝",
                    },
                )
                conn.commit()
                raise HTTPException(409, "同代已存在成功物化：拒绝重复插入（台账零增）")

            write_audit(
                conn,
                user,
                "materialize_ok",
                str(wid),
                {
                    "row_count": rows["c"],
                    "total_amount": rows["s"],
                    "partition": partition,
                    "generation": generation,
                },
            )
            conn.commit()
            return {
                "row_count": rows["c"],
                "total_amount": rows["s"],
                "partition_key": partition,
                "generation": generation,
            }
        finally:
            release(conn, lock_key, user)
            conn.commit()


@router.get("/api/materializations")
def list_mats(authorization: AuthHeader = None):
    require_user(authorization)
    with connect() as conn:
        rows = conn.execute(
            """SELECT m.*, w.name AS window_name
               FROM materializations m
               JOIN windows w ON w.id=m.window_id
               ORDER BY m.id DESC"""
        ).fetchall()
        return {"items": [dict(r) for r in rows]}


@router.post("/api/materializations/partition-stub")
def partition_stub(authorization: AuthHeader = None):
    """空桩模块：部分物化失败回滚策略待题面落地。"""
    user = require_user(authorization)
    with connect() as conn:
        write_audit(
            conn,
            user,
            "partition_mat_stub",
            None,
            {
                "paths": [
                    "整窗回滚",
                    "保留成功分区",
                ],
                "note": "正确性口径须择一，母仓不实现",
            },
        )
        conn.commit()
    return {"ok": True, "stub": True, "module": "partition_mat"}
