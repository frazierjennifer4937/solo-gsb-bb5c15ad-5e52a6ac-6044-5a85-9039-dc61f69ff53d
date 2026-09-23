"""核心业务逻辑：批次导入、跨批次合并、缺帧区间计算、补扫任务认领。"""
from __future__ import annotations

import hashlib
import json
import sqlite3
import uuid
from datetime import datetime, timedelta, timezone

from . import schemas


# ---------- 纯函数（便于测试） ----------

def content_hash(roll_no: str, start: int, end: int, frames: list[int]) -> str:
    """同一胶卷下相同（预期范围 + 帧号集合）的内容指纹，用于幂等判定。"""
    payload = json.dumps(
        {"roll_no": roll_no, "start": start, "end": end, "frames": sorted(set(frames))},
        separators=(",", ":"),
        ensure_ascii=False,
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def merge_intervals(intervals: list[tuple[int, int]]) -> list[tuple[int, int]]:
    """合并重叠或相邻的闭区间，例如 [(1,5),(5,8)] -> [(1,8)]。"""
    if not intervals:
        return []
    merged: list[tuple[int, int]] = []
    for s, e in sorted(intervals):
        if not merged or s > merged[-1][1] + 1:
            merged.append((s, e))
        else:
            prev_s, prev_e = merged[-1]
            merged[-1] = (prev_s, max(prev_e, e))
    return merged


def missing_intervals(
    expected: list[tuple[int, int]], frames: set[int]
) -> list[tuple[int, int]]:
    """返回合并后的预期范围内、未被扫描帧覆盖的连续缺帧区间。"""
    result: list[tuple[int, int]] = []
    for s, e in merge_intervals(expected):
        gap_start: int | None = None
        for n in range(s, e + 1):
            if n in frames:
                if gap_start is not None:
                    result.append((gap_start, n - 1))
                    gap_start = None
            elif gap_start is None:
                gap_start = n
        if gap_start is not None:
            result.append((gap_start, e))
    return result


def out_of_range_frames(
    expected: list[tuple[int, int]], frames: set[int]
) -> list[int]:
    expected = merge_intervals(expected)
    return sorted(
        f for f in frames if not any(s <= f <= e for s, e in expected)
    )


def subtract_intervals(
    base: list[tuple[int, int]], cuts: list[tuple[int, int]]
) -> list[tuple[int, int]]:
    """从一组已合并闭区间 base 中扣除 cuts 覆盖的部分，返回合并后的剩余闭区间。"""
    result: list[tuple[int, int]] = []
    for s, e in merge_intervals(base):
        segments = [(s, e)]
        for cs, ce in merge_intervals(cuts):
            next_segments: list[tuple[int, int]] = []
            for seg_s, seg_e in segments:
                if ce < seg_s or cs > seg_e:
                    next_segments.append((seg_s, seg_e))
                else:
                    if cs > seg_s:
                        next_segments.append((seg_s, min(seg_e, cs - 1)))
                    if ce < seg_e:
                        next_segments.append((max(seg_s, ce + 1), seg_e))
            segments = next_segments
        result.extend(segments)
    return merge_intervals(result)


def intersect_intervals(
    a: list[tuple[int, int]], b: list[tuple[int, int]]
) -> list[tuple[int, int]]:
    """两组已合并闭区间的交集（合并后的连续闭区间）。"""
    result: list[tuple[int, int]] = []
    for as_, ae in merge_intervals(a):
        for bs, be in merge_intervals(b):
            s, e = max(as_, bs), min(ae, be)
            if s <= e:
                result.append((s, e))
    return merge_intervals(result)


# ---------- 数据访问 ----------

def _expected_ranges(conn: sqlite3.Connection, roll_no: str) -> list[tuple[int, int]]:
    rows = conn.execute(
        "SELECT start_no, end_no FROM roll_expected WHERE roll_no = ?",
        (roll_no,),
    ).fetchall()
    return merge_intervals([(r["start_no"], r["end_no"]) for r in rows])


def _all_frames(conn: sqlite3.Connection, roll_no: str) -> set[int]:
    rows = conn.execute(
        "SELECT frame_no FROM roll_frames WHERE roll_no = ?", (roll_no,)
    ).fetchall()
    return {r["frame_no"] for r in rows}


def _batch_metas(conn: sqlite3.Connection, roll_no: str) -> list[schemas.BatchMeta]:
    rows = conn.execute(
        """SELECT id, expected_start, expected_end, frames_json, raw_frames_json,
                  frames_merged_json, duplicates_json, created_at
             FROM batches WHERE roll_no = ? ORDER BY created_at, rowid""",
        (roll_no,),
    ).fetchall()
    return [
        schemas.BatchMeta(
            batch_id=r["id"],
            expected_start=r["expected_start"],
            expected_end=r["expected_end"],
            # 新批次保留原始帧列表（含批内重复、保序）；旧库迁移来的批次退回去重集合
            frames=json.loads(r["raw_frames_json"] or r["frames_json"]),
            added_frames=json.loads(r["frames_merged_json"]),
            duplicates=json.loads(r["duplicates_json"]),
            created_at=r["created_at"],
        )
        for r in rows
    ]


def _state_from_db(conn: sqlite3.Connection, roll_no: str) -> dict:
    expected = _expected_ranges(conn, roll_no)
    frames = _all_frames(conn, roll_no)
    return {
        "expected": expected,
        "frames": frames,
        "missing": missing_intervals(expected, frames),
        "out_of_range": out_of_range_frames(expected, frames),
    }


def get_roll_state(conn: sqlite3.Connection, roll_no: str) -> schemas.RollState | None:
    if not conn.execute(
        "SELECT 1 FROM batches WHERE roll_no = ? LIMIT 1", (roll_no,)
    ).fetchone():
        return None
    st = _state_from_db(conn, roll_no)
    metas = _batch_metas(conn, roll_no)
    frames_sorted = sorted(st["frames"])
    return schemas.RollState(
        roll_no=roll_no,
        expected_ranges=[list(x) for x in st["expected"]],
        frames=frames_sorted,
        missing_ranges=[list(x) for x in st["missing"]],
        out_of_range_frames=st["out_of_range"],
        total_frames=len(frames_sorted),
        batch_count=len(metas),
        batches=metas,
    )


def list_rolls(conn: sqlite3.Connection) -> list[schemas.RollSummary]:
    rows = conn.execute(
        """SELECT roll_no,
                  (SELECT COUNT(*) FROM roll_frames f WHERE f.roll_no = b.roll_no) AS total,
                  COUNT(*) AS batch_count
             FROM batches b GROUP BY roll_no ORDER BY roll_no"""
    ).fetchall()
    out: list[schemas.RollSummary] = []
    for r in rows:
        st = _state_from_db(conn, r["roll_no"])
        out.append(
            schemas.RollSummary(
                roll_no=r["roll_no"],
                total_frames=r["total"],
                batch_count=r["batch_count"],
                missing_ranges=[list(x) for x in st["missing"]],
            )
        )
    return out


def get_batch(conn: sqlite3.Connection, batch_id: str) -> schemas.BatchResult | None:
    r = conn.execute("SELECT * FROM batches WHERE id = ?", (batch_id,)).fetchone()
    if r is None:
        return None
    st = _state_from_db(conn, r["roll_no"])
    return schemas.BatchResult(
        batch_id=r["id"],
        roll_no=r["roll_no"],
        repeated=True,
        added_frames=json.loads(r["frames_merged_json"]),
        duplicates=json.loads(r["duplicates_json"]),
        merged_frames=sorted(st["frames"]),
        expected_ranges=[list(x) for x in st["expected"]],
        missing_ranges=[list(x) for x in st["missing"]],
        out_of_range_frames=st["out_of_range"],
        created_at=r["created_at"],
    )


def submit_batch(conn: sqlite3.Connection, data: schemas.BatchIn) -> tuple[schemas.BatchResult, int]:
    """导入一个批次。返回 (结果, HTTP 状态码 201新建 / 200重复)。

    幂等保证：(roll_no, 内容指纹) 唯一。相同内容重复导入不写任何数据。
    """
    frames = sorted(set(data.frames))
    raw_frames = list(data.frames)  # 保留原始帧列表（含批内重复、保序），供撤销时重建
    digest = content_hash(
        data.roll_no, data.expected_start, data.expected_end, frames
    )

    conn.execute("BEGIN IMMEDIATE")
    try:
        existing = conn.execute(
            "SELECT id FROM batches WHERE roll_no = ? AND content_hash = ?",
            (data.roll_no, digest),
        ).fetchone()
        if existing is not None:
            conn.execute("ROLLBACK")
            result = get_batch(conn, existing["id"])
            assert result is not None
            return result, 200

        # 客户端自带 batch_id 若已被不同内容占用则拒绝（409 在 main 中处理）
        if data.batch_id is not None:
            clash = conn.execute(
                "SELECT roll_no, content_hash FROM batches WHERE id = ?",
                (data.batch_id,),
            ).fetchone()
            if clash is not None and (
                clash["roll_no"] != data.roll_no or clash["content_hash"] != digest
            ):
                raise BatchIdConflictError(data.batch_id)

        batch_id = data.batch_id or f"auto-{uuid.uuid4().hex}"

        existing_frames = _all_frames(conn, data.roll_no)

        within_submit: set[int] = set()
        dup_seen: set[int] = set()
        for f in data.frames:  # 未去重的原始列表，检出批内重复
            if f in dup_seen:
                within_submit.add(f)
            dup_seen.add(f)
        # 批内重复 + 该胶卷此前批次已扫到的帧
        duplicates = sorted(within_submit | (set(frames) & existing_frames))
        new_frames = sorted(set(frames) - existing_frames)

        conn.execute(
            """INSERT INTO batches
               (id, roll_no, expected_start, expected_end, frames_json,
                raw_frames_json, frames_merged_json, duplicates_json, content_hash)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                batch_id,
                data.roll_no,
                data.expected_start,
                data.expected_end,
                json.dumps(frames),
                json.dumps(raw_frames),
                json.dumps(new_frames),
                json.dumps(duplicates),
                digest,
            ),
        )
        conn.execute(
            "INSERT OR IGNORE INTO roll_expected (roll_no, start_no, end_no) VALUES (?, ?, ?)",
            (data.roll_no, data.expected_start, data.expected_end),
        )
        conn.executemany(
            "INSERT OR IGNORE INTO roll_frames (roll_no, frame_no, batch_id) VALUES (?, ?, ?)",
            [(data.roll_no, f, batch_id) for f in new_frames],
        )
        conn.execute("COMMIT")
    except BatchIdConflictError:
        conn.execute("ROLLBACK")
        raise
    except Exception:
        conn.execute("ROLLBACK")
        raise

    st = _state_from_db(conn, data.roll_no)
    result = schemas.BatchResult(
        batch_id=batch_id,
        roll_no=data.roll_no,
        repeated=False,
        added_frames=new_frames,
        duplicates=duplicates,
        merged_frames=sorted(st["frames"]),
        expected_ranges=[list(x) for x in st["expected"]],
        missing_ranges=[list(x) for x in st["missing"]],
        out_of_range_frames=st["out_of_range"],
    )
    return result, 201


class BatchIdConflictError(Exception):
    def __init__(self, batch_id: str):
        self.batch_id = batch_id
        super().__init__(f"batch_id 已被其他内容占用: {batch_id}")


# ---------- 批次撤销 ----------

class BatchNotFoundError(Exception):
    def __init__(self, batch_id: str):
        self.batch_id = batch_id
        super().__init__(f"批次不存在: {batch_id}")


class LegacyBatchDataError(Exception):
    """旧版库遗留批次缺少原始帧列表，撤销后无法区分批内与跨批重复。"""

    def __init__(self, legacy_batch_ids: list[str]):
        self.legacy_batch_ids = legacy_batch_ids
        super().__init__(
            f"存在无法区分批内/跨批重复的旧批次: {', '.join(legacy_batch_ids)}"
        )


def _within_batch_duplicates(raw_frames: list[int]) -> set[int]:
    """从原始帧列表中检出批内重复出现的帧号。"""
    seen: set[int] = set()
    dups: set[int] = set()
    for f in raw_frames:
        if f in seen:
            dups.add(f)
        seen.add(f)
    return dups


def delete_batch(conn: sqlite3.Connection, batch_id: str) -> None:
    """撤销一个误导入批次，全程在同一 SQLite 写事务内完成（失败整体回滚）。

    - 批次不存在：抛 BatchNotFoundError（接口返回 404），不触碰数据。
    - 删除目标批次后，按剩余批次的**原导入顺序**重建：
      预期范围集合（roll_expected）、帧归属（roll_frames，共享帧归最早扫到它
      的剩余批次，故共享帧不会随首个批次丢失）、各批新增帧与重复帧明细
      （批内重复保留；跨批重复按重建后的前缀集合重算）。
    - 若剩余批次中存在旧版数据（raw_frames_json 为 NULL，无法区分批内与
      跨批重复），抛 LegacyBatchDataError（接口返回 409）且不改变任何数据。
    - 最后一批撤销后胶卷视为不存在：清空其预期范围与帧，并把该胶卷全部
      未释放的历史认领标记为已释放，使其不阻碍同编号胶卷重新导入后的领取。
    """
    conn.execute("BEGIN IMMEDIATE")
    try:
        target = conn.execute(
            "SELECT id, roll_no FROM batches WHERE id = ?", (batch_id,)
        ).fetchone()
        if target is None:
            raise BatchNotFoundError(batch_id)
        roll_no = target["roll_no"]

        remaining = conn.execute(
            """SELECT id, expected_start, expected_end, raw_frames_json
                 FROM batches
                WHERE roll_no = ? AND id != ?
                ORDER BY created_at, rowid""",
            (roll_no, batch_id),
        ).fetchall()

        # 旧批次只有去重后的帧集合，无法重算“批内重复 + 跨批重复”明细
        legacy = [r["id"] for r in remaining if r["raw_frames_json"] is None]
        if legacy:
            raise LegacyBatchDataError(legacy)

        # 先清空帧归属再删批次，避免外键约束；随后按剩余批次整体重建
        conn.execute(
            "DELETE FROM roll_frames WHERE roll_no = ?", (roll_no,)
        )
        conn.execute("DELETE FROM batches WHERE id = ?", (batch_id,))
        conn.execute(
            "DELETE FROM roll_expected WHERE roll_no = ?", (roll_no,)
        )

        if not remaining:
            # 胶卷下再无批次：胶卷视为不存在。历史认领即时释放，
            # 同编号胶卷重新导入后不会被旧认领占住缺帧。
            conn.execute(
                """UPDATE rescan_claims
                      SET released_at = ?
                    WHERE roll_no = ? AND released_at IS NULL""",
                (_now_iso(), roll_no),
            )
            conn.execute("COMMIT")
            return

        conn.executemany(
            "INSERT OR IGNORE INTO roll_expected (roll_no, start_no, end_no) VALUES (?, ?, ?)",
            [(roll_no, r["expected_start"], r["expected_end"]) for r in remaining],
        )

        owned: set[int] = set()  # 按原导入顺序，此前批次已扫到的帧
        for r in remaining:
            raw_frames = json.loads(r["raw_frames_json"])
            frame_set = set(raw_frames)
            within_dup = _within_batch_duplicates(raw_frames)  # 批内重复保留
            new_frames = sorted(frame_set - owned)
            duplicates = sorted(within_dup | (frame_set & owned))

            conn.executemany(
                """INSERT OR IGNORE INTO roll_frames
                       (roll_no, frame_no, batch_id) VALUES (?, ?, ?)""",
                [(roll_no, f, r["id"]) for f in new_frames],
            )
            conn.execute(
                """UPDATE batches
                      SET frames_merged_json = ?, duplicates_json = ?
                    WHERE id = ?""",
                (json.dumps(new_frames), json.dumps(duplicates), r["id"]),
            )
            owned |= frame_set

        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise


# ---------- 补扫任务认领 ----------

class RollNotFoundError(Exception):
    pass


class NoClaimableFramesError(Exception):
    pass


def _now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def _roll_exists(conn: sqlite3.Connection, roll_no: str) -> bool:
    return (
        conn.execute(
            "SELECT 1 FROM batches WHERE roll_no = ? LIMIT 1", (roll_no,)
        ).fetchone()
        is not None
    )


_ACTIVE_CLAIMS_SQL = """
    SELECT id, roll_no, claimant, frame_start, frame_end, max_frames,
           ttl_seconds, created_at, expires_at, released_at
      FROM rescan_claims
     WHERE roll_no = ?
       AND released_at IS NULL
       AND expires_at > ?
     ORDER BY frame_start, id
"""


def _active_claim_rows(conn: sqlite3.Connection, roll_no: str) -> list[sqlite3.Row]:
    return conn.execute(_ACTIVE_CLAIMS_SQL, (roll_no, _now_iso())).fetchall()


def _claim_out(conn: sqlite3.Connection, r: sqlite3.Row) -> schemas.RescanClaimOut:
    """组装响应：认领帧段与当前缺帧区间取交，即该任务仍需补扫的帧段。"""
    st = _state_from_db(conn, r["roll_no"])
    remaining = intersect_intervals(
        st["missing"], [(r["frame_start"], r["frame_end"])]
    )
    return schemas.RescanClaimOut(
        id=r["id"],
        roll_no=r["roll_no"],
        claimant=r["claimant"],
        frame_start=r["frame_start"],
        frame_end=r["frame_end"],
        frame_count=r["frame_end"] - r["frame_start"] + 1,
        max_frames=r["max_frames"],
        ttl_seconds=r["ttl_seconds"],
        created_at=r["created_at"],
        expires_at=r["expires_at"],
        remaining_missing_ranges=[list(x) for x in remaining],
    )


def create_rescan_claim(
    conn: sqlite3.Connection, data: schemas.RescanClaimIn, roll_no: str
) -> schemas.RescanClaimOut:
    """原子领取一段缺帧。

    当前缺帧区间扣除所有未到期认领区间后，按帧号升序取首段长度不超过
    max_frames 的连续闭区间；整个过程在 BEGIN IMMEDIATE 写事务内完成，
    并发领取由 SQLite 写锁串行化，帧段不会交叠。
    """
    base = datetime.now(timezone.utc)
    now = base.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
    expires = (base + timedelta(seconds=data.ttl_seconds)).strftime(
        "%Y-%m-%dT%H:%M:%S.%f"
    )[:-3] + "Z"

    conn.execute("BEGIN IMMEDIATE")
    try:
        if not _roll_exists(conn, roll_no):
            raise RollNotFoundError(roll_no)

        st = _state_from_db(conn, roll_no)
        claimed = [
            (r["frame_start"], r["frame_end"])
            for r in _active_claim_rows(conn, roll_no)
        ]
        available = subtract_intervals(st["missing"], claimed)

        chosen: tuple[int, int] | None = None
        for s, e in available:  # 已按帧号升序
            if s <= e:  # subtract 已过滤空段，这里仅为类型检查友好
                chosen = (s, min(e, s + data.max_frames - 1))
                break
        if chosen is None:
            raise NoClaimableFramesError(roll_no)

        claim_id = uuid.uuid4().hex
        frame_start, frame_end = chosen
        conn.execute(
            """INSERT INTO rescan_claims
                   (id, roll_no, claimant, frame_start, frame_end,
                    max_frames, ttl_seconds, created_at, expires_at, released_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, NULL)""",
            (
                claim_id,
                roll_no,
                data.claimant,
                frame_start,
                frame_end,
                data.max_frames,
                data.ttl_seconds,
                now,
                expires,
            ),
        )
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise

    row = conn.execute(
        "SELECT * FROM rescan_claims WHERE id = ?", (claim_id,)
    ).fetchone()
    assert row is not None
    return _claim_out(conn, row)


def list_rescan_claims(
    conn: sqlite3.Connection, roll_no: str
) -> list[schemas.RescanClaimOut] | None:
    """列出某胶卷全部未到期、未释放的认领任务。胶卷不存在返回 None。"""
    if not _roll_exists(conn, roll_no):
        return None
    rows = _active_claim_rows(conn, roll_no)
    return [_claim_out(conn, r) for r in rows]


def release_rescan_claim(conn: sqlite3.Connection, claim_id: str) -> bool:
    """释放（删除）认领任务；任务不存在、已释放或已到期均返回 False。"""
    conn.execute("BEGIN IMMEDIATE")
    try:
        cur = conn.execute(
            """UPDATE rescan_claims
                  SET released_at = ?
                WHERE id = ?
                  AND released_at IS NULL
                  AND expires_at > ?""",
            (_now_iso(), claim_id, _now_iso()),
        )
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise
    return cur.rowcount > 0
