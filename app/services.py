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


def _insert_batch_rows(
    conn: sqlite3.Connection,
    *,
    batch_id: str,
    roll_no: str,
    expected_start: int,
    expected_end: int,
    raw_frames: list[int],
    digest: str,
) -> tuple[list[int], list[int]]:
    """在当前事务内写入一条批次及其帧归属/预期范围。

    供批次导入与补扫回执共用，保证两条路径的去重与归属规则完全一致：
    返回 (本批新增帧, 重复帧)，均为去重排序列表。调用方需自行保证 batch_id
    与 (roll_no, content_hash) 不冲突。
    """
    frames = sorted(set(raw_frames))
    existing_frames = _all_frames(conn, roll_no)

    within_submit = _within_batch_duplicates(raw_frames)
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
            roll_no,
            expected_start,
            expected_end,
            json.dumps(frames),
            json.dumps(list(raw_frames)),
            json.dumps(new_frames),
            json.dumps(duplicates),
            digest,
        ),
    )
    conn.execute(
        "INSERT OR IGNORE INTO roll_expected (roll_no, start_no, end_no) VALUES (?, ?, ?)",
        (roll_no, expected_start, expected_end),
    )
    conn.executemany(
        "INSERT OR IGNORE INTO roll_frames (roll_no, frame_no, batch_id) VALUES (?, ?, ?)",
        [(roll_no, f, batch_id) for f in new_frames],
    )
    return new_frames, duplicates


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

        new_frames, duplicates = _insert_batch_rows(
            conn,
            batch_id=batch_id,
            roll_no=data.roll_no,
            expected_start=data.expected_start,
            expected_end=data.expected_end,
            raw_frames=raw_frames,
            digest=digest,
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
       AND completed_at IS NULL
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
    """释放（删除）认领任务；任务不存在、已释放、已结案或已到期均返回 False。"""
    conn.execute("BEGIN IMMEDIATE")
    try:
        cur = conn.execute(
            """UPDATE rescan_claims
                  SET released_at = ?
                WHERE id = ?
                  AND released_at IS NULL
                  AND completed_at IS NULL
                  AND expires_at > ?""",
            (_now_iso(), claim_id, _now_iso()),
        )
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise
    return cur.rowcount > 0


# ---------- 补扫完成回执 ----------

class ClaimNotFoundError(Exception):
    """认领记录不存在（接口返回 404）。"""


class ClaimNotAcceptingError(Exception):
    """认领已到期、已释放或已结案，不能再提交回执（接口返回 409）。"""

    def __init__(self, claim_id: str, reason: str):
        self.claim_id = claim_id
        self.reason = reason
        super().__init__(f"认领 {claim_id} 无法接受回执: {reason}")


class ReceiptConflictError(Exception):
    """回执内容违规：帧区间/缺帧状态不符、内容变更或批次号冲突（409，数据不变）。"""

    def __init__(self, message: str):
        self.message = message
        super().__init__(message)


def submit_rescan_receipt(
    conn: sqlite3.Connection, claim_id: str, data: schemas.RescanReceiptIn
) -> tuple[schemas.RescanReceiptOut, int]:
    """提交补扫完成回执。返回 (回执, HTTP 状态码 201首次 / 200幂等重试)。

    - 只接受未到期、未释放、未结案的认领：不存在 → ClaimNotFoundError（404）；
      已到期/释放/结案 → ClaimNotAcceptingError（409），数据不变。
    - 帧号须全部落在认领闭区间 [frame_start, frame_end] 内，且提交时仍是该胶卷
      缺帧（未被任何批次扫到，回执内也不得重复）；否则 409，数据不变。
    - 以认领闭区间作为补扫批次的预期范围，批次导入、回执持久化与认领结案标记
      在同一个 BEGIN IMMEDIATE 写事务内完成，任何一步失败整体回滚。
    - 提交回执后认领即结案（部分补扫也结案），区间内剩余缺帧立即重新可被认领。
    - 相同认领 + 相同 batch_id + 相同原始帧列表的重试返回已存回执（200），
      不重复导入批次；内容变更、或对已结案认领提交新回执一律 409。
    """
    raw_frames = list(data.frames)
    now = _now_iso()

    conn.execute("BEGIN IMMEDIATE")
    try:
        claim = conn.execute(
            "SELECT * FROM rescan_claims WHERE id = ?", (claim_id,)
        ).fetchone()
        if claim is None:
            raise ClaimNotFoundError(claim_id)

        existing_receipt = conn.execute(
            "SELECT * FROM rescan_receipts WHERE claim_id = ?", (claim_id,)
        ).fetchone()
        if existing_receipt is not None:
            # 认领已结案：仅“相同认领 + 相同 batch_id + 相同原始帧列表”的重试
            # 返回已存回执；任何内容变更一律冲突，数据不变。
            same_retry = (
                existing_receipt["batch_id"] == data.batch_id
                and json.loads(existing_receipt["raw_frames_json"]) == raw_frames
            )
            if not same_retry:
                raise ReceiptConflictError(
                    "该认领已结案（已有回执），提交不同 batch_id 或帧列表的新回执"
                    "被拒绝，数据保持不变"
                )
            conn.execute("ROLLBACK")
            return _load_existing_receipt(conn, existing_receipt, claim)

        if claim["released_at"] is not None:
            raise ClaimNotAcceptingError(claim_id, "released")
        if claim["expires_at"] <= now:
            raise ClaimNotAcceptingError(claim_id, "expired")
        if claim["completed_at"] is not None:
            # 已结案却没有回执（理论上不会发生），按不可提交处理
            raise ClaimNotAcceptingError(claim_id, "completed")

        roll_no = claim["roll_no"]
        f_start, f_end = claim["frame_start"], claim["frame_end"]

        # 帧号须在认领闭区间内（负数等异常值天然越界，一并拒绝）
        out_of_claim = sorted({f for f in raw_frames if f < f_start or f > f_end})
        if out_of_claim:
            raise ReceiptConflictError(
                f"帧号 {out_of_claim} 超出认领闭区间 [{f_start}, {f_end}]，"
                "本次未导入任何数据"
            )

        # 提交时仍须是该胶卷缺帧：未被其他批次扫到，回执原始列表内也不得重复
        frames = sorted(set(raw_frames))
        within_dup = _within_batch_duplicates(raw_frames)
        already = sorted(set(frames) & _all_frames(conn, roll_no))
        not_missing = sorted(within_dup | set(already))
        if not_missing:
            raise ReceiptConflictError(
                f"帧号 {not_missing} 在提交时已不属于该胶卷缺帧"
                "（已被扫到或在本回执内重复），本次未导入任何数据"
            )

        # 补扫批次沿用批次接口的导入与幂等规则，以认领区间作为批次预期范围
        digest = content_hash(roll_no, f_start, f_end, frames)
        clash_batch = conn.execute(
            "SELECT roll_no, content_hash FROM batches WHERE id = ?",
            (data.batch_id,),
        ).fetchone()
        clash_content = conn.execute(
            "SELECT id FROM batches WHERE roll_no = ? AND content_hash = ?",
            (roll_no, digest),
        ).fetchone()
        if clash_batch is not None or clash_content is not None:
            # 内容相同的批次若已存在，则其中帧必然已扫到，与上面“仍属缺帧”
            # 校验互斥；无论何种占用均按冲突拒绝，数据不变。
            raise ReceiptConflictError(
                f"batch_id '{data.batch_id}' 或相同内容的批次已存在，"
                "无法作为补扫批次重复导入，本次未改动任何数据"
            )

        added_frames, duplicates = _insert_batch_rows(
            conn,
            batch_id=data.batch_id,
            roll_no=roll_no,
            expected_start=f_start,
            expected_end=f_end,
            raw_frames=raw_frames,
            digest=digest,
        )

        receipt_id = uuid.uuid4().hex
        conn.execute(
            """INSERT INTO rescan_receipts
                   (id, claim_id, roll_no, batch_id, raw_frames_json,
                    added_frames_json, duplicates_json, created_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                receipt_id,
                claim_id,
                roll_no,
                data.batch_id,
                json.dumps(raw_frames),
                json.dumps(added_frames),
                json.dumps(duplicates),
                now,
            ),
        )
        # 提交回执即结案（部分补扫也结案）：剩余缺帧随后立即重新可认领
        conn.execute(
            "UPDATE rescan_claims SET completed_at = ? WHERE id = ?",
            (now, claim_id),
        )
        conn.execute("COMMIT")
    except ClaimNotFoundError:
        conn.execute("ROLLBACK")
        raise
    except ClaimNotAcceptingError:
        conn.execute("ROLLBACK")
        raise
    except ReceiptConflictError:
        conn.execute("ROLLBACK")
        raise
    except Exception:
        conn.execute("ROLLBACK")
        raise

    claim_after = conn.execute(
        "SELECT * FROM rescan_claims WHERE id = ?", (claim_id,)
    ).fetchone()
    assert claim_after is not None
    return _build_receipt_out(
        conn,
        claim_after,
        receipt_id=receipt_id,
        batch_id=data.batch_id,
        raw_frames=raw_frames,
        added_frames=added_frames,
        duplicates=duplicates,
        repeated=False,
        created_at=now,
    ), 201


def _build_receipt_out(
    conn: sqlite3.Connection,
    claim: sqlite3.Row,
    *,
    receipt_id: str,
    batch_id: str,
    raw_frames: list[int],
    added_frames: list[int],
    duplicates: list[int],
    repeated: bool,
    created_at: str,
) -> schemas.RescanReceiptOut:
    st = _state_from_db(conn, claim["roll_no"])
    remaining = intersect_intervals(
        st["missing"], [(claim["frame_start"], claim["frame_end"])]
    )
    return schemas.RescanReceiptOut(
        receipt_id=receipt_id,
        claim_id=claim["id"],
        roll_no=claim["roll_no"],
        batch_id=batch_id,
        frames=raw_frames,
        added_frames=added_frames,
        duplicates=duplicates,
        repeated=repeated,
        claim_completed=claim["completed_at"] is not None,
        claim_frame_start=claim["frame_start"],
        claim_frame_end=claim["frame_end"],
        remaining_missing_ranges=[list(x) for x in remaining],
        created_at=created_at,
    )


def _load_existing_receipt(
    conn: sqlite3.Connection, receipt: sqlite3.Row, claim: sqlite3.Row
) -> tuple[schemas.RescanReceiptOut, int]:
    """组装已存回执（幂等重试）；即使补扫批次后来被撤销也能原样返回。"""
    out = _build_receipt_out(
        conn,
        claim,
        receipt_id=receipt["id"],
        batch_id=receipt["batch_id"],
        raw_frames=json.loads(receipt["raw_frames_json"]),
        added_frames=json.loads(receipt["added_frames_json"]),
        duplicates=json.loads(receipt["duplicates_json"]),
        repeated=True,
        created_at=receipt["created_at"],
    )
    return out, 200
