"""FastAPI 入口：胶片档案扫描整理 API。"""
from __future__ import annotations

import os
from contextlib import asynccontextmanager

import uvicorn
from fastapi import Depends, FastAPI, HTTPException, Response, status
from sqlite3 import Connection

from . import database, schemas, services


@asynccontextmanager
async def lifespan(app: FastAPI):
    # 启动即建库建表（幂等），依赖与数据库初始化都纳入启动流程
    database.init_db()
    yield


app = FastAPI(
    title="胶片档案扫描整理 API",
    description="按胶卷汇总扫描帧、去重、统计预期范围内的连续缺帧；批次导入幂等；支持误导入批次撤销与补扫任务认领。",
    version="1.2.0",
    lifespan=lifespan,
)


@app.get("/health", response_model=schemas.HealthOut, tags=["系统"])
def health():
    return {"status": "ok", "db": str(database.get_db_path())}


@app.post(
    "/batches",
    response_model=schemas.BatchResult,
    status_code=status.HTTP_201_CREATED,
    tags=["批次"],
)
def submit_batch(
    payload: schemas.BatchIn,
    response: Response,
    conn: Connection = Depends(database.get_db),
):
    """提交一个扫描批次。相同内容重复提交返回 200 且不改变任何结果。"""
    try:
        result, code = services.submit_batch(conn, payload)
    except services.BatchIdConflictError as e:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f"batch_id '{e.batch_id}' 已被其他内容占用，请更换后重试",
        )
    response.status_code = code
    return result


@app.get("/batches/{batch_id}", response_model=schemas.BatchResult, tags=["批次"])
def get_batch(batch_id: str, conn: Connection = Depends(database.get_db)):
    result = services.get_batch(conn, batch_id)
    if result is None:
        raise HTTPException(status_code=404, detail="批次不存在")
    return result


@app.delete(
    "/batches/{batch_id}",
    status_code=status.HTTP_204_NO_CONTENT,
    tags=["批次"],
)
def delete_batch(batch_id: str, conn: Connection = Depends(database.get_db)):
    """撤销一个误导入的批次。

    删除该批并按剩余批次原导入顺序重建预期范围、帧归属及各批新增/重复明细
    （共享帧不会丢失，批内重复保留）。批次不存在返回 404；若剩余批次为无法
    区分批内/跨批重复的旧数据，返回 409 且数据不变。
    """
    try:
        services.delete_batch(conn, batch_id.strip())
    except services.BatchNotFoundError:
        raise HTTPException(status_code=404, detail="批次不存在")
    except services.LegacyBatchDataError as e:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=(
                "撤销被拒绝：该胶卷存在旧版格式的历史批次，缺少原始帧列表，"
                f"无法区分批内与跨批重复（相关批次：{', '.join(e.legacy_batch_ids)}）；"
                "为保证重建结果准确，本次未改动任何数据。"
            ),
        )
    return Response(status_code=status.HTTP_204_NO_CONTENT)


@app.get("/rolls", response_model=list[schemas.RollSummary], tags=["胶卷"])
def list_rolls(conn: Connection = Depends(database.get_db)):
    """列出所有胶卷及其汇总状态。"""
    return services.list_rolls(conn)


@app.get("/rolls/{roll_no}", response_model=schemas.RollState, tags=["胶卷"])
def get_roll(roll_no: str, conn: Connection = Depends(database.get_db)):
    """查询单个胶卷的合并帧号、重复帧明细与连续缺帧区间。"""
    result = services.get_roll_state(conn, roll_no.strip())
    if result is None:
        raise HTTPException(status_code=404, detail="胶卷不存在（尚未导入任何批次）")
    return result


@app.post(
    "/rolls/{roll_no}/rescan-claims",
    response_model=schemas.RescanClaimOut,
    status_code=status.HTTP_201_CREATED,
    tags=["补扫认领"],
)
def create_rescan_claim(
    roll_no: str,
    payload: schemas.RescanClaimIn,
    conn: Connection = Depends(database.get_db),
):
    """认领一段缺帧用于补扫。

    当前缺帧区间扣除所有未到期认领后，按帧号升序分配首段不超过
    ``max_frames`` 的连续闭区间。胶卷不存在返回 404，暂无可领取帧返回 409。
    """
    try:
        return services.create_rescan_claim(conn, payload, roll_no.strip())
    except services.RollNotFoundError:
        raise HTTPException(status_code=404, detail="胶卷不存在（尚未导入任何批次）")
    except services.NoClaimableFramesError:
        raise HTTPException(status_code=409, detail="当前没有可领取的缺帧（可能已被其他未到期任务认领）")


@app.get(
    "/rolls/{roll_no}/rescan-claims",
    response_model=list[schemas.RescanClaimOut],
    tags=["补扫认领"],
)
def list_rescan_claims(
    roll_no: str, conn: Connection = Depends(database.get_db)
):
    """列出该胶卷全部未到期的补扫认领任务，及其帧段内仍缺失的帧段。"""
    result = services.list_rescan_claims(conn, roll_no.strip())
    if result is None:
        raise HTTPException(status_code=404, detail="胶卷不存在（尚未导入任何批次）")
    return result


@app.delete(
    "/rescan-claims/{claim_id}",
    status_code=status.HTTP_204_NO_CONTENT,
    tags=["补扫认领"],
)
def release_rescan_claim(
    claim_id: str, conn: Connection = Depends(database.get_db)
):
    """释放（删除）一个补扫认领任务；任务不存在、已释放或已到期返回 404。"""
    if not services.release_rescan_claim(conn, claim_id.strip()):
        raise HTTPException(status_code=404, detail="补扫任务不存在、已释放或已到期")
    return Response(status_code=status.HTTP_204_NO_CONTENT)


if __name__ == "__main__":
    uvicorn.run(
        "app.main:app",
        host=os.environ.get("HOST", "0.0.0.0"),
        port=int(os.environ.get("PORT", "8000")),
    )
