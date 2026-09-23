"""Pydantic 请求/响应模型。"""
from __future__ import annotations

from typing import Annotated

from pydantic import BaseModel, Field, field_validator

PositiveInt = Annotated[int, Field(ge=1)]


class BatchIn(BaseModel):
    roll_no: str = Field(..., min_length=1, description="胶卷编号")
    expected_start: int = Field(..., ge=0, description="预期帧号起始（含）")
    expected_end: int = Field(..., ge=0, description="预期帧号结束（含）")
    frames: list[int] = Field(..., min_length=1, description="本批扫描帧号列表")
    batch_id: str | None = Field(
        None, description="客户端批次号（可选）；留空则按内容生成"
    )

    @field_validator("roll_no", "batch_id")
    @classmethod
    def _strip(cls, v: str | None) -> str | None:
        if v is None:
            return None
        v = v.strip()
        return v or None

    @field_validator("expected_end")
    @classmethod
    def _check_range(cls, v: int, info) -> int:
        start = info.data.get("expected_start")
        if start is not None and v < start:
            raise ValueError("expected_end 不能小于 expected_start")
        return v


class BatchResult(BaseModel):
    batch_id: str
    roll_no: str
    repeated: bool = Field(..., description="True=内容相同的批次已导入过，本次未做任何更改")
    added_frames: list[int]
    duplicates: list[int] = Field(..., description="重复帧（批内重复 + 胶卷已有），只计一次")
    merged_frames: list[int] = Field(..., description="导入后该胶卷的全部帧号（去重排序）")
    expected_ranges: list[list[int]]
    missing_ranges: list[list[int]] = Field(..., description="预期范围内的连续缺帧区间")
    out_of_range_frames: list[int] = Field(..., description="落在全部预期范围之外的帧")
    created_at: str | None = None


class RollState(BaseModel):
    roll_no: str
    expected_ranges: list[list[int]]
    frames: list[int]
    missing_ranges: list[list[int]]
    out_of_range_frames: list[int]
    total_frames: int
    batch_count: int
    batches: list["BatchMeta"]


class BatchMeta(BaseModel):
    batch_id: str
    expected_start: int
    expected_end: int
    frames: list[int]
    added_frames: list[int]
    duplicates: list[int]
    created_at: str


class RollSummary(BaseModel):
    roll_no: str
    total_frames: int
    batch_count: int
    missing_ranges: list[list[int]]


class HealthOut(BaseModel):
    status: str
    db: str


class RescanClaimIn(BaseModel):
    claimant: str = Field(..., min_length=1, description="领取人标识")
    max_frames: PositiveInt = Field(..., description="本次最多领取的帧数（正整数）")
    ttl_seconds: PositiveInt = Field(..., description="认领有效秒数（正整数），到期后帧段自动让出")

    @field_validator("claimant")
    @classmethod
    def _strip_claimant(cls, v: str) -> str:
        v = v.strip()
        if not v:
            raise ValueError("claimant 不能为空")
        return v


class RescanClaimOut(BaseModel):
    id: str
    roll_no: str
    claimant: str
    frame_start: int = Field(..., description="认领帧段起始（含）")
    frame_end: int = Field(..., description="认领帧段结束（含）")
    frame_count: int
    max_frames: int
    ttl_seconds: int
    created_at: str
    expires_at: str
    remaining_missing_ranges: list[list[int]] = Field(
        ..., description="该任务帧段中目前仍缺失（未扫到）的连续闭区间"
    )


class RescanReceiptIn(BaseModel):
    batch_id: str = Field(
        ..., min_length=1, description="本次补扫导入的批次号（必填，沿用批次接口）"
    )
    frames: list[int] = Field(
        ...,
        min_length=1,
        description="实际扫到的非空帧号原始列表（至少 1 个；须落在认领闭区间内）",
    )

    @field_validator("batch_id")
    @classmethod
    def _strip_batch_id(cls, v: str) -> str:
        v = v.strip()
        if not v:
            raise ValueError("batch_id 不能为空")
        return v


class RescanReceiptOut(BaseModel):
    receipt_id: str
    claim_id: str
    roll_no: str
    batch_id: str
    frames: list[int] = Field(..., description="回执中的原始帧列表（与提交一致，保序）")
    added_frames: list[int] = Field(..., description="本批补扫新增的帧号（去重排序）")
    duplicates: list[int] = Field(..., description="批内重复/此前已扫的帧号（只计一次）")
    repeated: bool = Field(
        ..., description="True=相同认领/批次/原始帧列表的回执已存在，本次未做任何更改"
    )
    claim_completed: bool = Field(..., description="认领是否随本次回执结案")
    claim_frame_start: int
    claim_frame_end: int
    remaining_missing_ranges: list[list[int]] = Field(
        ..., description="认领闭区间内目前仍缺失的连续帧段（提交后即时可见）"
    )
    created_at: str


RollState.model_rebuild()
