from typing import Any

from pydantic import BaseModel


class TextPayload(BaseModel):
    summary: str
    details: str | None = None


class TablePayload(BaseModel):
    columns: list[str]
    rows: list[dict[str, Any]]
    total_rows: int | None = None


class ChartPayload(BaseModel):
    renderer: str = "echarts"
    title: str
    option: dict[str, Any]
    source: list[dict[str, Any]]
    description: str | None = None
