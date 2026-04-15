from __future__ import annotations

import json
import urllib.request
from datetime import datetime
from typing import Any

from ndea.config import Settings
from ndea.planning.models import QueryWorkflowPayload
from ndea.portal.models import (
    PortalQueryMetadataPayload,
    PortalQueryPayload,
    PortalTableColumnPayload,
    PortalTablePayload,
)


TIME_COLUMN_TOKENS = ("date", "time", "day", "week", "month", "year", "日期", "时间", "天", "周", "月", "年", "学年")
PIE_QUERY_TOKENS = ("占比", "比例", "构成", "结构", "分布", "占所有", "占总体")


def embed_texts(base_url: str, model: str, texts: list[str]) -> list[list[float]]:
    payload = json.dumps({"model": model, "input": texts}, ensure_ascii=False).encode("utf-8")
    request = urllib.request.Request(
        url=f"{base_url.rstrip('/')}/api/embed",
        data=payload,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(request, timeout=120) as response:
        body = json.loads(response.read().decode("utf-8"))
    embeddings = body.get("embeddings")
    if not isinstance(embeddings, list) or len(embeddings) != len(texts):
        raise RuntimeError("Embedding service returned unexpected payload")
    return embeddings


class PortalQueryService:
    def __init__(
        self,
        settings: Settings | None = None,
        workflow_service: Any | None = None,
        embedder: Any | None = None,
    ) -> None:
        self._settings = settings or Settings()
        self._workflow_service = workflow_service
        self._embedder = embedder or embed_texts

    def query(
        self,
        query_text: str,
        database: str | None = None,
        request_context: dict[str, object] | None = None,
        policy_context: dict[str, object] | None = None,
    ) -> PortalQueryPayload:
        try:
            query_vector = self._embedder(
                self._settings.embedding_base_url,
                self._settings.embedding_model,
                [query_text],
            )[0]
        except Exception as exc:
            return self._error_payload(f"Embedding failed: {exc}")

        try:
            workflow_service = self._require_workflow_service()
            workflow_payload = workflow_service.run(
                query_text=query_text,
                query_vector=query_vector,
                database=database or self._settings.mysql_database or None,
                execute=True,
                request_context=request_context,
                policy_context=policy_context,
            )
        except Exception as exc:
            return self._error_payload(f"Query workflow failed: {exc}")

        normalized = self._normalize_workflow_payload(workflow_payload)
        return self._to_portal_payload(normalized)

    def _require_workflow_service(self) -> Any:
        if self._workflow_service is not None:
            return self._workflow_service
        from ndea.tools.query_workflow import get_query_workflow_service

        self._workflow_service = get_query_workflow_service()
        return self._workflow_service

    def _normalize_workflow_payload(self, payload: Any) -> QueryWorkflowPayload:
        if isinstance(payload, QueryWorkflowPayload):
            return payload
        if hasattr(payload, "model_dump"):
            payload = payload.model_dump(mode="json")
        return QueryWorkflowPayload.model_validate(payload)

    def _to_portal_payload(self, payload: QueryWorkflowPayload) -> PortalQueryPayload:
        table = self._build_table(payload)
        visualization = self._build_visualization(payload, table)
        return PortalQueryPayload(
            text=self._build_text(payload),
            table=table,
            visualization=visualization,
            clarification_required=payload.clarification_required,
            clarification_question=(payload.clarification_questions[0] if payload.clarification_questions else None),
            executed=payload.executed,
            sql=self._resolve_sql(payload),
            metadata=PortalQueryMetadataPayload(
                tool_trace=list(payload.tool_trace),
                confidence=payload.plan.confidence,
                selected_sql_asset_id=payload.plan.selected_sql_asset_id,
                metric_id=payload.plan.metric_id,
            ),
        )

    def _build_text(self, payload: QueryWorkflowPayload) -> str:
        summary = payload.response_text.summary
        details = payload.response_text.details
        if details:
            return f"{summary}\n\n{details}"
        return summary

    def _build_table(self, payload: QueryWorkflowPayload) -> PortalTablePayload | None:
        table = payload.response_table
        if table is None:
            return None
        return PortalTablePayload(
            columns=[
                PortalTableColumnPayload(key=column, label=self._humanize_column(column))
                for column in table.columns
            ],
            rows=[dict(row) for row in table.rows],
        )

    def _build_visualization(
        self,
        payload: QueryWorkflowPayload,
        table: PortalTablePayload | None,
    ) -> dict[str, Any] | None:
        if table is None or len(table.rows) < 2 or len(table.columns) < 2:
            return None

        category_key, numeric_keys = self._infer_table_schema(table.rows, table.columns)
        if category_key is None or not numeric_keys:
            return None

        chart_kind = self._select_chart_kind(payload, category_key, numeric_keys, table.rows)
        option = self._build_echarts_option(chart_kind, table, category_key, numeric_keys)
        if option is None:
            return None

        return {
            "type": "visualization",
            "version": "1.0",
            "renderer": "echarts",
            "title": payload.query_text,
            "description": payload.response_chart.description if payload.response_chart is not None else payload.response_text.summary,
            "chart": {
                "kind": chart_kind,
                "spec": {
                    "option": option,
                },
            },
            "data": {
                "source": table.rows,
                "fields": [
                    {
                        "key": column.key,
                        "type": "number" if column.key in numeric_keys else "string",
                        "label": column.label,
                    }
                    for column in table.columns
                ],
            },
            "style": {
                "theme": "light",
                "width": "100%",
                "height": 360,
                "responsive": True,
            },
            "interaction": {
                "tooltip": True,
                "legend": len(numeric_keys) > 1 or chart_kind == "pie",
                "dataZoom": chart_kind == "line",
                "saveAsImage": True,
                "clickable": False,
            },
            "fallback": {
                "type": "table",
                "showRawData": True,
            },
            "meta": {
                "source": "external-mcp",
                "generatedAt": datetime.now().astimezone().isoformat(),
            },
        }

    def _infer_table_schema(
        self,
        rows: list[dict[str, Any]],
        columns: list[PortalTableColumnPayload],
    ) -> tuple[str | None, list[str]]:
        numeric_keys = [
            column.key
            for column in columns
            if all(self._is_numeric(row.get(column.key)) for row in rows)
        ]
        category_key = next((column.key for column in columns if column.key not in numeric_keys), None)
        return category_key, [key for key in numeric_keys if key != category_key]

    def _select_chart_kind(
        self,
        payload: QueryWorkflowPayload,
        category_key: str,
        numeric_keys: list[str],
        rows: list[dict[str, Any]],
    ) -> str:
        lowered_key = category_key.lower()
        if payload.plan.intent_type == "trend" or payload.plan.time_grain or any(token in lowered_key for token in TIME_COLUMN_TOKENS):
            return "line"
        if len(numeric_keys) == 1 and len(rows) <= 12 and any(token in payload.query_text for token in PIE_QUERY_TOKENS):
            return "pie"
        return "bar"

    def _build_echarts_option(
        self,
        chart_kind: str,
        table: PortalTablePayload,
        category_key: str,
        numeric_keys: list[str],
    ) -> dict[str, Any] | None:
        if chart_kind == "pie":
            series_key = numeric_keys[0]
            category_label = self._label_for_column(table.columns, category_key)
            value_label = self._label_for_column(table.columns, series_key)
            return {
                "tooltip": {"trigger": "item"},
                "legend": {"bottom": 0},
                "series": [
                    {
                        "type": "pie",
                        "radius": ["35%", "68%"],
                        "data": [
                            {
                                "name": row.get(category_key),
                                "value": self._to_number(row.get(series_key)),
                            }
                            for row in table.rows
                        ],
                        "label": {"formatter": "{b}: {d}%"},
                    }
                ],
                "title": {
                    "text": value_label,
                    "subtext": category_label,
                    "left": "center",
                },
            }

        x_axis_data = [row.get(category_key) for row in table.rows]
        series_type = "line" if chart_kind == "line" else "bar"
        return {
            "tooltip": {"trigger": "axis"},
            "legend": {"top": 0},
            "grid": {"left": 24, "right": 24, "top": 48, "bottom": 24, "containLabel": True},
            "xAxis": {"type": "category", "data": x_axis_data},
            "yAxis": {"type": "value"},
            "series": [
                {
                    "name": self._label_for_column(table.columns, series_key),
                    "type": series_type,
                    "smooth": chart_kind == "line",
                    "data": [self._to_number(row.get(series_key)) for row in table.rows],
                }
                for series_key in numeric_keys
            ],
        }

    def _resolve_sql(self, payload: QueryWorkflowPayload) -> str | None:
        execution = payload.execution or {}
        effective_sql = execution.get("effective_sql")
        if isinstance(effective_sql, str) and effective_sql:
            return effective_sql
        sql = execution.get("sql")
        if isinstance(sql, str) and sql:
            return sql
        if payload.generation is not None and getattr(payload.generation, "sql", None):
            return getattr(payload.generation, "sql")
        return payload.plan.selected_sql

    def _humanize_column(self, value: str) -> str:
        if not value:
            return value
        return value.replace("_", " ").strip()

    def _label_for_column(self, columns: list[PortalTableColumnPayload], key: str) -> str:
        for column in columns:
            if column.key == key:
                return column.label
        return key

    def _is_numeric(self, value: Any) -> bool:
        if value is None:
            return True
        return isinstance(value, (int, float))

    def _to_number(self, value: Any) -> float:
        if isinstance(value, bool):
            return float(int(value))
        if isinstance(value, (int, float)):
            return float(value)
        return 0.0

    def _error_payload(self, message: str) -> PortalQueryPayload:
        return PortalQueryPayload(
            text=message,
            table=None,
            visualization=None,
            clarification_required=False,
            clarification_question=None,
            executed=False,
            sql=None,
            metadata=PortalQueryMetadataPayload(),
        )
