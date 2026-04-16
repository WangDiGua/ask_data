from __future__ import annotations

import json
import math
import urllib.request
from typing import Any

from ndea.config import Settings
from ndea.portal.models import (
    PortalQueryMetadataPayload,
    PortalQueryPayload,
    PortalTableColumnPayload,
    PortalTablePayload,
)
from ndea.query_v2 import QueryRequestV2, QueryResponseV2


TIME_TOKENS = ("date", "time", "day", "week", "month", "year", "日期", "时间", "天", "周", "月", "年", "学年", "学期")
PIE_TOKENS = ("占比", "比例", "构成", "结构", "分布")
STACKED_TOKENS = ("构成", "占比", "比例", "结构")
RANKING_TOKENS = ("排名", "top", "最多", "最高", "最大")


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
        query_service: Any | None = None,
        workflow_service: Any | None = None,
        embedder: Any | None = None,
    ) -> None:
        self._settings = settings or Settings()
        self._query_service = query_service or workflow_service
        self._embedder = embedder or embed_texts

    def query(
        self,
        query_text: str,
        database: str | None = None,
        request_context: dict[str, object] | None = None,
        policy_context: dict[str, object] | None = None,
    ) -> PortalQueryPayload:
        try:
            service = self._require_query_service()
            response = service.run(
                QueryRequestV2(
                    query_text=query_text,
                    database=database or self._settings.mysql_database or None,
                    request_context=request_context,
                    policy_context=policy_context,
                )
            )
        except Exception as exc:
            return self._error_payload(f"Query workflow failed: {exc}")
        return self._to_portal_payload(self._normalize_response(response))

    def _require_query_service(self) -> Any:
        if self._query_service is not None:
            return self._query_service
        from ndea.services import QueryServiceV2

        self._query_service = QueryServiceV2(settings=self._settings)
        return self._query_service

    def _normalize_response(self, payload: Any) -> QueryResponseV2:
        if isinstance(payload, QueryResponseV2):
            return payload
        if hasattr(payload, "model_dump"):
            payload = payload.model_dump(mode="json")
        return QueryResponseV2.model_validate(payload)

    def _to_portal_payload(self, payload: QueryResponseV2) -> PortalQueryPayload:
        table = self._build_table(payload)
        visualization = self._build_visualization(payload, table)
        interpretation = payload.interpretation
        selected_plan = interpretation.selected_plan
        selected_sql = interpretation.selected_sql
        clarification_reason = payload.clarification.reason or (
            selected_plan.clarification_question if selected_plan is not None else None
        )
        return PortalQueryPayload(
            text=self._build_text(payload),
            table=table,
            visualization=visualization,
            clarification_required=payload.clarification.required,
            clarification_question=payload.clarification.question,
            executed=payload.executed,
            sql=payload.sql,
            metadata=PortalQueryMetadataPayload(
                tool_trace=list(payload.tool_trace),
                confidence=payload.confidence,
                selected_sql_asset_id=selected_sql.candidate_id if selected_sql is not None else None,
                metric_id=interpretation.ir.metric,
                answer_mode=interpretation.ir.answer_mode,
                resolved_tables=list(selected_plan.candidate_tables if selected_plan is not None else []),
                resolved_entities=[{"type": "entity_scope", "value": interpretation.ir.entity_scope}]
                if interpretation.ir.entity_scope
                else [],
                sql_strategy=selected_sql.source if selected_sql is not None else None,
                clarification_reason=clarification_reason,
            ),
        )

    def _build_text(self, payload: QueryResponseV2) -> str:
        summary = payload.answer.summary
        details = payload.answer.details
        if details:
            return f"{summary}\n\n{details}"
        return summary

    def _build_table(self, payload: QueryResponseV2) -> PortalTablePayload | None:
        if payload.table is None:
            return None
        label_map = self._column_label_map(payload)
        return PortalTablePayload(
            columns=[
                PortalTableColumnPayload(
                    key=column,
                    label=label_map.get(column, self._humanize_column(column)),
                )
                for column in payload.table.columns
            ],
            rows=[dict(row) for row in payload.table.rows],
        )

    def _build_visualization(
        self,
        payload: QueryResponseV2,
        table: PortalTablePayload | None,
    ) -> dict[str, Any] | None:
        if payload.interpretation.ir.answer_mode in {"detail", "roster", "record"}:
            return None
        if payload.chart is not None:
            return {
                "type": "visualization",
                "version": "2.0",
                "renderer": payload.chart.renderer,
                "title": payload.chart.title,
                "description": payload.chart.description,
                "chart": {
                    "kind": payload.chart.option.get("series", [{}])[0].get("type", "bar"),
                    "spec": {"option": payload.chart.option},
                },
                "data": {"source": payload.chart.source},
            }
        if table is None or len(table.columns) < 2 or len(table.rows) < 2:
            return None

        schema = self._infer_table_schema(table.rows, table.columns)
        if not schema["numeric_keys"]:
            return None
        chart_kind = self._select_chart_kind(payload, schema, table.rows)
        option = self._build_echarts_option(chart_kind, table, schema)
        if option is None:
            return None

        return {
            "type": "visualization",
            "version": "2.0",
            "renderer": "echarts",
            "title": payload.interpretation.interaction.query_text,
            "description": payload.answer.summary,
            "chart": {"kind": chart_kind, "spec": {"option": option}},
            "data": {"source": table.rows},
        }

    def _infer_table_schema(
        self,
        rows: list[dict[str, Any]],
        columns: list[PortalTableColumnPayload],
    ) -> dict[str, Any]:
        numeric_keys = [column.key for column in columns if self._is_numeric_column(rows, column.key)]
        categorical_keys = [column.key for column in columns if column.key not in numeric_keys]
        time_key = next((key for key in categorical_keys if self._looks_like_time_dimension(key, rows)), None)
        return {
            "numeric_keys": numeric_keys,
            "categorical_keys": categorical_keys,
            "primary_category_key": time_key or (categorical_keys[0] if categorical_keys else None),
            "time_key": time_key,
        }

    def _select_chart_kind(
        self,
        payload: QueryResponseV2,
        schema: dict[str, Any],
        rows: list[dict[str, Any]],
    ) -> str:
        query_text = payload.interpretation.interaction.query_text.lower()
        numeric_keys = schema["numeric_keys"]
        categorical_keys = schema["categorical_keys"]
        if schema["time_key"] is not None or payload.interpretation.ir.intent_type == "trend":
            return "line"
        if len(categorical_keys) >= 2 and len(numeric_keys) == 1:
            return "heatmap"
        if len(numeric_keys) == 1 and len(rows) <= 12 and any(token in query_text for token in PIE_TOKENS):
            return "pie"
        if len(numeric_keys) > 1:
            return "bar-stacked" if any(token in query_text for token in STACKED_TOKENS) else "bar-grouped"
        primary_key = schema["primary_category_key"]
        if primary_key and (any(token in query_text for token in RANKING_TOKENS) or len(rows) > 6):
            return "bar-horizontal"
        return "bar"

    def _build_echarts_option(
        self,
        chart_kind: str,
        table: PortalTablePayload,
        schema: dict[str, Any],
    ) -> dict[str, Any] | None:
        if chart_kind == "pie":
            return self._build_pie_option(table, schema)
        if chart_kind == "heatmap":
            return self._build_heatmap_option(table, schema)
        return self._build_cartesian_option(chart_kind, table, schema)

    def _build_pie_option(self, table: PortalTablePayload, schema: dict[str, Any]) -> dict[str, Any] | None:
        category_key = schema["primary_category_key"]
        numeric_keys = schema["numeric_keys"]
        if category_key is None or len(numeric_keys) != 1:
            return None
        series_key = numeric_keys[0]
        return {
            "tooltip": {"trigger": "item"},
            "series": [
                {
                    "type": "pie",
                    "radius": ["40%", "72%"],
                    "data": [
                        {
                            "name": self._format_category_value(row.get(category_key)),
                            "value": self._to_number(row.get(series_key)),
                        }
                        for row in table.rows
                    ],
                }
            ],
        }

    def _build_heatmap_option(self, table: PortalTablePayload, schema: dict[str, Any]) -> dict[str, Any] | None:
        categorical_keys = schema["categorical_keys"]
        numeric_keys = schema["numeric_keys"]
        if len(categorical_keys) < 2 or len(numeric_keys) != 1:
            return None
        x_key, y_key = categorical_keys[:2]
        value_key = numeric_keys[0]
        x_values = self._unique_in_order(self._format_category_value(row.get(x_key)) for row in table.rows)
        y_values = self._unique_in_order(self._format_category_value(row.get(y_key)) for row in table.rows)
        x_index = {value: index for index, value in enumerate(x_values)}
        y_index = {value: index for index, value in enumerate(y_values)}
        points = [
            [
                x_index[self._format_category_value(row.get(x_key))],
                y_index[self._format_category_value(row.get(y_key))],
                self._to_number(row.get(value_key)),
            ]
            for row in table.rows
        ]
        max_value = max((point[2] for point in points), default=0.0)
        return {
            "xAxis": {"type": "category", "data": x_values},
            "yAxis": {"type": "category", "data": y_values},
            "visualMap": {"min": 0, "max": math.ceil(max_value) if max_value > 0 else 1},
            "series": [{"type": "heatmap", "data": points}],
        }

    def _build_cartesian_option(
        self,
        chart_kind: str,
        table: PortalTablePayload,
        schema: dict[str, Any],
    ) -> dict[str, Any] | None:
        category_key = schema["primary_category_key"]
        numeric_keys = schema["numeric_keys"]
        if category_key is None or not numeric_keys:
            return None
        series_type = "line" if chart_kind == "line" else "bar"
        horizontal = chart_kind == "bar-horizontal"
        stacked = chart_kind == "bar-stacked"
        labels = [self._format_category_value(row.get(category_key)) for row in table.rows]
        series = []
        for series_key in numeric_keys:
            item = {
                "name": self._label_for_column(table.columns, series_key),
                "type": series_type,
                "data": [self._to_number(row.get(series_key)) for row in table.rows],
            }
            if stacked:
                item["stack"] = "total"
            series.append(item)
        if horizontal:
            return {
                "xAxis": {"type": "value"},
                "yAxis": {"type": "category", "data": labels},
                "series": series,
            }
        return {
            "xAxis": {"type": "category", "data": labels},
            "yAxis": {"type": "value"},
            "series": series,
        }

    def _column_label_map(self, payload: QueryResponseV2) -> dict[str, str]:
        labels: dict[str, str] = {}
        if payload.table is not None:
            for column in payload.table.columns:
                labels[column] = self._humanize_column(column)
        selected_plan = payload.interpretation.selected_plan
        if selected_plan is not None:
            for dimension in selected_plan.dimensions:
                column = dimension.get("column")
                if column:
                    labels[column] = dimension.get("name") or self._humanize_column(column)
        return labels

    def _looks_like_time_dimension(self, key: str, rows: list[dict[str, Any]]) -> bool:
        lowered = key.lower()
        if any(token in lowered for token in TIME_TOKENS):
            return True
        samples = [row.get(key) for row in rows[:8]]
        return all(self._looks_like_time_value(value) for value in samples if value is not None)

    def _looks_like_time_value(self, value: Any) -> bool:
        if isinstance(value, int):
            return 1900 <= value <= 2100
        if not isinstance(value, str):
            return False
        stripped = value.strip()
        if not stripped:
            return False
        if stripped.isdigit() and len(stripped) == 4:
            return True
        return any(token in stripped for token in ("-", "/", "年", "月", "周", "学年", "学期"))

    def _label_for_column(self, columns: list[PortalTableColumnPayload], key: str) -> str:
        for column in columns:
            if column.key == key:
                return column.label
        return key

    def _humanize_column(self, value: str) -> str:
        return value.replace("_", " ").strip()

    def _is_numeric(self, value: Any) -> bool:
        return value is None or isinstance(value, (int, float))

    def _is_numeric_column(self, rows: list[dict[str, Any]], key: str) -> bool:
        seen_numeric = False
        for row in rows:
            value = row.get(key)
            if value is None:
                continue
            if isinstance(value, bool) or not isinstance(value, (int, float)):
                return False
            seen_numeric = True
        return seen_numeric

    def _to_number(self, value: Any) -> float:
        if isinstance(value, bool):
            return float(int(value))
        if isinstance(value, (int, float)):
            return float(value)
        if isinstance(value, str) and value.strip():
            try:
                return float(value.strip())
            except ValueError:
                return 0.0
        return 0.0

    def _format_category_value(self, value: Any) -> str:
        if value is None:
            return "未标注"
        return str(value)

    def _unique_in_order(self, values: Any) -> list[str]:
        result: list[str] = []
        seen: set[str] = set()
        for value in values:
            if value in seen:
                continue
            seen.add(value)
            result.append(value)
        return result

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
