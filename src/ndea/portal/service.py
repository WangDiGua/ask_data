from __future__ import annotations

import json
import math
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


TIME_COLUMN_TOKENS = (
    "date",
    "time",
    "day",
    "week",
    "month",
    "year",
    "日期",
    "时间",
    "天",
    "周",
    "月",
    "年",
    "学年",
)
PIE_QUERY_TOKENS = ("占比", "比例", "构成", "结构", "分布", "占所有", "占总体")
STACKED_QUERY_TOKENS = ("构成", "占比", "比例", "分布", "结构")
RANKING_QUERY_TOKENS = ("排名", "前", "top", "最多", "最高", "最低")


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
            clarification_question=(
                payload.clarification_questions[0]
                if payload.clarification_questions
                else None
            ),
            executed=payload.executed,
            sql=self._resolve_sql(payload),
            metadata=PortalQueryMetadataPayload(
                tool_trace=list(payload.tool_trace),
                confidence=payload.plan.confidence,
                selected_sql_asset_id=payload.plan.selected_sql_asset_id,
                metric_id=payload.plan.metric_id,
                answer_mode=self._answer_mode(payload),
                resolved_tables=list(payload.plan.candidate_tables),
                resolved_entities=list(payload.plan.resolved_entities),
                sql_strategy=self._sql_strategy(payload),
                clarification_reason=payload.plan.clarification_reason,
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
        label_map = self._column_label_map(payload)
        return PortalTablePayload(
            columns=[
                PortalTableColumnPayload(
                    key=column,
                    label=label_map.get(column, self._humanize_column(column)),
                )
                for column in table.columns
            ],
            rows=[dict(row) for row in table.rows],
        )

    def _build_visualization(
        self,
        payload: QueryWorkflowPayload,
        table: PortalTablePayload | None,
    ) -> dict[str, Any] | None:
        if self._answer_mode(payload) not in {"aggregate", "trend", "ranking", "comparison", "metric"}:
            return None
        if table is None or len(table.rows) < 2 or len(table.columns) < 2:
            return None

        schema = self._infer_table_schema(table.rows, table.columns)
        if not schema["numeric_keys"]:
            return None

        chart_kind = self._select_chart_kind(payload, schema, table.rows)
        option = self._build_echarts_option(chart_kind, table, schema)
        if option is None:
            return None

        legend_enabled = (
            len(schema["numeric_keys"]) > 1
            or chart_kind in {"pie", "heatmap"}
        )

        return {
            "type": "visualization",
            "version": "1.0",
            "renderer": "echarts",
            "title": payload.query_text,
            "description": (
                payload.response_chart.description
                if payload.response_chart is not None
                else payload.response_text.summary
            ),
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
                        "type": (
                            "number"
                            if column.key in schema["numeric_keys"]
                            else "string"
                        ),
                        "label": column.label,
                    }
                    for column in table.columns
                ],
            },
            "style": {
                "theme": "light",
                "width": "100%",
                "height": 420,
                "responsive": True,
            },
            "interaction": {
                "tooltip": True,
                "legend": legend_enabled,
                "dataZoom": chart_kind in {"line", "line-area"},
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
    ) -> dict[str, Any]:
        numeric_keys = [
            column.key
            for column in columns
            if all(self._is_numeric(row.get(column.key)) for row in rows)
        ]
        categorical_keys = [
            column.key for column in columns if column.key not in numeric_keys
        ]

        time_key = next(
            (
                key
                for key in categorical_keys
                if self._looks_like_time_dimension(key, rows)
            ),
            None,
        )
        primary_category_key = (
            time_key
            or (categorical_keys[0] if categorical_keys else None)
        )

        return {
            "numeric_keys": numeric_keys,
            "categorical_keys": categorical_keys,
            "primary_category_key": primary_category_key,
            "secondary_category_key": (
                categorical_keys[1] if len(categorical_keys) > 1 else None
            ),
            "time_key": time_key,
        }

    def _select_chart_kind(
        self,
        payload: QueryWorkflowPayload,
        schema: dict[str, Any],
        rows: list[dict[str, Any]],
    ) -> str:
        numeric_keys = schema["numeric_keys"]
        categorical_keys = schema["categorical_keys"]
        primary_category_key = schema["primary_category_key"]

        if (
            schema["time_key"] is not None
            or payload.plan.intent_type == "trend"
            or payload.plan.time_grain
        ):
            return "line-area" if len(numeric_keys) == 1 else "line"

        if len(categorical_keys) >= 2 and len(numeric_keys) == 1:
            return "heatmap"

        if (
            len(numeric_keys) == 1
            and len(rows) <= 12
            and self._is_proportion_query(payload.query_text)
        ):
            return "pie"

        if len(numeric_keys) > 1:
            if self._is_stacked_query(payload.query_text):
                return "bar-stacked"
            return "bar-grouped"

        if primary_category_key and self._prefer_horizontal_bar(
            payload.query_text,
            rows,
            primary_category_key,
        ):
            return "bar-horizontal"

        return "bar"

    def _build_echarts_option(
        self,
        chart_kind: str,
        table: PortalTablePayload,
        schema: dict[str, Any],
    ) -> dict[str, Any] | None:
        if chart_kind == "heatmap":
            return self._build_heatmap_option(table, schema)
        if chart_kind == "pie":
            return self._build_pie_option(table, schema)
        return self._build_cartesian_option(chart_kind, table, schema)

    def _build_pie_option(
        self,
        table: PortalTablePayload,
        schema: dict[str, Any],
    ) -> dict[str, Any] | None:
        category_key = schema["primary_category_key"]
        numeric_keys = schema["numeric_keys"]
        if category_key is None or len(numeric_keys) != 1:
            return None

        series_key = numeric_keys[0]
        category_label = self._label_for_column(table.columns, category_key)
        value_label = self._label_for_column(table.columns, series_key)
        return {
            "tooltip": {"trigger": "item"},
            "legend": {"bottom": 0},
            "series": [
                {
                    "type": "pie",
                    "radius": ["38%", "70%"],
                    "avoidLabelOverlap": True,
                    "data": [
                        {
                            "name": self._format_category_value(row.get(category_key)),
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

    def _build_heatmap_option(
        self,
        table: PortalTablePayload,
        schema: dict[str, Any],
    ) -> dict[str, Any] | None:
        categorical_keys = schema["categorical_keys"]
        numeric_keys = schema["numeric_keys"]
        if len(categorical_keys) < 2 or len(numeric_keys) != 1:
            return None

        x_key = categorical_keys[0]
        y_key = categorical_keys[1]
        value_key = numeric_keys[0]
        x_values = self._unique_in_order(
            self._format_category_value(row.get(x_key)) for row in table.rows
        )
        y_values = self._unique_in_order(
            self._format_category_value(row.get(y_key)) for row in table.rows
        )
        if not x_values or not y_values:
            return None

        x_index = {value: idx for idx, value in enumerate(x_values)}
        y_index = {value: idx for idx, value in enumerate(y_values)}
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
            "tooltip": {"position": "top"},
            "grid": {"left": 96, "right": 24, "top": 48, "bottom": 36},
            "xAxis": {
                "type": "category",
                "data": x_values,
                "splitArea": {"show": True},
            },
            "yAxis": {
                "type": "category",
                "data": y_values,
                "splitArea": {"show": True},
            },
            "visualMap": {
                "min": 0,
                "max": math.ceil(max_value) if max_value > 0 else 1,
                "calculable": True,
                "orient": "horizontal",
                "left": "center",
                "bottom": 0,
            },
            "series": [
                {
                    "name": self._label_for_column(table.columns, value_key),
                    "type": "heatmap",
                    "data": points,
                    "label": {"show": True},
                    "emphasis": {
                        "itemStyle": {
                            "shadowBlur": 8,
                            "shadowColor": "rgba(0,0,0,0.18)",
                        }
                    },
                }
            ],
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

        x_axis_data = [
            self._format_category_value(row.get(category_key)) for row in table.rows
        ]
        horizontal = chart_kind == "bar-horizontal"
        series_type = "line" if chart_kind in {"line", "line-area"} else "bar"
        stacked = chart_kind == "bar-stacked"
        area_style = {"opacity": 0.18} if chart_kind == "line-area" else None

        series = []
        for index, series_key in enumerate(numeric_keys):
            entry = {
                "name": self._label_for_column(table.columns, series_key),
                "type": series_type,
                "data": [self._to_number(row.get(series_key)) for row in table.rows],
                "emphasis": {"focus": "series"},
            }
            if series_type == "line":
                entry["smooth"] = True
                if area_style is not None:
                    entry["areaStyle"] = area_style
            if stacked:
                entry["stack"] = "total"
            if horizontal and index == 0:
                entry["barMaxWidth"] = 28
            series.append(entry)

        option: dict[str, Any] = {
            "tooltip": {"trigger": "axis"},
            "legend": {"top": 0},
            "grid": {
                "left": 24,
                "right": 24,
                "top": 48,
                "bottom": 24,
                "containLabel": True,
            },
            "series": series,
        }

        if horizontal:
            option["xAxis"] = {"type": "value"}
            option["yAxis"] = {
                "type": "category",
                "data": x_axis_data,
                "axisLabel": {"width": 160, "overflow": "truncate"},
            }
            option["grid"]["left"] = 96
        else:
            option["xAxis"] = {
                "type": "category",
                "data": x_axis_data,
                "axisLabel": {"interval": 0, "rotate": 28 if self._has_long_labels(x_axis_data) else 0},
            }
            option["yAxis"] = {"type": "value"}

        return option

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

    def _answer_mode(self, payload: QueryWorkflowPayload) -> str:
        if payload.clarification_required:
            return "clarification"
        if payload.plan.answer_mode:
            return payload.plan.answer_mode
        intent_type = payload.plan.intent_type
        if intent_type in {"attribute_lookup", "record_lookup", "roster", "detail"}:
            return intent_type
        if intent_type in {"trend", "ranking", "comparison", "metric"}:
            return "aggregate"
        return intent_type

    def _sql_strategy(self, payload: QueryWorkflowPayload) -> str | None:
        generation = payload.generation
        if generation is not None and getattr(generation, "strategy", None):
            return str(getattr(generation, "strategy"))
        if payload.plan.chosen_strategy:
            return payload.plan.chosen_strategy
        return None

    def _looks_like_time_dimension(
        self,
        key: str,
        rows: list[dict[str, Any]],
    ) -> bool:
        lowered_key = key.lower()
        if any(token in lowered_key for token in TIME_COLUMN_TOKENS):
            return True

        values = [row.get(key) for row in rows[:8]]
        return all(self._looks_like_time_value(value) for value in values if value is not None)

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
        return any(token in stripped for token in ("-", "/", "年", "月", "周"))

    def _prefer_horizontal_bar(
        self,
        query_text: str,
        rows: list[dict[str, Any]],
        category_key: str,
    ) -> bool:
        labels = [self._format_category_value(row.get(category_key)) for row in rows]
        if any(token in query_text.lower() for token in RANKING_QUERY_TOKENS):
            return True
        return len(rows) > 6 or self._has_long_labels(labels)

    def _has_long_labels(self, labels: list[str]) -> bool:
        return any(len(label) > 10 for label in labels)

    def _is_proportion_query(self, query_text: str) -> bool:
        return any(token in query_text for token in PIE_QUERY_TOKENS)

    def _is_stacked_query(self, query_text: str) -> bool:
        return any(token in query_text for token in STACKED_QUERY_TOKENS)

    def _format_category_value(self, value: Any) -> str:
        if value is None:
            return "未标注"
        return str(value)

    def _humanize_column(self, value: str) -> str:
        if not value:
            return value
        return value.replace("_", " ").strip()

    def _label_for_column(
        self,
        columns: list[PortalTableColumnPayload],
        key: str,
    ) -> str:
        for column in columns:
            if column.key == key:
                return column.label
        return key

    def _column_label_map(self, payload: QueryWorkflowPayload) -> dict[str, str]:
        labels: dict[str, str] = {}
        for field in payload.plan.lookup_attributes:
            labels[field.output_alias or field.dimension_id] = field.name
        for field in payload.plan.dimensions:
            labels[field.output_alias or field.dimension_id] = field.name
        if payload.response_table is not None:
            for column in payload.response_table.columns:
                labels.setdefault(column, self._humanize_column(column))
        return labels

    def _is_numeric(self, value: Any) -> bool:
        if value is None:
            return True
        return isinstance(value, (int, float))

    def _to_number(self, value: Any) -> float:
        if isinstance(value, bool):
            return float(int(value))
        if isinstance(value, (int, float)):
            return float(value)
        if isinstance(value, str):
            stripped = value.strip()
            if stripped:
                try:
                    parsed = float(stripped)
                except ValueError:
                    return 0.0
                if not math.isnan(parsed):
                    return parsed
        return 0.0

    def _unique_in_order(self, values: Any) -> list[str]:
        ordered: list[str] = []
        seen: set[str] = set()
        for value in values:
            if value in seen:
                continue
            seen.add(value)
            ordered.append(value)
        return ordered

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
