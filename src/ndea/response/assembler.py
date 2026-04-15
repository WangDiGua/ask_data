from typing import Any

from pydantic import BaseModel

from ndea.planning.models import QueryPlanPayload
from ndea.protocol import ChartPayload, TablePayload, TextPayload


class AssembledResponsePayload(BaseModel):
    text: TextPayload
    table: TablePayload | None = None
    chart: ChartPayload | None = None


class ResponseAssemblerService:
    def assemble(
        self,
        plan: QueryPlanPayload,
        execution: dict[str, Any] | None,
    ) -> AssembledResponsePayload:
        if execution is None:
            return AssembledResponsePayload(
                text=TextPayload(summary=plan.clarification_reason or plan.summary),
                table=None,
                chart=None,
            )

        if not bool(execution.get("allowed", False)):
            summary_text = self._read_summary(execution) or plan.summary
            return AssembledResponsePayload(
                text=TextPayload(summary=summary_text),
                table=None,
                chart=None,
            )

        text = self._build_text(execution, plan)
        table = self._build_table(execution)
        chart = self._build_chart(plan, table)
        return AssembledResponsePayload(text=text, table=table, chart=chart)

    def _build_text(self, execution: dict[str, Any], plan: QueryPlanPayload) -> TextPayload:
        if plan.intent_type == "attribute_lookup":
            attribute_text = self._build_attribute_lookup_text(execution, plan)
            if attribute_text is not None:
                return attribute_text
        summary = execution.get("summary")
        if isinstance(summary, dict):
            return TextPayload.model_validate(summary)
        return TextPayload(summary=plan.summary)

    def _build_table(self, execution: dict[str, Any]) -> TablePayload | None:
        table = execution.get("table")
        if isinstance(table, dict):
            return TablePayload.model_validate(table)
        return None

    def _build_chart(
        self,
        plan: QueryPlanPayload,
        table: TablePayload | None,
    ) -> ChartPayload | None:
        if table is None or len(table.columns) < 2 or len(table.rows) < 2:
            return None

        chart_type = self._chart_type_for_intent(plan.intent_type)
        if chart_type is None:
            return None

        x_key = table.columns[0]
        y_key = table.columns[1]
        y_values: list[float] = []
        for row in table.rows:
            value = row.get(y_key)
            if not isinstance(value, (int, float)):
                return None
            y_values.append(float(value))

        option = {
            "xAxis": {"type": "category", "data": [row.get(x_key) for row in table.rows]},
            "yAxis": {"type": "value"},
            "series": [{"type": chart_type, "data": y_values}],
        }
        return ChartPayload(
            title=plan.query_text,
            option=option,
            source=table.rows,
            description=f"Suggested {chart_type} chart for {plan.intent_type} query",
        )

    def _chart_type_for_intent(self, intent_type: str) -> str | None:
        if intent_type == "trend":
            return "line"
        if intent_type in {"ranking", "comparison"}:
            return "bar"
        return None

    def _read_summary(self, execution: dict[str, Any]) -> str | None:
        summary = execution.get("summary")
        if isinstance(summary, dict):
            value = summary.get("summary")
            if isinstance(value, str):
                return value
        reason = execution.get("reason")
        if isinstance(reason, str):
            return reason
        return None

    def _build_attribute_lookup_text(
        self,
        execution: dict[str, Any],
        plan: QueryPlanPayload,
    ) -> TextPayload | None:
        table = execution.get("table")
        if not isinstance(table, dict):
            return None
        rows = table.get("rows")
        if not isinstance(rows, list) or not rows:
            return None
        row = rows[0]
        if not isinstance(row, dict):
            return None

        identifier = plan.lookup_identifier
        if identifier is None:
            return None

        if len(plan.lookup_attributes) == 1:
            attribute = plan.lookup_attributes[0]
            alias = attribute.output_alias or attribute.dimension_id
            value = row.get(alias)
            if value is not None:
                return TextPayload(summary=f"{identifier.label}{identifier.value}的{attribute.name}是{value}")

        details = "；".join(
            f"{attribute.name}={row.get(attribute.output_alias or attribute.dimension_id)}"
            for attribute in plan.lookup_attributes
        )
        return TextPayload(summary=f"{identifier.label}{identifier.value}的查询结果如下", details=details or None)
