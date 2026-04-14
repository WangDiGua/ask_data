from pydantic import BaseModel

from ndea.planning.models import QueryPlanPayload


class SQLGenerationPayload(BaseModel):
    generated: bool
    sql: str | None = None
    strategy: str | None = None
    reason: str | None = None


class SQLGeneratorService:
    def generate(self, plan: QueryPlanPayload) -> SQLGenerationPayload:
        if plan.selected_sql:
            return SQLGenerationPayload(
                generated=True,
                sql=plan.selected_sql,
                strategy="rag_candidate",
                reason=None,
            )

        if plan.clarification_required:
            return SQLGenerationPayload(
                generated=False,
                sql=None,
                strategy=None,
                reason="Clarification required before SQL generation",
            )

        if plan.resolved_metric is not None and plan.metric_id is not None:
            return self._generate_structured_metric_sql(plan)

        if not plan.candidate_tables:
            return SQLGenerationPayload(
                generated=False,
                sql=None,
                strategy=None,
                reason="No candidate tables available for SQL generation",
        )

        return self.generate_for_table(plan, plan.candidate_tables[0])

    def _generate_structured_metric_sql(
        self,
        plan: QueryPlanPayload,
    ) -> SQLGenerationPayload:
        metric = plan.resolved_metric
        if metric is None:
            return SQLGenerationPayload(
                generated=False,
                sql=None,
                strategy=None,
                reason="Structured metric plan is missing resolved metric payload",
            )

        select_parts: list[str] = []
        group_parts: list[str] = []
        for dimension in plan.dimensions:
            alias = dimension.output_alias or dimension.dimension_id
            select_parts.append(f"{dimension.expression} AS {alias}")
            group_parts.append(dimension.expression)

        select_parts.append(f"{metric.measure_expression} AS total")
        sql_parts = [f"SELECT {', '.join(select_parts)}", f"FROM {metric.base_table}"]
        sql_parts.extend(step.join_sql for step in plan.join_plan)

        where_clauses = [filter_payload.expression for filter_payload in plan.filters]
        if plan.time_scope is not None and plan.time_scope.field and plan.time_scope.value is not None:
            where_clauses.append(f"{plan.time_scope.field} = '{plan.time_scope.value}'")
        elif (
            plan.time_scope is not None
            and plan.time_scope.field
            and plan.time_scope.start is not None
            and plan.time_scope.end is not None
        ):
            where_clauses.append(
                f"{plan.time_scope.field} BETWEEN '{plan.time_scope.start}' AND '{plan.time_scope.end}'"
            )

        if where_clauses:
            sql_parts.append(f"WHERE {' AND '.join(where_clauses)}")
        if group_parts:
            sql_parts.append(f"GROUP BY {', '.join(group_parts)}")
            sql_parts.append("ORDER BY total DESC")

        return SQLGenerationPayload(
            generated=True,
            sql=" ".join(sql_parts),
            strategy="structured_metric_contract",
            reason=None,
        )

    def generate_for_table(
        self,
        plan: QueryPlanPayload,
        table: str,
    ) -> SQLGenerationPayload:
        if plan.clarification_required:
            return SQLGenerationPayload(
                generated=False,
                sql=None,
                strategy=None,
                reason="Clarification required before SQL generation",
            )

        if plan.intent_type == "metric":
            return SQLGenerationPayload(
                generated=True,
                sql=f"SELECT COUNT(*) AS total FROM {table}",
                strategy="count_metric",
                reason=None,
            )

        if plan.intent_type == "detail":
            return SQLGenerationPayload(
                generated=True,
                sql=f"SELECT * FROM {table}",
                strategy="detail_scan",
                reason=None,
            )

        if plan.intent_type == "ranking":
            return SQLGenerationPayload(
                generated=True,
                sql=f"SELECT * FROM {table} LIMIT 10",
                strategy="ranking_preview",
                reason=None,
            )

        return SQLGenerationPayload(
            generated=False,
            sql=None,
            strategy=None,
            reason="Planner needs richer time semantics for trend/comparison SQL generation",
        )
