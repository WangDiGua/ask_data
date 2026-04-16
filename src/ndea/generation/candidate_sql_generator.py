from __future__ import annotations

from typing import Any
from uuid import uuid4

from ndea.planning.core_registry import field_by_id, get_core_table
from ndea.query_v2 import PlanCandidate, QueryIR, SQLCandidate


class CandidateSQLGenerator:
    def __init__(self, nl2sql_engine: Any | None = None, sql_case_retriever: Any | None = None) -> None:
        self._nl2sql_engine = nl2sql_engine
        self._sql_case_retriever = sql_case_retriever

    def generate(self, query_text: str, ir: QueryIR, plans: list[PlanCandidate]) -> list[SQLCandidate]:
        candidates: list[SQLCandidate] = []
        for plan in plans:
            generated_by_engine = self._generate_with_engine(query_text, plan)
            if generated_by_engine:
                candidates.append(
                    SQLCandidate(
                        candidate_id=uuid4().hex,
                        plan_candidate_id=plan.candidate_id,
                        source=plan.source,
                        sql=generated_by_engine,
                        reasoning="NL2SQL engine generated SQL candidate",
                        model_name=self._model_name(),
                        score=min(0.99, max(plan.confidence, 0.55)),
                    )
                )
            if plan.base_table:
                structured_sql = self._generate_structured_sql(plan)
                if structured_sql:
                    candidates.append(
                        SQLCandidate(
                            candidate_id=uuid4().hex,
                            plan_candidate_id=plan.candidate_id,
                            source=plan.source,
                            sql=structured_sql,
                            reasoning=plan.reasoning,
                            model_name=self._model_name(),
                            score=plan.confidence,
                        )
                    )

            historical_sql = self._historical_sql(query_text, plan)
            if historical_sql:
                candidates.append(
                    SQLCandidate(
                        candidate_id=uuid4().hex,
                        plan_candidate_id=plan.candidate_id,
                        source="historical-case",
                        sql=historical_sql,
                        reasoning="Historical successful SQL case rewrite",
                        model_name=self._model_name(),
                        score=max(0.2, plan.confidence - 0.05),
                    )
                )

            template_sql = self._template_sql(plan, ir)
            if template_sql:
                candidates.append(
                    SQLCandidate(
                        candidate_id=uuid4().hex,
                        plan_candidate_id=plan.candidate_id,
                        source="template-fallback",
                        sql=template_sql,
                        reasoning="Template fallback SQL candidate",
                        model_name=self._model_name(),
                        score=max(0.15, plan.confidence - 0.1),
                    )
                )

        return candidates

    def _generate_structured_sql(self, plan: PlanCandidate) -> str | None:
        if plan.base_table is None:
            return None
        if plan.answer_mode in {"detail", "roster", "record"}:
            select_clause = self._detail_select_clause(plan)
            sql_parts = [f"SELECT {select_clause}", f"FROM {plan.base_table}"]
            sql_parts.extend(join["join_sql"] for join in plan.joins if join.get("join_sql"))
            if plan.filters:
                sql_parts.append(f"WHERE {' AND '.join(plan.filters)}")
            if plan.sort:
                sql_parts.append(f"ORDER BY {', '.join(plan.sort)}")
            else:
                default_sort = self._default_sort(plan.base_table)
                if default_sort:
                    sql_parts.append(f"ORDER BY {', '.join(default_sort)}")
            sql_parts.append(f"LIMIT {max(1, min(200, plan.limit or 50))}")
            return " ".join(sql_parts)

        select_parts = []
        group_parts = []
        for dimension in plan.dimensions:
            alias = dimension.get("output_alias") or dimension["dimension_id"]
            select_parts.append(f"{dimension['expression']} AS {alias}")
            group_parts.append(dimension["expression"])
        select_parts.append(f"{plan.measure_expression} AS total")

        sql_parts = [f"SELECT {', '.join(select_parts)}", f"FROM {plan.base_table}"]
        sql_parts.extend(join["join_sql"] for join in plan.joins if join.get("join_sql"))
        where_clauses = list(dict.fromkeys(plan.filters))
        if where_clauses:
            sql_parts.append(f"WHERE {' AND '.join(where_clauses)}")
        if group_parts:
            sql_parts.append(f"GROUP BY {', '.join(group_parts)}")
        if plan.sort:
            sql_parts.append(f"ORDER BY {', '.join(plan.sort)}")
        elif group_parts:
            sql_parts.append("ORDER BY total DESC")
        if plan.limit is not None:
            sql_parts.append(f"LIMIT {max(1, min(200, plan.limit))}")
        return " ".join(sql_parts)

    def _detail_select_clause(self, plan: PlanCandidate) -> str:
        if plan.dimensions:
            return ", ".join(
                f"{item['expression']} AS {item.get('output_alias') or item['dimension_id']}" for item in plan.dimensions
            )
        table = get_core_table(plan.base_table)
        if table is None or not table.default_projection:
            return "*"
        available_tables = {plan.base_table, *[join["right_table"] for join in plan.joins]}
        projections: list[str] = []
        for field_id in table.default_projection:
            field = field_by_id(table, field_id)
            if field is None or field.table not in available_tables:
                continue
            projections.append(f"{field.expression} AS {field.output_alias}")
        return ", ".join(projections) if projections else "*"

    def _default_sort(self, base_table: str) -> tuple[str, ...]:
        table = get_core_table(base_table)
        if table is None:
            return ()
        return table.default_sort

    def _historical_sql(self, query_text: str, plan: PlanCandidate) -> str | None:
        if self._sql_case_retriever is None:
            return None
        return self._sql_case_retriever.retrieve(query_text=query_text, plan=plan)

    def _generate_with_engine(self, query_text: str, plan: PlanCandidate) -> str | None:
        if self._nl2sql_engine is None or not hasattr(self._nl2sql_engine, "generate"):
            return None
        try:
            sql = self._nl2sql_engine.generate(query_text=query_text, plan=plan)
        except Exception:
            return None
        return sql if isinstance(sql, str) and sql.strip() else None

    def _template_sql(self, plan: PlanCandidate, ir: QueryIR) -> str | None:
        if plan.base_table is None:
            return None
        if ir.intent_type in {"metric", "ranking", "trend"}:
            return f"SELECT COUNT(*) AS total FROM {plan.base_table}"
        if ir.intent_type == "detail":
            return f"SELECT {self._detail_select_clause(plan)} FROM {plan.base_table} LIMIT {max(1, min(200, plan.limit or 50))}"
        return None

    def _model_name(self) -> str:
        if self._nl2sql_engine is None:
            return "heuristic"
        return getattr(self._nl2sql_engine, "model_name", "llamaindex")
