from __future__ import annotations

from ndea.config import Settings
from ndea.planning.models import QueryPlanPayload
from ndea.sql_advisor.models import SQLAdvisoryPayload, SQLAdvisorExample


class VannaStyleSQLAdvisorService:
    def __init__(
        self,
        settings: Settings,
        sql_rag=None,
        exemplar_limit: int = 3,
    ) -> None:
        self._settings = settings
        self._sql_rag = sql_rag
        self._exemplar_limit = max(1, exemplar_limit)

    def advise(
        self,
        query_text: str,
        plan: QueryPlanPayload,
        query_vector: list[float] | None = None,
    ) -> SQLAdvisoryPayload:
        examples = self._examples_from_plan(plan)
        selected_sql = plan.selected_sql
        selected_sql_asset_id = plan.selected_sql_asset_id
        confidence = plan.confidence
        strategy = None

        if examples:
            strategy = "vanna_style_ranked_examples"
            if selected_sql is None:
                top_example = examples[0]
                selected_sql = top_example.sql
                selected_sql_asset_id = top_example.asset_id
                confidence = top_example.score

        if (selected_sql is None or not examples) and self._sql_rag is not None and query_vector is not None:
            rag_payload = self._sql_rag.retrieve(
                query_text=query_text,
                query_vector=query_vector,
                limit=self._exemplar_limit,
            )
            rag_examples = [
                SQLAdvisorExample(
                    asset_id=candidate.asset_id,
                    question=candidate.question,
                    sql=candidate.sql,
                    score=candidate.hybrid_score or candidate.score,
                    metadata=candidate.metadata,
                )
                for candidate in rag_payload.candidates
            ]
            if rag_examples:
                examples = rag_examples
                strategy = "vanna_style_retrieval"
                if selected_sql is None:
                    selected_sql = rag_examples[0].sql
                    selected_sql_asset_id = rag_examples[0].asset_id
                    confidence = rag_examples[0].score

        if selected_sql is not None and strategy is None:
            strategy = "planner_selected_candidate"
        if confidence is None and selected_sql is not None:
            confidence = 0.85

        notes: list[str] = []
        if examples:
            notes.append(f"Guided by {len(examples)} SQL exemplars")
        if plan.clarification_required:
            notes.append("Clarification required before SQL execution")

        return SQLAdvisoryPayload(
            selected_sql=selected_sql,
            selected_sql_asset_id=selected_sql_asset_id,
            strategy=strategy,
            confidence=confidence,
            examples=examples,
            notes=notes,
        )

    def _examples_from_plan(self, plan: QueryPlanPayload) -> list[SQLAdvisorExample]:
        return [
            SQLAdvisorExample(
                asset_id=candidate.asset_id,
                sql=candidate.sql,
                score=candidate.compatibility_score,
                metadata={"selection_reason": candidate.selection_reason},
            )
            for candidate in plan.ranked_sql_candidates[: self._exemplar_limit]
        ]
