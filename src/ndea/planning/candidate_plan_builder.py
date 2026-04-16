from __future__ import annotations

from uuid import uuid4

from ndea.query_v2 import PlanCandidate, QueryIR, SchemaHint, SemanticHint


class CandidatePlanBuilder:
    def build(self, ir: QueryIR, semantic_hint: SemanticHint, schema_hint: SchemaHint) -> list[PlanCandidate]:
        candidates: list[PlanCandidate] = []
        if semantic_hint.base_table is not None:
            candidates.append(
                PlanCandidate(
                    candidate_id=uuid4().hex,
                    intent_type=ir.intent_type,
                    answer_mode=ir.answer_mode,
                    source="semantic-first",
                    base_table=semantic_hint.base_table,
                    measure_expression=semantic_hint.measure_expression,
                    entity_scope=semantic_hint.entity_scope,
                    candidate_tables=list(dict.fromkeys(semantic_hint.candidate_tables)),
                    dimensions=semantic_hint.dimensions,
                    filters=list(dict.fromkeys(semantic_hint.filters)),
                    joins=semantic_hint.joins,
                    time_scope=ir.time_scope,
                    sort=ir.sort,
                    limit=ir.limit,
                    ambiguities=list(dict.fromkeys([*ir.ambiguities, *semantic_hint.ambiguities])),
                    semantic_score=semantic_hint.confidence,
                    schema_score=schema_hint.confidence,
                    confidence=round((semantic_hint.confidence * 0.75) + (schema_hint.confidence * 0.25), 2),
                    reasoning="Campus semantic resolver matched canonical campus entities",
                    requires_clarification=bool(ir.ambiguities),
                    clarification_question=self._clarification_question(ir),
                )
            )

        if schema_hint.base_table is not None:
            candidates.append(
                PlanCandidate(
                    candidate_id=uuid4().hex,
                    intent_type=ir.intent_type,
                    answer_mode=ir.answer_mode,
                    source="schema-first",
                    base_table=schema_hint.base_table,
                    entity_scope=ir.entity_scope,
                    candidate_tables=list(dict.fromkeys(schema_hint.candidate_tables)),
                    dimensions=schema_hint.dimensions,
                    filters=list(dict.fromkeys(schema_hint.filters)),
                    joins=schema_hint.joins,
                    time_scope=ir.time_scope,
                    sort=ir.sort,
                    limit=ir.limit,
                    ambiguities=list(ir.ambiguities),
                    semantic_score=semantic_hint.confidence,
                    schema_score=schema_hint.confidence,
                    confidence=round((schema_hint.confidence * 0.7) + (semantic_hint.confidence * 0.3), 2),
                    reasoning="Schema resolver inferred likely tables and columns from live metadata",
                    requires_clarification=bool(ir.ambiguities),
                    clarification_question=self._clarification_question(ir),
                )
            )

        if not candidates:
            candidates.append(
                PlanCandidate(
                    candidate_id=uuid4().hex,
                    intent_type=ir.intent_type,
                    answer_mode=ir.answer_mode,
                    source="template-fallback",
                    base_table=None,
                    candidate_tables=[],
                    dimensions=[],
                    filters=[],
                    joins=[],
                    time_scope=ir.time_scope,
                    sort=ir.sort,
                    limit=ir.limit,
                    ambiguities=list(ir.ambiguities),
                    semantic_score=semantic_hint.confidence,
                    schema_score=schema_hint.confidence,
                    confidence=round(max(semantic_hint.confidence, schema_hint.confidence, 0.15), 2),
                    reasoning="No high-confidence semantic or schema match; fallback candidate only",
                    requires_clarification=True,
                    clarification_question=self._clarification_question(ir) or "请明确统计对象、时间范围和业务口径。",
                )
            )
        return candidates

    def _clarification_question(self, ir: QueryIR) -> str | None:
        if "entity_scope_required" in ir.ambiguities:
            return "你想查询学生、教职工，还是全校在册人员？"
        if ir.ambiguities:
            return "请补充统计对象、时间范围或业务口径。"
        return None
