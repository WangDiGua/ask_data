from __future__ import annotations

from collections.abc import Iterator
from typing import Any, TypedDict
from uuid import uuid4

from ndea.runtime import configure_runtime

configure_runtime()

from langgraph.graph import END, START, StateGraph

from ndea.adapters import LangfuseTracer
from ndea.execution import QueryExecutorV2
from ndea.generation import CandidateSQLGenerator
from ndea.interaction import InteractionService
from ndea.learning import LearningStore, MilvusLearningSync
from ndea.planning.candidate_plan_builder import CandidatePlanBuilder
from ndea.protocol import ChartPayload, TablePayload, TextPayload
from ndea.query_v2 import (
    ClarificationPayload,
    PlanCandidate,
    QueryInterpretationPayload,
    QueryRequestV2,
    QueryResponseV2,
    RankingDecision,
    SQLCandidate,
    VerificationReport,
)
from ndea.ranking import CandidateRanker
from ndea.resolution import SchemaResolver
from ndea.semantic.campus_semantic_resolver import CampusSemanticResolver
from ndea.understanding import IntentParser
from ndea.verification import SQLVerifier


class QueryV2State(TypedDict, total=False):
    request: QueryRequestV2
    session_id: str
    trace: dict[str, Any]
    tool_trace: list[str]
    interaction: Any
    ir: Any
    semantic_hint: Any
    schema_hint: Any
    plans: list[PlanCandidate]
    sql_candidates: list[SQLCandidate]
    verification_reports: list[VerificationReport]
    ranking: RankingDecision
    execution: dict[str, Any] | None
    response: QueryResponseV2


class QueryGraphV2:
    def __init__(
        self,
        interaction_service: InteractionService,
        intent_parser: IntentParser,
        semantic_resolver: CampusSemanticResolver,
        schema_resolver: SchemaResolver,
        plan_builder: CandidatePlanBuilder,
        sql_generator: CandidateSQLGenerator,
        sql_verifier: SQLVerifier,
        ranker: CandidateRanker,
        executor: QueryExecutorV2,
        learning_store: LearningStore | None,
        milvus_sync: MilvusLearningSync | None,
        tracer: LangfuseTracer,
    ) -> None:
        self._interaction_service = interaction_service
        self._intent_parser = intent_parser
        self._semantic_resolver = semantic_resolver
        self._schema_resolver = schema_resolver
        self._plan_builder = plan_builder
        self._sql_generator = sql_generator
        self._sql_verifier = sql_verifier
        self._ranker = ranker
        self._executor = executor
        self._learning_store = learning_store
        self._milvus_sync = milvus_sync
        self._tracer = tracer
        self._graph = self._build_graph()

    def run(self, request: QueryRequestV2) -> QueryResponseV2:
        state = self._graph.invoke(self._initial_state(request))
        return state["response"]

    def stream(self, request: QueryRequestV2) -> Iterator[dict[str, Any]]:
        for mode, chunk in self._graph.stream(self._initial_state(request), stream_mode=["updates", "values"]):
            if mode == "updates":
                node_name = next(iter(chunk))
                yield {"type": "node", "node": node_name, "payload": self._serialize(chunk[node_name])}
                continue
            if chunk.get("response") is not None:
                yield {"type": "final", "payload": self._serialize(chunk["response"])}

    def _initial_state(self, request: QueryRequestV2) -> QueryV2State:
        return {
            "request": request,
            "session_id": uuid4().hex,
            "trace": self._tracer.start_trace("query_service_v2", {"query_text": request.query_text}),
            "tool_trace": [],
        }

    def _build_graph(self):
        graph = StateGraph(QueryV2State)
        graph.add_node("interaction", self._interaction_node)
        graph.add_node("intent_parse", self._intent_parse_node)
        graph.add_node("semantic_resolve", self._semantic_resolve_node)
        graph.add_node("schema_resolve", self._schema_resolve_node)
        graph.add_node("build_plan_candidates", self._build_plan_candidates_node)
        graph.add_node("generate_sql_candidates", self._generate_sql_candidates_node)
        graph.add_node("verify_candidates", self._verify_candidates_node)
        graph.add_node("rank_candidates", self._rank_candidates_node)
        graph.add_node("confidence_gate", self._confidence_gate_node)
        graph.add_node("execute", self._execute_node)
        graph.add_node("respond", self._respond_node)
        graph.add_node("learn", self._learn_node)
        graph.add_edge(START, "interaction")
        graph.add_edge("interaction", "intent_parse")
        graph.add_edge("intent_parse", "semantic_resolve")
        graph.add_edge("semantic_resolve", "schema_resolve")
        graph.add_edge("schema_resolve", "build_plan_candidates")
        graph.add_edge("build_plan_candidates", "generate_sql_candidates")
        graph.add_edge("generate_sql_candidates", "verify_candidates")
        graph.add_edge("verify_candidates", "rank_candidates")
        graph.add_edge("rank_candidates", "confidence_gate")
        graph.add_edge("confidence_gate", "execute")
        graph.add_edge("execute", "respond")
        graph.add_edge("respond", "learn")
        graph.add_edge("learn", END)
        return graph.compile()

    def _interaction_node(self, state: QueryV2State) -> QueryV2State:
        request = state["request"]
        interaction = self._interaction_service.process(request.query_text, request.request_context)
        self._tracer.record_node(state["trace"], "interaction", interaction.model_dump(mode="json"))
        return {"interaction": interaction, "tool_trace": [*state["tool_trace"], "interaction"]}

    def _intent_parse_node(self, state: QueryV2State) -> QueryV2State:
        ir = self._intent_parser.parse(state["interaction"].rewritten_query_text)
        self._tracer.record_node(state["trace"], "intent_parse", ir.model_dump(mode="json"))
        return {"ir": ir, "tool_trace": [*state["tool_trace"], "intent_parse"]}

    def _semantic_resolve_node(self, state: QueryV2State) -> QueryV2State:
        semantic_hint = self._semantic_resolver.resolve(state["ir"])
        self._tracer.record_node(state["trace"], "semantic_resolve", semantic_hint.model_dump(mode="json"))
        return {"semantic_hint": semantic_hint, "tool_trace": [*state["tool_trace"], "semantic_resolve"]}

    def _schema_resolve_node(self, state: QueryV2State) -> QueryV2State:
        request = state["request"]
        schema_hint = self._schema_resolver.resolve(
            database=request.database,
            ir=state["ir"],
            query_text=state["interaction"].rewritten_query_text,
        )
        self._tracer.record_node(state["trace"], "schema_resolve", schema_hint.model_dump(mode="json"))
        return {"schema_hint": schema_hint, "tool_trace": [*state["tool_trace"], "schema_resolve"]}

    def _build_plan_candidates_node(self, state: QueryV2State) -> QueryV2State:
        plans = self._plan_builder.build(state["ir"], state["semantic_hint"], state["schema_hint"])
        self._tracer.record_node(
            state["trace"],
            "build_plan_candidates",
            {"plans": [plan.model_dump(mode="json") for plan in plans]},
        )
        return {"plans": plans, "tool_trace": [*state["tool_trace"], "build_plan_candidates"]}

    def _generate_sql_candidates_node(self, state: QueryV2State) -> QueryV2State:
        sql_candidates = self._sql_generator.generate(
            query_text=state["interaction"].rewritten_query_text,
            ir=state["ir"],
            plans=state["plans"],
        )
        self._tracer.record_node(
            state["trace"],
            "generate_sql_candidates",
            {"sql_candidates": [candidate.model_dump(mode="json") for candidate in sql_candidates]},
        )
        return {"sql_candidates": sql_candidates, "tool_trace": [*state["tool_trace"], "generate_sql_candidates"]}

    def _verify_candidates_node(self, state: QueryV2State) -> QueryV2State:
        request = state["request"]
        allowed_tables = set(((request.policy_context or {}).get("allowed_tables") or []))
        plan_map = {plan.candidate_id: plan for plan in state["plans"]}
        reports = [
            self._sql_verifier.verify(
                database=request.database,
                plan=plan_map[candidate.plan_candidate_id],
                candidate=candidate,
                allowed_tables=allowed_tables,
            )
            for candidate in state["sql_candidates"]
            if candidate.plan_candidate_id in plan_map
        ]
        self._tracer.record_node(
            state["trace"],
            "verify_candidates",
            {"verification_reports": [report.model_dump(mode="json") for report in reports]},
        )
        return {"verification_reports": reports, "tool_trace": [*state["tool_trace"], "verify_candidates"]}

    def _rank_candidates_node(self, state: QueryV2State) -> QueryV2State:
        ranking = self._ranker.rank(state["plans"], state["sql_candidates"], state["verification_reports"])
        self._tracer.record_node(state["trace"], "rank_candidates", ranking.model_dump(mode="json"))
        return {"ranking": ranking, "tool_trace": [*state["tool_trace"], "rank_candidates"]}

    def _confidence_gate_node(self, state: QueryV2State) -> QueryV2State:
        self._tracer.record_node(
            state["trace"],
            "confidence_gate",
            {"confidence": state["ranking"].confidence, "reason": state["ranking"].reason},
        )
        return {"tool_trace": [*state["tool_trace"], "confidence_gate"]}

    def _execute_node(self, state: QueryV2State) -> QueryV2State:
        request = state["request"]
        ranking = state["ranking"]
        execution = None
        selected_plan = next((plan for plan in state["plans"] if plan.candidate_id == ranking.selected_plan_candidate_id), None)
        threshold = self._execution_threshold(selected_plan)
        if ranking.confidence >= threshold and ranking.selected_sql_candidate_id is not None:
            sql_map = {candidate.candidate_id: candidate for candidate in state["sql_candidates"]}
            execution = self._executor.execute(
                database=request.database,
                candidate=sql_map.get(ranking.selected_sql_candidate_id),
                request_context=request.request_context,
                policy_context=request.policy_context,
                options=request.options,
            )
        self._tracer.record_node(state["trace"], "execute", {"executed": execution is not None})
        return {"execution": execution, "tool_trace": [*state["tool_trace"], "execute"]}

    def _respond_node(self, state: QueryV2State) -> QueryV2State:
        request = state["request"]
        ranking = state["ranking"]
        selected_plan = next((plan for plan in state["plans"] if plan.candidate_id == ranking.selected_plan_candidate_id), None)
        selected_sql = next((item for item in state["sql_candidates"] if item.candidate_id == ranking.selected_sql_candidate_id), None)
        selected_verification = next(
            (item for item in state["verification_reports"] if item.sql_candidate_id == ranking.selected_sql_candidate_id),
            None,
        )
        clarification = self._clarification_payload(state, ranking, selected_plan)
        answer, table, chart, executed, audit = self._build_answer(state, selected_plan, selected_sql)
        response = QueryResponseV2(
            session_id=state["session_id"],
            interpretation=QueryInterpretationPayload(
                interaction=state["interaction"],
                ir=state["ir"],
                selected_plan=selected_plan,
                selected_sql=selected_sql,
                verification=selected_verification,
                ambiguities=list(state["ir"].ambiguities),
            ),
            answer=answer,
            table=table,
            chart=chart,
            sql=selected_sql.sql if selected_sql is not None else None,
            audit=audit,
            confidence=ranking.confidence,
            clarification=clarification,
            learning_trace_id=f"learn-{state['session_id']}",
            tool_trace=state["tool_trace"] + ["respond"],
            executed=executed,
            debug=self._debug_payload(state) if request.options.debug else {},
        )
        self._tracer.record_node(state["trace"], "respond", response.model_dump(mode="json"))
        return {"response": response, "tool_trace": [*state["tool_trace"], "respond"]}

    def _learn_node(self, state: QueryV2State) -> QueryV2State:
        response = state["response"]
        if self._learning_store is not None:
            events, promotions = self._learning_store.record(response)
            if hasattr(self._learning_store, "persist_response"):
                try:
                    self._learning_store.persist_response(response, events=events, promotions=promotions)
                except Exception as exc:
                    response.debug.setdefault("learning_error", str(exc) or exc.__class__.__name__)
            if self._milvus_sync is not None:
                response.debug.setdefault("learning_sync", self._milvus_sync.sync(promotions))
            response.debug.setdefault("learning_events", [event.model_dump(mode="json") for event in events])
        self._tracer.finish_trace(state["trace"], response.model_dump(mode="json"))
        return {"response": response, "tool_trace": [*state["tool_trace"], "learn"]}

    def _clarification_payload(
        self,
        state: QueryV2State,
        ranking: RankingDecision,
        selected_plan: PlanCandidate | None,
    ) -> ClarificationPayload:
        if (
            ranking.confidence >= self._execution_threshold(selected_plan)
            and selected_plan is not None
            and not selected_plan.requires_clarification
        ):
            return ClarificationPayload(required=False)
        question = selected_plan.clarification_question if selected_plan is not None else None
        if question is None and state["plans"]:
            question = state["plans"][0].clarification_question
        return ClarificationPayload(
            required=True,
            reason=ranking.reason or "Need clarification before execution",
            question=question or "Please clarify the target population, time range, or business definition.",
            options=["student", "faculty", "all_people"] if "entity_scope_required" in state["ir"].ambiguities else [],
        )

    def _build_answer(
        self,
        state: QueryV2State,
        selected_plan: PlanCandidate | None,
        selected_sql: SQLCandidate | None,
    ) -> tuple[TextPayload, TablePayload | None, ChartPayload | None, bool, dict[str, Any]]:
        request = state["request"]
        execution = state.get("execution")
        if execution is None:
            if request.options.dry_run and selected_sql is not None:
                return (
                    TextPayload(summary="Dry run completed. SQL candidate is ready for execution."),
                    None,
                    None,
                    False,
                    {},
                )
            clarification = self._clarification_payload(state, state["ranking"], selected_plan)
            return (
                TextPayload(summary=clarification.question or "Clarification is required before execution."),
                None,
                None,
                False,
                {},
            )
        if not bool(execution.get("allowed", False)):
            return (
                TextPayload(summary=str(execution.get("reason") or "Query was rejected.")),
                None,
                None,
                False,
                execution.get("audit", {}),
            )
        table_payload = None
        chart_payload = None
        if isinstance(execution.get("table"), dict):
            table_payload = TablePayload.model_validate(execution["table"])
            chart_payload = self._chart_for(state["ir"].intent_type, table_payload)
        summary = execution.get("summary")
        answer = TextPayload.model_validate(summary) if isinstance(summary, dict) else TextPayload(summary="Query executed successfully")
        return answer, table_payload, chart_payload, True, execution.get("audit", {})

    def _chart_for(self, intent_type: str, table: TablePayload) -> ChartPayload | None:
        if len(table.columns) < 2 or len(table.rows) < 2:
            return None
        x_key, y_key = table.columns[:2]
        if not all(isinstance(row.get(y_key), (int, float)) for row in table.rows):
            return None
        chart_type = "line" if intent_type == "trend" else "bar"
        return ChartPayload(
            title=f"{intent_type} chart",
            option={
                "xAxis": {"type": "category", "data": [row.get(x_key) for row in table.rows]},
                "yAxis": {"type": "value"},
                "series": [{"type": chart_type, "data": [row.get(y_key) for row in table.rows]}],
            },
            source=table.rows,
            description=f"Suggested {chart_type} chart for {intent_type}",
        )

    def _debug_payload(self, state: QueryV2State) -> dict[str, Any]:
        return {
            "plans": [plan.model_dump(mode="json") for plan in state["plans"]],
            "sql_candidates": [candidate.model_dump(mode="json") for candidate in state["sql_candidates"]],
            "verification_reports": [report.model_dump(mode="json") for report in state["verification_reports"]],
            "ranking": state["ranking"].model_dump(mode="json"),
        }

    def _execution_threshold(self, selected_plan: PlanCandidate | None) -> float:
        if selected_plan is None:
            return 0.75
        if selected_plan.answer_mode in {"detail", "roster", "record"}:
            return 0.7
        return 0.75

    def _serialize(self, payload: Any) -> Any:
        if hasattr(payload, "model_dump"):
            return payload.model_dump(mode="json")
        if isinstance(payload, list):
            return [self._serialize(item) for item in payload]
        if isinstance(payload, dict):
            return {key: self._serialize(value) for key, value in payload.items()}
        return payload
