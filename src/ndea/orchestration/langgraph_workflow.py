from __future__ import annotations

from collections.abc import Iterator
from typing import Any, TypedDict

from langgraph.graph import END, START, StateGraph

from ndea.config import Settings
from ndea.context import RequestContext, coerce_request_context
from ndea.planning.models import QueryWorkflowPayload, SQLAttemptPayload
from ndea.planning.workflow import QueryWorkflowService
from ndea.sql_advisor import SQLAdvisoryPayload, VannaStyleSQLAdvisorService
from ndea.sql_generation import SQLGenerationPayload, SQLRepairPayload


class WorkflowState(TypedDict, total=False):
    query_text: str
    query_vector: list[float]
    database: str | None
    execute: bool
    raw_request_context: RequestContext | dict[str, object] | None
    request_context: dict[str, Any]
    policy_context: dict[str, object] | None
    context: RequestContext
    resolved_policy: Any
    started_at: float
    notes: list[str]
    tool_trace: list[str]
    sql_attempts: list[SQLAttemptPayload]
    degraded: bool
    error_code: str | None
    plan: Any
    advisory: SQLAdvisoryPayload
    generation: SQLGenerationPayload
    repair: SQLRepairPayload
    execution: dict[str, Any] | None
    current_sql: str | None
    current_source: str | None
    final_payload: QueryWorkflowPayload


class LangGraphQueryWorkflowService(QueryWorkflowService):
    def __init__(
        self,
        planner: Any,
        query_service: Any,
        advisor: Any | None = None,
        generator: Any | None = None,
        repairer: Any | None = None,
        assembler: Any | None = None,
        policy_resolver=None,
        audit_sink=None,
        trace_id_factory=None,
        request_id_factory=None,
        audit_id_factory=None,
        time_source=None,
        max_repair_attempts: int = 1,
    ) -> None:
        super().__init__(
            planner=planner,
            query_service=query_service,
            generator=generator,
            repairer=repairer,
            assembler=assembler,
            policy_resolver=policy_resolver,
            audit_sink=audit_sink,
            trace_id_factory=trace_id_factory,
            request_id_factory=request_id_factory,
            audit_id_factory=audit_id_factory,
            time_source=time_source,
            max_repair_attempts=max_repair_attempts,
        )
        self._advisor = advisor or VannaStyleSQLAdvisorService(settings=Settings())
        self._graph = self._build_graph()

    def run(
        self,
        query_text: str,
        query_vector: list[float],
        database: str | None = None,
        execute: bool = False,
        request_context: RequestContext | dict[str, object] | None = None,
        policy_context: dict[str, object] | None = None,
    ) -> QueryWorkflowPayload:
        state = self._graph.invoke(
            self._initial_state(
                query_text=query_text,
                query_vector=query_vector,
                database=database,
                execute=execute,
                request_context=request_context,
                policy_context=policy_context,
            )
        )
        payload = state["final_payload"]
        if isinstance(payload, QueryWorkflowPayload):
            return payload
        return QueryWorkflowPayload.model_validate(payload)

    def stream(
        self,
        query_text: str,
        query_vector: list[float],
        database: str | None = None,
        execute: bool = False,
        request_context: RequestContext | dict[str, object] | None = None,
        policy_context: dict[str, object] | None = None,
    ) -> Iterator[dict[str, Any]]:
        initial_state = self._initial_state(
            query_text=query_text,
            query_vector=query_vector,
            database=database,
            execute=execute,
            request_context=request_context,
            policy_context=policy_context,
        )
        for mode, chunk in self._graph.stream(initial_state, stream_mode=["updates", "values"]):
            if mode == "updates":
                node_name = next(iter(chunk))
                yield {
                    "type": "node",
                    "node": node_name,
                    "payload": self._serialize_for_json(self._node_event_payload(node_name, chunk[node_name])),
                }
                continue

            final_payload = chunk.get("final_payload")
            if final_payload is not None:
                yield {"type": "final", "payload": self._serialize_for_json(final_payload)}

    def _build_graph(self):
        graph = StateGraph(WorkflowState)
        graph.add_node("planner", self._planner_node)
        graph.add_node("advisor", self._advisor_node)
        graph.add_node("generator", self._generator_node)
        graph.add_node("executor", self._executor_node)
        graph.add_node("repair", self._repair_node)
        graph.add_node("assembler", self._assembler_node)
        graph.add_edge(START, "planner")
        graph.add_conditional_edges("planner", self._after_planner, {"advisor": "advisor", "assembler": "assembler"})
        graph.add_conditional_edges(
            "advisor",
            self._after_advisor,
            {"generator": "generator", "executor": "executor", "assembler": "assembler"},
        )
        graph.add_conditional_edges(
            "generator",
            self._after_generator,
            {"executor": "executor", "assembler": "assembler"},
        )
        graph.add_conditional_edges("executor", self._after_executor, {"repair": "repair", "assembler": "assembler"})
        graph.add_conditional_edges("repair", self._after_repair, {"executor": "executor", "assembler": "assembler"})
        graph.add_edge("assembler", END)
        return graph.compile()

    def _initial_state(
        self,
        query_text: str,
        query_vector: list[float],
        database: str | None,
        execute: bool,
        request_context: RequestContext | dict[str, object] | None,
        policy_context: dict[str, object] | None,
    ) -> WorkflowState:
        raw_request_context = request_context
        context = coerce_request_context(
            request_context or {"policy_context": policy_context} if policy_context is not None else request_context,
            trace_id_factory=self._trace_id_factory,
            request_id_factory=self._request_id_factory,
        )
        return WorkflowState(
            query_text=query_text,
            query_vector=query_vector,
            database=database,
            execute=execute,
            raw_request_context=raw_request_context,
            request_context=context.model_dump(mode="json"),
            policy_context=policy_context,
            context=context,
            resolved_policy=self._policy_resolver.resolve(context, legacy_policy_context=policy_context),
            started_at=self._time_source(),
            notes=[],
            tool_trace=[],
            sql_attempts=[],
            degraded=False,
            error_code=None,
            current_sql=None,
            current_source=None,
        )

    def _planner_node(self, state: WorkflowState) -> WorkflowState:
        plan = self._call_planner(
            query_text=state["query_text"],
            query_vector=state["query_vector"],
            request_context=state.get("raw_request_context"),
        )
        return {
            "plan": plan,
            "tool_trace": [*state.get("tool_trace", []), "langgraph_planner"],
            "degraded": bool(state.get("degraded", False) or plan.degraded),
        }

    def _advisor_node(self, state: WorkflowState) -> WorkflowState:
        plan = state["plan"]
        try:
            advisory = self._advisor.advise(
                query_text=state["query_text"],
                query_vector=state["query_vector"],
                plan=plan,
            )
        except TypeError:
            advisory = self._advisor.advise(query_text=state["query_text"], plan=plan)
        current_sql = plan.selected_sql or advisory.selected_sql
        current_source = "rag_candidate" if plan.selected_sql else advisory.strategy or "sql_advisor"
        return {
            "advisory": advisory,
            "current_sql": current_sql,
            "current_source": current_source if current_sql else None,
            "notes": [*state.get("notes", []), *list(advisory.notes)],
            "tool_trace": [*state.get("tool_trace", []), "sql_advisor"],
        }

    def _generator_node(self, state: WorkflowState) -> WorkflowState:
        generation = self._generator.generate(state["plan"])
        notes = list(state.get("notes", []))
        if generation.reason and not generation.generated:
            notes.append(generation.reason)
        return {
            "generation": generation,
            "current_sql": generation.sql,
            "current_source": generation.strategy or "sql_generator" if generation.sql else None,
            "notes": notes,
            "tool_trace": [*state.get("tool_trace", []), "sql_generator"],
        }

    def _executor_node(self, state: WorkflowState) -> WorkflowState:
        current_sql = state.get("current_sql")
        if not current_sql:
            return {"error_code": "repair_exhausted"}
        execution = self._execute_sql(
            database=state["database"],
            sql=current_sql,
            request_context=state.get("raw_request_context"),
            policy_context=state.get("policy_context"),
        )
        failure_reason = self._read_failure_reason(execution)
        allowed = bool(execution.get("allowed", False))
        attempts = [
            *state.get("sql_attempts", []),
            SQLAttemptPayload(
                attempt_number=len(state.get("sql_attempts", [])) + 1,
                sql=current_sql,
                source=state.get("current_source") or "generated_sql",
                status="succeeded" if allowed else "failed",
                reason=None if allowed else failure_reason,
            ),
        ]
        return {
            "execution": execution,
            "sql_attempts": attempts,
            "degraded": bool(state.get("degraded", False) or execution.get("degraded", False)),
            "error_code": self._read_error_code(execution),
            "tool_trace": [*state.get("tool_trace", []), "langgraph_executor"],
        }

    def _repair_node(self, state: WorkflowState) -> WorkflowState:
        repair = self._repairer.repair(
            plan=state["plan"],
            failed_sql=state.get("current_sql") or "",
            failure_reason=self._read_failure_reason(state.get("execution") or {}) or "Execution failed",
            attempt_number=len(state.get("sql_attempts", [])),
        )
        notes = list(state.get("notes", []))
        if repair.reason and not repair.repaired:
            notes.append(repair.reason)
        return {
            "repair": repair,
            "notes": notes,
            "current_sql": repair.sql if repair.repaired else state.get("current_sql"),
            "current_source": repair.strategy or state.get("current_source"),
            "tool_trace": [*state.get("tool_trace", []), "sql_repair"],
        }

    def _assembler_node(self, state: WorkflowState) -> WorkflowState:
        plan = state["plan"]
        execution_payload = state.get("execution")
        response = self._assembler.assemble(plan, execution_payload)
        tool_trace = [*state.get("tool_trace", []), "response_assembler"]
        payload = self._build_payload(state, plan, execution_payload, response, tool_trace)
        self._emit_audit_event(
            payload=payload,
            query_text=state["query_text"],
            selected_sql=state.get("current_sql"),
            execution_payload=execution_payload,
            latency_ms=int(round((self._time_source() - state["started_at"]) * 1000)),
        )
        return {"final_payload": payload}

    def _after_planner(self, state: WorkflowState) -> str:
        return "assembler" if state["plan"].clarification_required else "advisor"

    def _after_advisor(self, state: WorkflowState) -> str:
        if not state["execute"] or not state.get("database"):
            return "assembler"
        return "executor" if state.get("current_sql") else "generator"

    def _after_generator(self, state: WorkflowState) -> str:
        if not state["execute"] or not state.get("database") or not state.get("current_sql"):
            return "assembler"
        return "executor"

    def _after_executor(self, state: WorkflowState) -> str:
        execution = state.get("execution") or {}
        if execution.get("allowed") or state.get("error_code") == "service_unavailable":
            return "assembler"
        if len(state.get("sql_attempts", [])) > self._max_repair_attempts:
            return "assembler"
        return "repair"

    def _after_repair(self, state: WorkflowState) -> str:
        repair = state.get("repair")
        if repair is not None and repair.repaired and repair.sql:
            return "executor"
        return "assembler"

    def _build_payload(
        self,
        state: WorkflowState,
        plan,
        execution_payload: dict[str, Any] | None,
        response,
        tool_trace: list[str],
    ) -> QueryWorkflowPayload:
        executed = bool(execution_payload is not None and execution_payload.get("allowed", False))
        notes = list(state.get("notes", []))
        error_code = state.get("error_code")
        if error_code is None and execution_payload is not None:
            error_code = self._read_error_code(execution_payload)
        if error_code is None and plan.clarification_required and state["execute"]:
            error_code = "clarification_required"
        if error_code is None and state["execute"] and not state.get("database"):
            notes.append("Database is required for execution.")
        if error_code is None and state["execute"] and not state.get("current_sql") and not plan.clarification_required:
            error_code = "repair_exhausted"
        if error_code is None and plan.degraded and not executed:
            error_code = "planner_degraded"
        if plan.clarification_required and state["execute"]:
            clarification_note = "Clarification required before execution."
            if clarification_note not in notes:
                notes.append(clarification_note)
        repair_payload = state.get("repair")
        if repair_payload is not None and repair_payload.reason and repair_payload.reason not in notes:
            notes.append(repair_payload.reason)
        audit_id = self._read_text(execution_payload or {}, "audit_id") or self._audit_id_factory()
        return QueryWorkflowPayload(
            trace_id=state["context"].trace_id,
            request_id=state["context"].request_id,
            query_text=state["query_text"],
            database=state.get("database"),
            request_context=state["request_context"],
            policy_context=state.get("policy_context"),
            degraded=bool(state.get("degraded", False)),
            error_code=error_code,
            audit_id=audit_id,
            policy_summary=state["resolved_policy"].summary(),
            clarification_required=plan.clarification_required,
            clarification_questions=list(plan.clarification_questions),
            resolved_metric=plan.resolved_metric.model_dump(mode="json") if plan.resolved_metric is not None else None,
            resolved_dimensions=[dimension.model_dump(mode="json") for dimension in plan.dimensions],
            resolved_filters=[filter_payload.model_dump(mode="json") for filter_payload in plan.filters],
            resolved_time_scope=plan.time_scope.model_dump(mode="json") if plan.time_scope is not None else None,
            executed=executed,
            tool_trace=tool_trace,
            notes=notes,
            plan=plan,
            generation=state.get("generation"),
            repair=repair_payload,
            sql_attempts=state.get("sql_attempts", []),
            execution=execution_payload,
            response_text=response.text,
            response_table=response.table,
            response_chart=response.chart,
        )

    def _node_event_payload(self, node_name: str, delta: dict[str, Any]) -> dict[str, Any]:
        if node_name == "planner":
            plan = delta.get("plan")
            if hasattr(plan, "summary"):
                return {
                    "summary": plan.summary,
                    "clarification_required": plan.clarification_required,
                    "degraded": plan.degraded,
                }
        if node_name == "advisor":
            advisory = delta.get("advisory")
            if hasattr(advisory, "strategy"):
                return {
                    "strategy": advisory.strategy,
                    "confidence": advisory.confidence,
                    "selected_sql": advisory.selected_sql,
                }
        if node_name == "generator":
            generation = delta.get("generation")
            if hasattr(generation, "generated"):
                return {"generated": generation.generated, "strategy": generation.strategy}
        if node_name == "executor":
            execution = delta.get("execution")
            if isinstance(execution, dict):
                return {
                    "allowed": execution.get("allowed"),
                    "error_code": execution.get("error_code"),
                    "effective_sql": execution.get("effective_sql"),
                }
        if node_name == "repair":
            repair = delta.get("repair")
            if hasattr(repair, "repaired"):
                return {"repaired": repair.repaired, "strategy": repair.strategy, "trigger": repair.trigger}
        return delta

    def _serialize_for_json(self, value: Any) -> Any:
        if hasattr(value, "model_dump"):
            return value.model_dump(mode="json")
        if isinstance(value, dict):
            return {key: self._serialize_for_json(item) for key, item in value.items()}
        if isinstance(value, list):
            return [self._serialize_for_json(item) for item in value]
        return value
