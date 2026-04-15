from __future__ import annotations

import logging
import time
from collections.abc import Callable
from typing import Any
from uuid import uuid4

from ndea.context import RequestContext, coerce_request_context
from ndea.observability import AuditEvent, StructuredLoggerAuditSink
from ndea.planning.models import QueryPlanPayload, QueryWorkflowPayload, SQLAttemptPayload
from ndea.response import ResponseAssemblerService
from ndea.security.policy import PolicyResolver
from ndea.sql_generation import (
    SQLGenerationPayload,
    SQLGeneratorService,
    SQLRepairPayload,
    SQLRepairService,
)


class QueryWorkflowService:
    def __init__(
        self,
        planner: Any,
        query_service: Any,
        generator: Any | None = None,
        repairer: Any | None = None,
        assembler: Any | None = None,
        policy_resolver: PolicyResolver | None = None,
        audit_sink: Any | None = None,
        trace_id_factory: Callable[[], str] | None = None,
        request_id_factory: Callable[[], str] | None = None,
        audit_id_factory: Callable[[], str] | None = None,
        time_source: Callable[[], float] | None = None,
        max_repair_attempts: int = 1,
    ) -> None:
        self._planner = planner
        self._query_service = query_service
        self._generator = generator or SQLGeneratorService()
        self._repairer = repairer or SQLRepairService(generator=self._generator)
        self._assembler = assembler or ResponseAssemblerService()
        self._policy_resolver = policy_resolver or PolicyResolver()
        self._audit_sink = audit_sink or StructuredLoggerAuditSink()
        self._trace_id_factory = trace_id_factory or (lambda: uuid4().hex)
        self._request_id_factory = request_id_factory or (lambda: uuid4().hex)
        self._audit_id_factory = audit_id_factory or (lambda: uuid4().hex)
        self._time_source = time_source or time.perf_counter
        self._max_repair_attempts = max(0, max_repair_attempts)
        self._logger = logging.getLogger("ndea.workflow")

    def run(
        self,
        query_text: str,
        query_vector: list[float],
        database: str | None = None,
        execute: bool = False,
        request_context: RequestContext | dict[str, object] | None = None,
        policy_context: dict[str, object] | None = None,
    ) -> QueryWorkflowPayload:
        started_at = self._time_source()
        raw_request_context = request_context
        context = coerce_request_context(
            request_context or {"policy_context": policy_context} if policy_context is not None else request_context,
            trace_id_factory=self._trace_id_factory,
            request_id_factory=self._request_id_factory,
        )
        context_dump = context.model_dump(mode="json")
        resolved_policy = self._policy_resolver.resolve(context, legacy_policy_context=policy_context)

        plan = self._call_planner(query_text, query_vector, raw_request_context)
        tool_trace = ["query_planner"]
        notes: list[str] = []
        executed = False
        generation_payload: SQLGenerationPayload | None = None
        repair_payload: SQLRepairPayload | None = None
        sql_attempts: list[SQLAttemptPayload] = []
        execution_payload: dict[str, Any] | None = None
        resolved_sql: str | None = plan.selected_sql
        workflow_error_code: str | None = None
        degraded = bool(plan.degraded)
        audit_id: str | None = None

        if plan.selected_sql:
            generation_payload = SQLGenerationPayload(
                generated=True,
                sql=plan.selected_sql,
                strategy="rag_candidate",
                reason=None,
            )
        else:
            generation_payload = self._generator.generate(plan)
            if generation_payload.generated:
                resolved_sql = generation_payload.sql
                tool_trace.append("sql_generator")

        if execute:
            if plan.clarification_required:
                notes.append("Clarification required before execution.")
                workflow_error_code = "clarification_required"
            elif not database:
                notes.append("Database is required for execution.")
            elif not resolved_sql:
                notes.append(
                    (generation_payload.reason if generation_payload is not None else None)
                    or "No SQL candidate could be generated for execution."
                )
                workflow_error_code = "repair_exhausted"
            else:
                execution_payload, executed, repair_payload, sql_attempts = self._execute_with_repair(
                    plan=plan,
                    database=database,
                    initial_sql=resolved_sql,
                    initial_source=self._initial_sql_source(plan, generation_payload),
                    request_context=raw_request_context,
                    policy_context=policy_context,
                    tool_trace=tool_trace,
                    notes=notes,
                )
                degraded = degraded or bool(execution_payload.get("degraded", False))
                workflow_error_code = self._read_error_code(execution_payload)
                audit_id = self._read_text(execution_payload, "audit_id")
                if (
                    not executed
                    and repair_payload is not None
                    and not repair_payload.repaired
                    and workflow_error_code is None
                ):
                    workflow_error_code = "repair_exhausted"
        elif plan.degraded:
            workflow_error_code = "planner_degraded"

        if workflow_error_code is None and plan.degraded and not executed and execution_payload is None:
            workflow_error_code = "planner_degraded"

        response = self._assembler.assemble(plan, execution_payload)
        tool_trace.append("response_assembler")
        payload = QueryWorkflowPayload(
            trace_id=context.trace_id,
            request_id=context.request_id,
            query_text=query_text,
            database=database,
            request_context=context_dump,
            policy_context=policy_context,
            degraded=degraded,
            error_code=workflow_error_code,
            audit_id=audit_id or self._audit_id_factory(),
            policy_summary=resolved_policy.summary(),
            clarification_required=plan.clarification_required,
            clarification_questions=list(plan.clarification_questions),
            resolved_metric=(
                plan.resolved_metric.model_dump(mode="json")
                if plan.resolved_metric is not None
                else None
            ),
            resolved_dimensions=[
                dimension.model_dump(mode="json")
                for dimension in plan.dimensions
            ],
            resolved_filters=[
                filter_payload.model_dump(mode="json")
                for filter_payload in plan.filters
            ],
            resolved_time_scope=(
                plan.time_scope.model_dump(mode="json")
                if plan.time_scope is not None
                else None
            ),
            executed=executed,
            tool_trace=tool_trace,
            notes=notes,
            plan=plan if isinstance(plan, QueryPlanPayload) else QueryPlanPayload.model_validate(plan),
            generation=generation_payload,
            repair=repair_payload,
            sql_attempts=sql_attempts,
            execution=execution_payload,
            response_text=response.text,
            response_table=response.table,
            response_chart=response.chart,
        )
        self._emit_audit_event(
            payload=payload,
            query_text=query_text,
            selected_sql=resolved_sql,
            execution_payload=execution_payload,
            latency_ms=int(round((self._time_source() - started_at) * 1000)),
        )
        return payload

    def _call_planner(
        self,
        query_text: str,
        query_vector: list[float],
        request_context: RequestContext | dict[str, object] | None,
    ) -> QueryPlanPayload:
        try:
            if request_context is None:
                plan = self._planner.plan(query_text=query_text, query_vector=query_vector)
            else:
                plan = self._planner.plan(
                    query_text=query_text,
                    query_vector=query_vector,
                    request_context=request_context,
                )
        except TypeError as exc:
            if "request_context" not in str(exc):
                raise
            plan = self._planner.plan(query_text=query_text, query_vector=query_vector)
        if isinstance(plan, QueryPlanPayload):
            return plan
        return QueryPlanPayload.model_validate(plan)

    def _execute_with_repair(
        self,
        plan: QueryPlanPayload,
        database: str,
        initial_sql: str,
        initial_source: str,
        request_context: RequestContext | dict[str, object] | None,
        policy_context: dict[str, object] | None,
        tool_trace: list[str],
        notes: list[str],
    ) -> tuple[dict[str, Any], bool, SQLRepairPayload | None, list[SQLAttemptPayload]]:
        current_sql = initial_sql
        current_source = initial_source
        latest_repair: SQLRepairPayload | None = None
        attempts: list[SQLAttemptPayload] = []

        for attempt_number in range(1, self._max_repair_attempts + 2):
            tool_trace.append("safe_executor")
            execution_payload = self._execute_sql(
                database,
                current_sql,
                request_context=request_context,
                policy_context=policy_context,
            )
            failure_reason = self._read_failure_reason(execution_payload)
            error_code = self._read_error_code(execution_payload)
            allowed = bool(execution_payload.get("allowed", False))
            attempts.append(
                SQLAttemptPayload(
                    attempt_number=attempt_number,
                    sql=current_sql,
                    source=current_source,
                    status="succeeded" if allowed else "failed",
                    reason=None if allowed else failure_reason,
                )
            )
            if allowed:
                return execution_payload, True, latest_repair, attempts

            if error_code == "service_unavailable":
                if failure_reason:
                    notes.append(failure_reason)
                return execution_payload, False, latest_repair, attempts

            if attempt_number > self._max_repair_attempts:
                if failure_reason:
                    notes.append(failure_reason)
                return execution_payload, False, latest_repair, attempts

            latest_repair = self._repairer.repair(
                plan=plan,
                failed_sql=current_sql,
                failure_reason=failure_reason or "Execution failed",
                attempt_number=attempt_number,
            )
            tool_trace.append("sql_repair")
            if not latest_repair.repaired or not latest_repair.sql:
                notes.append(
                    latest_repair.reason
                    or failure_reason
                    or "Execution failed and could not be repaired"
                )
                return execution_payload, False, latest_repair, attempts

            current_sql = latest_repair.sql
            current_source = latest_repair.strategy or "repaired_sql"

        return {"allowed": False, "reason": "Execution did not start"}, False, latest_repair, attempts

    def _execute_sql(
        self,
        database: str,
        sql: str,
        request_context: RequestContext | dict[str, object] | None = None,
        policy_context: dict[str, object] | None = None,
    ) -> dict[str, Any]:
        trace_id = self._read_text(request_context or {}, "trace_id")
        request_id = self._read_text(request_context or {}, "request_id")
        try:
            try:
                call_kwargs: dict[str, Any] = {}
                if request_context is not None:
                    call_kwargs["request_context"] = request_context
                if policy_context is not None:
                    call_kwargs["policy_context"] = policy_context
                execution_result = self._query_service.execute_query(database, sql, **call_kwargs)
            except TypeError as exc:
                if "request_context" in str(exc):
                    execution_result = self._query_service.execute_query(
                        database,
                        sql,
                        **({"policy_context": policy_context} if policy_context is not None else {}),
                    )
                elif "policy_context" in str(exc):
                    execution_result = self._query_service.execute_query(
                        database,
                        sql,
                        **({"request_context": request_context} if request_context is not None else {}),
                    )
                else:
                    raise
        except Exception as exc:  # pragma: no cover - exercised through workflow tests
            message = str(exc) or exc.__class__.__name__
            error_code = self._classify_execution_exception(message)
            return {
                "trace_id": trace_id,
                "request_id": request_id,
                "database": database,
                "sql": sql,
                "effective_sql": sql,
                "allowed": False,
                "reason": message,
                "summary": {"summary": message, "details": None},
                "table": None,
                "degraded": error_code == "service_unavailable",
                "error_code": error_code,
            }

        if hasattr(execution_result, "model_dump"):
            return execution_result.model_dump()
        return execution_result

    def _emit_audit_event(
        self,
        payload: QueryWorkflowPayload,
        query_text: str,
        selected_sql: str | None,
        execution_payload: dict[str, Any] | None,
        latency_ms: int,
    ) -> None:
        audit = execution_payload.get("audit", {}) if isinstance(execution_payload, dict) else {}
        permission_actions = {
            "applied_row_filters": list(audit.get("applied_row_filters", [])) if isinstance(audit, dict) else [],
            "masked_columns": list(audit.get("masked_columns", [])) if isinstance(audit, dict) else [],
            "blocked_columns": list(audit.get("blocked_columns", [])) if isinstance(audit, dict) else [],
            "policy_summary": payload.policy_summary,
        }
        dependency_status = self._build_dependency_status(payload, execution_payload)
        final_status = self._final_status(payload)
        event = AuditEvent(
            audit_id=payload.audit_id or self._audit_id_factory(),
            trace_id=payload.trace_id,
            request_id=payload.request_id,
            actor_id=self._read_text(payload.request_context or {}, "actor_id"),
            tenant_id=self._read_text(payload.request_context or {}, "tenant_id"),
            query_text=query_text,
            intent_type=payload.plan.intent_type,
            tool_trace=list(payload.tool_trace),
            selected_sql=selected_sql,
            effective_sql=self._read_text(execution_payload or {}, "effective_sql"),
            sql_attempts=[attempt.model_dump() for attempt in payload.sql_attempts],
            permission_actions=permission_actions,
            degraded=payload.degraded,
            final_status=final_status,
            error_code=payload.error_code,
            latency_ms=latency_ms,
            dependency_status=dependency_status,
        )
        try:
            self._audit_sink.emit(event)
        except Exception as exc:  # pragma: no cover - behavior asserted through workflow tests
            self._logger.warning("Failed to emit audit event: %s", exc)

    def _build_dependency_status(
        self,
        payload: QueryWorkflowPayload,
        execution_payload: dict[str, Any] | None,
    ) -> dict[str, str]:
        mysql_status = "skipped"
        if payload.executed or execution_payload is not None:
            mysql_status = "healthy"
            if self._read_error_code(execution_payload or {}) == "service_unavailable":
                mysql_status = "unavailable"
        milvus_status = "degraded" if payload.plan.degraded else "healthy"
        return {"mysql": mysql_status, "milvus": milvus_status}

    def _final_status(self, payload: QueryWorkflowPayload) -> str:
        if payload.executed and payload.error_code is None:
            return "succeeded"
        if payload.error_code is not None:
            return payload.error_code
        return "failed"

    def _initial_sql_source(
        self,
        plan: QueryPlanPayload,
        generation_payload: SQLGenerationPayload | None,
    ) -> str:
        if plan.selected_sql:
            return "rag_candidate"
        if generation_payload is None:
            return "generated_sql"
        return generation_payload.strategy or "generated_sql"

    def _read_failure_reason(self, execution_payload: dict[str, Any]) -> str | None:
        summary = execution_payload.get("summary")
        if isinstance(summary, dict):
            value = summary.get("summary")
            if isinstance(value, str) and value:
                return value

        reason = execution_payload.get("reason")
        if isinstance(reason, str) and reason:
            return reason
        return None

    def _read_error_code(self, execution_payload: dict[str, Any]) -> str | None:
        value = execution_payload.get("error_code")
        if isinstance(value, str) and value:
            return value
        return None

    def _read_text(self, payload: dict[str, Any], field: str) -> str | None:
        value = payload.get(field)
        if value in (None, ""):
            return None
        return str(value)

    def _classify_execution_exception(self, message: str) -> str | None:
        lowered = message.lower()
        unavailable_markers = (
            "unavailable",
            "connection refused",
            "connect timeout",
            "timed out",
            "can't connect",
            "cannot connect",
            "connection reset",
            "connection aborted",
            "server has gone away",
        )
        if any(marker in lowered for marker in unavailable_markers):
            return "service_unavailable"
        return None
