from __future__ import annotations

from collections.abc import Callable
from typing import Any
from uuid import uuid4

from pydantic import BaseModel, Field

from ndea.config import Settings
from ndea.context import PolicyContext, RequestContext, coerce_request_context
from ndea.metadata.mysql_client import open_mysql_connection
from ndea.protocol import TablePayload, TextPayload
from ndea.security.permission import (
    TablePermissionChecker,
    parse_allowed_tables,
    parse_column_policy,
    parse_row_filters,
)
from ndea.security.policy import PolicyResolver
from ndea.security.safe_executor import ExplainCheckVerdict, PermissionCheckVerdict, SafeExecutionResult, SafeExecutor
from ndea.security.sql_guard import SQLGuard, SQLGuardVerdict


class QueryAuditPayload(BaseModel):
    actor_id: str | None = None
    original_sql: str
    effective_sql: str
    applied_row_filters: list[str] = Field(default_factory=list)
    masked_columns: list[str] = Field(default_factory=list)
    blocked_columns: list[str] = Field(default_factory=list)
    truncated: bool = False


class GuardedQueryPayload(BaseModel):
    trace_id: str
    request_id: str
    audit_id: str
    database: str
    sql: str
    effective_sql: str
    allowed: bool
    degraded: bool = False
    error_code: str | None = None
    policy_summary: dict[str, object] = Field(default_factory=dict)
    reason: str | None = None
    summary: TextPayload
    table: TablePayload | None = None
    guard: SQLGuardVerdict
    permission: PermissionCheckVerdict | None = None
    explain: ExplainCheckVerdict | None = None
    audit: QueryAuditPayload
    truncated: bool = False


class MySQLGuardedQueryService:
    def __init__(
        self,
        settings: Settings,
        connection_factory: Callable[[str], Any] | None = None,
        executor: SafeExecutor | None = None,
        permission_checker: Callable[[str], object] | None = None,
        policy_resolver: PolicyResolver | None = None,
        audit_id_factory: Callable[[], str] | None = None,
    ) -> None:
        self._settings = settings
        self._connection_factory = connection_factory or (
            lambda database: open_mysql_connection(settings, database=database)
        )
        self._executor = executor or SafeExecutor()
        self._permission_checker = permission_checker
        self._policy_resolver = policy_resolver or PolicyResolver(
            PolicyContext(
                allowed_tables=parse_allowed_tables(settings.permission_allowed_tables),
                blocked_columns=parse_column_policy(settings.permission_blocked_columns),
                masked_columns=parse_column_policy(settings.permission_masked_columns),
                row_filters=parse_row_filters(settings.permission_row_filters),
            )
        )
        self._audit_id_factory = audit_id_factory or (lambda: uuid4().hex)
        self._guard = SQLGuard()

    def execute_query(
        self,
        database: str,
        sql: str,
        request_context: RequestContext | dict[str, object] | None = None,
        policy_context: dict[str, object] | None = None,
    ) -> GuardedQueryPayload:
        context = coerce_request_context(request_context or {"policy_context": policy_context} if policy_context is not None else request_context)
        resolved_policy = self._policy_resolver.resolve(context, legacy_policy_context=policy_context)
        run_state: dict[str, object] = {
            "columns": [],
            "truncated": False,
            "effective_sql": sql,
        }
        audit_id = self._audit_id_factory()

        def query_runner(statement: str) -> list[dict[str, object]]:
            run_state["effective_sql"] = statement
            columns, rows, truncated = self._run_query(database, statement)
            run_state["columns"] = columns
            run_state["truncated"] = truncated
            return rows

        permission_checker = self._permission_checker or TablePermissionChecker.from_policy_context(
            resolved_policy
        ).check

        try:
            result = self._executor.execute(
                sql,
                query_runner,
                explain_checker=lambda statement: self._check_explain(database, statement),
                permission_checker=permission_checker,
            )
        except Exception as exc:
            return self._build_unavailable_payload(
                database=database,
                sql=sql,
                context=context,
                resolved_policy=resolved_policy,
                audit_id=audit_id,
                run_state=run_state,
                reason=str(exc) or exc.__class__.__name__,
            )

        return self._build_payload(
            database=database,
            sql=sql,
            context=context,
            resolved_policy=resolved_policy,
            result=result,
            audit_id=audit_id,
            run_state=run_state,
        )

    def _run_query(
        self,
        database: str,
        sql: str,
    ) -> tuple[list[str], list[dict[str, object]], bool]:
        connection = self._connection_factory(database)
        try:
            with connection.cursor(dictionary=True) as cursor:
                cursor.execute(sql)
                rows = cursor.fetchmany(self._settings.mysql_query_row_limit + 1)
                columns = list(getattr(cursor, "column_names", ()) or ())
        finally:
            connection.close()

        truncated = len(rows) > self._settings.mysql_query_row_limit
        return columns, rows[: self._settings.mysql_query_row_limit], truncated

    def _check_explain(self, database: str, sql: str) -> ExplainCheckVerdict:
        connection = self._connection_factory(database)
        try:
            with connection.cursor(dictionary=True) as cursor:
                cursor.execute(f"EXPLAIN {sql}")
                rows = cursor.fetchall()
        finally:
            connection.close()

        estimated_rows = self._extract_estimated_rows(rows)
        if estimated_rows is None:
            return ExplainCheckVerdict(allowed=True, reason=None, estimated_cost=None)

        if estimated_rows > float(self._settings.mysql_explain_row_limit):
            limit = self._settings.mysql_explain_row_limit
            return ExplainCheckVerdict(
                allowed=False,
                reason=f"EXPLAIN estimated {int(estimated_rows)} rows, exceeding limit {limit}",
                estimated_cost=estimated_rows,
            )

        return ExplainCheckVerdict(
            allowed=True,
            reason=None,
            estimated_cost=estimated_rows,
        )

    def _extract_estimated_rows(self, rows: list[dict[str, object]]) -> float | None:
        estimates: list[float] = []
        for row in rows:
            value = row.get("rows")
            if value is None:
                continue
            try:
                estimates.append(float(value))
            except (TypeError, ValueError):
                continue
        if not estimates:
            return None
        return max(estimates)

    def _build_payload(
        self,
        database: str,
        sql: str,
        context,
        resolved_policy,
        result: SafeExecutionResult,
        audit_id: str,
        run_state: dict[str, object],
    ) -> GuardedQueryPayload:
        truncated = bool(run_state["truncated"])
        permission_verdict = result.permission
        effective_sql = str(result.effective_sql or run_state["effective_sql"] or sql)
        audit = QueryAuditPayload(
            actor_id=(permission_verdict.actor_id if permission_verdict is not None else None)
            or context.actor_id,
            original_sql=sql,
            effective_sql=effective_sql,
            applied_row_filters=list(permission_verdict.applied_row_filters if permission_verdict else []),
            masked_columns=[],
            blocked_columns=list(permission_verdict.blocked_columns if permission_verdict else []),
            truncated=truncated if result.allowed else False,
        )
        error_code = None
        if not result.allowed:
            error_code = self._error_code_for_result(result)

        if result.allowed:
            rows, masked_columns = self._mask_rows(
                result.rows or [],
                list(permission_verdict.masked_columns if permission_verdict else []),
            )
            details_parts: list[str] = []
            if truncated:
                details_parts.append(
                    f"Results were limited to {self._settings.mysql_query_row_limit} rows."
                )
            if masked_columns:
                details_parts.append(f"Masked columns: {', '.join(masked_columns)}.")
            audit.masked_columns = masked_columns

            table = TablePayload(
                columns=list(run_state["columns"]),
                rows=rows,
                total_rows=len(rows),
            )
            summary = TextPayload(
                summary=f"Returned {len(rows)} rows from {database}",
                details=" ".join(details_parts) or None,
            )
            return GuardedQueryPayload(
                trace_id=context.trace_id,
                request_id=context.request_id,
                audit_id=audit_id,
                database=database,
                sql=sql,
                effective_sql=effective_sql,
                allowed=True,
                degraded=False,
                error_code=None,
                policy_summary=resolved_policy.summary(),
                reason=None,
                summary=summary,
                table=table,
                guard=result.guard,
                permission=permission_verdict,
                explain=result.explain,
                audit=audit,
                truncated=truncated,
            )

        summary = TextPayload(summary=result.reason or "Query was rejected")
        return GuardedQueryPayload(
            trace_id=context.trace_id,
            request_id=context.request_id,
            audit_id=audit_id,
            database=database,
            sql=sql,
            effective_sql=effective_sql,
            allowed=False,
            degraded=False,
            error_code=error_code,
            policy_summary=resolved_policy.summary(),
            reason=result.reason,
            summary=summary,
            table=None,
            guard=result.guard,
            permission=permission_verdict,
            explain=result.explain,
            audit=audit,
            truncated=False,
        )

    def _build_unavailable_payload(
        self,
        database: str,
        sql: str,
        context,
        resolved_policy,
        audit_id: str,
        run_state: dict[str, object],
        reason: str,
    ) -> GuardedQueryPayload:
        effective_sql = str(run_state["effective_sql"] or sql)
        audit = QueryAuditPayload(
            actor_id=context.actor_id,
            original_sql=sql,
            effective_sql=effective_sql,
            truncated=False,
        )
        summary = TextPayload(summary=reason)
        return GuardedQueryPayload(
            trace_id=context.trace_id,
            request_id=context.request_id,
            audit_id=audit_id,
            database=database,
            sql=sql,
            effective_sql=effective_sql,
            allowed=False,
            degraded=True,
            error_code="service_unavailable",
            policy_summary=resolved_policy.summary(),
            reason=reason,
            summary=summary,
            table=None,
            guard=self._guard.validate(sql),
            permission=None,
            explain=None,
            audit=audit,
            truncated=False,
        )

    def _error_code_for_result(self, result: SafeExecutionResult) -> str:
        if result.permission is not None and not result.permission.allowed:
            return "policy_denied"
        if not result.guard.allowed:
            return "guard_rejected"
        if result.explain is not None and not result.explain.allowed:
            return "guard_rejected"
        return "guard_rejected"

    def _mask_rows(
        self,
        rows: list[dict[str, object]],
        masked_columns: list[str],
    ) -> tuple[list[dict[str, object]], list[str]]:
        if not masked_columns:
            return rows, []

        masked_names = {
            column.split(".", 1)[-1].strip().lower()
            for column in masked_columns
            if column.strip()
        }
        applied: set[str] = set()
        masked_rows: list[dict[str, object]] = []
        for row in rows:
            masked_row: dict[str, object] = {}
            for key, value in row.items():
                if str(key).strip().lower() in masked_names:
                    masked_row[key] = "[REDACTED]"
                    applied.add(str(key))
                else:
                    masked_row[key] = value
            masked_rows.append(masked_row)
        return masked_rows, sorted(applied)
