from collections.abc import Callable

from pydantic import BaseModel, Field

from ndea.security.sql_guard import SQLGuard, SQLGuardVerdict


class ExplainCheckVerdict(BaseModel):
    allowed: bool
    reason: str | None = None
    estimated_cost: float | None = None


class PermissionCheckVerdict(BaseModel):
    allowed: bool
    reason: str | None = None
    rewritten_sql: str | None = None
    applied_row_filters: list[str] = Field(default_factory=list)
    masked_columns: list[str] = Field(default_factory=list)
    blocked_columns: list[str] = Field(default_factory=list)
    actor_id: str | None = None


class SafeExecutionResult(BaseModel):
    allowed: bool
    reason: str | None = None
    guard: SQLGuardVerdict
    permission: PermissionCheckVerdict | None = None
    explain: ExplainCheckVerdict | None = None
    effective_sql: str | None = None
    rows: list[dict[str, object]] | None = None


class SafeExecutor:
    def __init__(self, guard: SQLGuard | None = None) -> None:
        self._guard = guard or SQLGuard()

    def execute(
        self,
        sql: str,
        query_runner: Callable[[str], list[dict[str, object]]],
        explain_checker: Callable[[str], ExplainCheckVerdict] | None = None,
        permission_checker: Callable[[str], PermissionCheckVerdict] | None = None,
    ) -> SafeExecutionResult:
        guard_verdict = self._guard.validate(sql)
        if not guard_verdict.allowed:
            return SafeExecutionResult(
                allowed=False,
                reason=guard_verdict.reason,
                guard=guard_verdict,
                effective_sql=sql,
            )

        permission_verdict: PermissionCheckVerdict | None = None
        effective_sql = sql
        if permission_checker is not None:
            permission_verdict = permission_checker(sql)
            if not permission_verdict.allowed:
                return SafeExecutionResult(
                    allowed=False,
                    reason=permission_verdict.reason,
                    guard=guard_verdict,
                    permission=permission_verdict,
                    effective_sql=permission_verdict.rewritten_sql or sql,
                )
            effective_sql = permission_verdict.rewritten_sql or sql

        explain_verdict: ExplainCheckVerdict | None = None
        if guard_verdict.needs_explain:
            if explain_checker is None:
                return SafeExecutionResult(
                    allowed=False,
                    reason="Complex queries require explain approval",
                    guard=guard_verdict,
                    permission=permission_verdict,
                    effective_sql=effective_sql,
                )

            explain_verdict = explain_checker(effective_sql)
            if not explain_verdict.allowed:
                return SafeExecutionResult(
                    allowed=False,
                    reason=explain_verdict.reason,
                    guard=guard_verdict,
                    permission=permission_verdict,
                    explain=explain_verdict,
                    effective_sql=effective_sql,
                )

        rows = query_runner(effective_sql)
        return SafeExecutionResult(
            allowed=True,
            reason=None,
            guard=guard_verdict,
            permission=permission_verdict,
            explain=explain_verdict,
            effective_sql=effective_sql,
            rows=rows,
        )
