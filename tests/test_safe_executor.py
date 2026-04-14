from ndea.security.safe_executor import (
    ExplainCheckVerdict,
    PermissionCheckVerdict,
    SafeExecutor,
)


def test_safe_executor_blocks_runner_when_guard_rejects() -> None:
    calls: list[str] = []

    def runner(sql: str) -> list[dict[str, object]]:
        calls.append(sql)
        return []

    result = SafeExecutor().execute("DELETE FROM student", runner)

    assert result.allowed is False
    assert result.reason == "Only read-only SELECT statements are allowed"
    assert calls == []


def test_safe_executor_rejects_complex_query_without_explain_checker() -> None:
    calls: list[str] = []

    def runner(sql: str) -> list[dict[str, object]]:
        calls.append(sql)
        return []

    sql = "SELECT department, COUNT(*) AS total FROM student GROUP BY department"
    result = SafeExecutor().execute(sql, runner)

    assert result.allowed is False
    assert result.reason == "Complex queries require explain approval"
    assert result.guard.needs_explain is True
    assert calls == []


def test_safe_executor_blocks_runner_on_permission_rejection() -> None:
    calls: list[str] = []

    def runner(sql: str) -> list[dict[str, object]]:
        calls.append("runner")
        return []

    def permission_checker(sql: str) -> PermissionCheckVerdict:
        calls.append("permission")
        return PermissionCheckVerdict(
            allowed=False,
            reason="Query is outside policy scope",
        )

    result = SafeExecutor().execute(
        "SELECT 1",
        runner,
        permission_checker=permission_checker,
    )

    assert result.allowed is False
    assert result.reason == "Query is outside policy scope"
    assert calls == ["permission"]


def test_safe_executor_runs_explain_before_query_and_returns_rows() -> None:
    calls: list[str] = []

    def runner(sql: str) -> list[dict[str, object]]:
        calls.append("runner")
        return [{"department": "math", "total": 3}]

    def explain_checker(sql: str) -> ExplainCheckVerdict:
        calls.append("explain")
        return ExplainCheckVerdict(
            allowed=True,
            reason=None,
            estimated_cost=12.5,
        )

    sql = "SELECT department, COUNT(*) AS total FROM student GROUP BY department"
    result = SafeExecutor().execute(
        sql,
        runner,
        explain_checker=explain_checker,
    )

    assert result.allowed is True
    assert result.reason is None
    assert calls == ["explain", "runner"]
    assert result.explain is not None
    assert result.explain.estimated_cost == 12.5
    assert result.rows == [{"department": "math", "total": 3}]


def test_safe_executor_executes_rewritten_sql_from_permission_checker() -> None:
    calls: list[str] = []

    def runner(sql: str) -> list[dict[str, object]]:
        calls.append(sql)
        return [{"student_id": 1}]

    def permission_checker(sql: str) -> PermissionCheckVerdict:
        return PermissionCheckVerdict(
            allowed=True,
            reason=None,
            rewritten_sql="SELECT student_id FROM student WHERE student.tenant_id = 7",
            applied_row_filters=["student.tenant_id = 7"],
            masked_columns=["student.ssn"],
            blocked_columns=[],
            actor_id="user-7",
        )

    result = SafeExecutor().execute(
        "SELECT student_id FROM student",
        runner,
        permission_checker=permission_checker,
    )

    assert result.allowed is True
    assert result.permission is not None
    assert result.permission.actor_id == "user-7"
    assert result.effective_sql == "SELECT student_id FROM student WHERE student.tenant_id = 7"
    assert calls == ["SELECT student_id FROM student WHERE student.tenant_id = 7"]
