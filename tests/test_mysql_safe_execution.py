from ndea.config import Settings
from ndea.security.mysql_safe_execution import MySQLGuardedQueryService


class FakeCursor:
    def __init__(
        self,
        *,
        explain_rows: list[dict[str, object]] | None = None,
        query_rows: list[dict[str, object]] | None = None,
        executed: list[str] | None = None,
    ) -> None:
        self._explain_rows = explain_rows or []
        self._query_rows = query_rows or []
        self._executed = executed if executed is not None else []
        self._active_rows: list[dict[str, object]] = []
        self.column_names: tuple[str, ...] = ()

    def execute(self, query: str, params=None) -> None:
        normalized = " ".join(query.split())
        self._executed.append(normalized)
        if normalized.startswith("EXPLAIN "):
            self._active_rows = list(self._explain_rows)
        else:
            self._active_rows = list(self._query_rows)
        if self._active_rows:
            self.column_names = tuple(self._active_rows[0].keys())
        else:
            self.column_names = ()

    def fetchall(self) -> list[dict[str, object]]:
        return list(self._active_rows)

    def fetchmany(self, size: int) -> list[dict[str, object]]:
        return list(self._active_rows[:size])

    def __enter__(self) -> "FakeCursor":
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        return False


class FakeConnection:
    def __init__(
        self,
        *,
        explain_rows: list[dict[str, object]] | None = None,
        query_rows: list[dict[str, object]] | None = None,
        executed: list[str] | None = None,
    ) -> None:
        self._explain_rows = explain_rows or []
        self._query_rows = query_rows or []
        self._executed = executed if executed is not None else []

    def cursor(self, dictionary=True) -> FakeCursor:
        return FakeCursor(
            explain_rows=self._explain_rows,
            query_rows=self._query_rows,
            executed=self._executed,
        )

    def close(self) -> None:
        return None


def test_execute_returns_rows_for_simple_select() -> None:
    executed: list[str] = []
    settings = Settings(mysql_query_row_limit=2, mysql_explain_row_limit=50)
    service = MySQLGuardedQueryService(
        settings,
        connection_factory=lambda database: FakeConnection(
            query_rows=[{"student_id": 1, "name": "Alice"}],
            executed=executed,
        ),
    )

    payload = service.execute_query(
        "campus",
        "SELECT student_id, name FROM student",
        request_context={
            "trace_id": "trace-simple",
            "request_id": "request-simple",
            "actor_id": "user-1",
        },
    )

    assert payload.allowed is True
    assert payload.trace_id == "trace-simple"
    assert payload.request_id == "request-simple"
    assert payload.audit_id
    assert payload.error_code is None
    assert payload.database == "campus"
    assert payload.summary.summary == "Returned 1 rows from campus"
    assert payload.table is not None
    assert payload.table.columns == ["student_id", "name"]
    assert payload.table.rows == [{"student_id": 1, "name": "Alice"}]
    assert payload.truncated is False
    assert executed == ["SELECT student_id, name FROM student"]


def test_execute_rejects_when_explain_rows_exceed_limit() -> None:
    executed: list[str] = []
    settings = Settings(mysql_query_row_limit=5, mysql_explain_row_limit=100)
    service = MySQLGuardedQueryService(
        settings,
        connection_factory=lambda database: FakeConnection(
            explain_rows=[{"rows": 250}],
            executed=executed,
        ),
    )

    payload = service.execute_query(
        "campus",
        "SELECT department, COUNT(*) AS total FROM student GROUP BY department",
    )

    assert payload.allowed is False
    assert payload.error_code == "guard_rejected"
    assert payload.reason == "EXPLAIN estimated 250 rows, exceeding limit 100"
    assert payload.table is None
    assert payload.explain is not None
    assert payload.explain.allowed is False
    assert payload.explain.estimated_cost == 250.0
    assert executed == [
        "EXPLAIN SELECT department, COUNT(*) AS total FROM student GROUP BY department"
    ]


def test_execute_runs_explain_and_truncates_rows_for_complex_query() -> None:
    executed: list[str] = []
    settings = Settings(mysql_query_row_limit=2, mysql_explain_row_limit=1000)
    service = MySQLGuardedQueryService(
        settings,
        connection_factory=lambda database: FakeConnection(
            explain_rows=[{"rows": 12}],
            query_rows=[
                {"department": "math", "total": 3},
                {"department": "physics", "total": 2},
                {"department": "chemistry", "total": 1},
            ],
            executed=executed,
        ),
    )

    payload = service.execute_query(
        "campus",
        "SELECT department, COUNT(*) AS total FROM student GROUP BY department",
    )

    assert payload.allowed is True
    assert payload.explain is not None
    assert payload.explain.allowed is True
    assert payload.explain.estimated_cost == 12.0
    assert payload.truncated is True
    assert payload.summary.details == "Results were limited to 2 rows."
    assert payload.table is not None
    assert payload.table.rows == [
        {"department": "math", "total": 3},
        {"department": "physics", "total": 2},
    ]
    assert executed == [
        "EXPLAIN SELECT department, COUNT(*) AS total FROM student GROUP BY department",
        "SELECT department, COUNT(*) AS total FROM student GROUP BY department",
    ]


def test_execute_blocks_query_when_permission_policy_rejects_table() -> None:
    executed: list[str] = []
    settings = Settings(
        mysql_query_row_limit=5,
        mysql_explain_row_limit=1000,
        permission_allowed_tables="student",
    )
    service = MySQLGuardedQueryService(
        settings,
        connection_factory=lambda database: FakeConnection(
            query_rows=[{"department_id": 1}],
            executed=executed,
        ),
    )

    payload = service.execute_query(
        "campus",
        "SELECT * FROM department",
        request_context={
            "trace_id": "trace-denied",
            "request_id": "request-denied",
            "actor_id": "user-1",
        },
    )

    assert payload.allowed is False
    assert payload.error_code == "policy_denied"
    assert payload.reason == "Access to tables is not allowed: department"
    assert payload.table is None
    assert executed == []


def test_execute_applies_row_filter_and_masks_columns_with_policy_context() -> None:
    executed: list[str] = []
    settings = Settings(mysql_query_row_limit=5, mysql_explain_row_limit=1000)
    service = MySQLGuardedQueryService(
        settings,
        connection_factory=lambda database: FakeConnection(
            query_rows=[{"student_id": 1, "ssn": "123-45-6789"}],
            executed=executed,
        ),
    )

    payload = service.execute_query(
        "campus",
        "SELECT student_id, ssn FROM student",
        request_context={
            "trace_id": "trace-policy",
            "request_id": "request-policy",
            "actor_id": "user-7",
            "tenant_id": "tenant-7",
            "policy": {
                "allowed_tables": ["student"],
                "masked_columns": {"student": ["ssn"]},
                "row_filters": {"student": "{table}.tenant_id = 7"},
            },
        },
    )

    assert payload.allowed is True
    assert payload.trace_id == "trace-policy"
    assert payload.request_id == "request-policy"
    assert payload.audit_id
    assert payload.sql == "SELECT student_id, ssn FROM student"
    assert payload.effective_sql == "SELECT student_id, ssn FROM student WHERE student.tenant_id = 7"
    assert payload.permission is not None
    assert payload.permission.actor_id == "user-7"
    assert payload.audit.actor_id == "user-7"
    assert payload.audit.applied_row_filters == ["student.tenant_id = 7"]
    assert payload.audit.masked_columns == ["ssn"]
    assert payload.table is not None
    assert payload.table.rows == [{"student_id": 1, "ssn": "[REDACTED]"}]
    assert executed == ["SELECT student_id, ssn FROM student WHERE student.tenant_id = 7"]


def test_execute_returns_service_unavailable_when_mysql_connection_fails() -> None:
    settings = Settings(mysql_query_row_limit=5, mysql_explain_row_limit=1000)
    service = MySQLGuardedQueryService(
        settings,
        connection_factory=lambda database: (_ for _ in ()).throw(RuntimeError("mysql unavailable")),
    )

    payload = service.execute_query(
        "campus",
        "SELECT student_id FROM student",
        request_context={
            "trace_id": "trace-down",
            "request_id": "request-down",
            "actor_id": "user-9",
            "policy": {"allowed_tables": ["student"]},
        },
    )

    assert payload.allowed is False
    assert payload.trace_id == "trace-down"
    assert payload.request_id == "request-down"
    assert payload.error_code == "service_unavailable"
    assert payload.degraded is True
    assert payload.audit_id
    assert payload.reason == "mysql unavailable"
    assert payload.policy_summary["allowed_tables"] == ["student"]
