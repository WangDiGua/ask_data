from ndea.security.sql_guard import SQLGuard


def test_sql_guard_allows_simple_select() -> None:
    verdict = SQLGuard().validate("SELECT 1")
    assert verdict.allowed is True
    assert verdict.reason is None
    assert verdict.rejection_code is None
    assert verdict.statement_count == 1
    assert verdict.statement_type == "select"
    assert verdict.needs_explain is False


def test_sql_guard_blocks_delete() -> None:
    verdict = SQLGuard().validate("DELETE FROM student")
    assert verdict.allowed is False
    assert verdict.reason == "Only read-only SELECT statements are allowed"
    assert verdict.rejection_code == "unsupported_statement"
    assert verdict.statement_count == 1
    assert verdict.statement_type == "delete"
    assert verdict.needs_explain is False


def test_sql_guard_blocks_invalid_sql() -> None:
    verdict = SQLGuard().validate("SELEC 1")
    assert verdict.allowed is False
    assert verdict.reason == "SQL could not be parsed"
    assert verdict.rejection_code == "parse_error"
    assert verdict.statement_count == 0
    assert verdict.statement_type is None


def test_sql_guard_blocks_multiple_statements() -> None:
    verdict = SQLGuard().validate("SELECT 1; SELECT 2")
    assert verdict.allowed is False
    assert verdict.reason == "Only a single SQL statement is allowed"
    assert verdict.rejection_code == "multiple_statements"
    assert verdict.statement_count == 2
    assert verdict.statement_type is None


def test_sql_guard_marks_grouped_query_for_explain() -> None:
    verdict = SQLGuard().validate(
        "SELECT department, COUNT(*) AS total FROM student GROUP BY department"
    )
    assert verdict.allowed is True
    assert verdict.reason is None
    assert verdict.statement_count == 1
    assert verdict.statement_type == "select"
    assert verdict.needs_explain is True
