from ndea.security.permission import TablePermissionChecker


def test_permission_checker_allows_query_when_tables_are_in_allowlist() -> None:
    checker = TablePermissionChecker(allowed_tables={"student", "department"})

    verdict = checker.check(
        "SELECT * FROM student JOIN department ON student.department_id = department.id"
    )

    assert verdict.allowed is True
    assert verdict.reason is None


def test_permission_checker_rejects_query_when_table_is_not_allowed() -> None:
    checker = TablePermissionChecker(allowed_tables={"student"})

    verdict = checker.check(
        "SELECT * FROM student JOIN department ON student.department_id = department.id"
    )

    assert verdict.allowed is False
    assert verdict.reason == "Access to tables is not allowed: department"


def test_permission_checker_rejects_blocked_column_reference() -> None:
    checker = TablePermissionChecker(
        allowed_tables={"student"},
        blocked_columns={"student": {"ssn"}},
    )

    verdict = checker.check("SELECT student_id, ssn FROM student")

    assert verdict.allowed is False
    assert verdict.reason == "Access to columns is not allowed: student.ssn"
    assert verdict.blocked_columns == ["student.ssn"]


def test_permission_checker_rewrites_sql_with_row_filter_and_mask_policy() -> None:
    checker = TablePermissionChecker(
        allowed_tables={"student"},
        masked_columns={"student": {"ssn"}},
        row_filters={"student": "{table}.tenant_id = 7"},
        actor_id="user-7",
    )

    verdict = checker.check("SELECT student_id, ssn FROM student WHERE status = 'active'")

    assert verdict.allowed is True
    assert verdict.reason is None
    assert verdict.actor_id == "user-7"
    assert verdict.masked_columns == ["student.ssn"]
    assert verdict.applied_row_filters == ["student.tenant_id = 7"]
    assert verdict.rewritten_sql == (
        "SELECT student_id, ssn FROM student WHERE status = 'active' "
        "AND student.tenant_id = 7"
    )
