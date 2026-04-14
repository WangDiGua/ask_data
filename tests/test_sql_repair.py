from ndea.planning import QueryPlanPayload
from ndea.sql_generation import SQLGeneratorService, SQLRepairService


def test_sql_repair_service_falls_back_to_generated_sql_for_unknown_column() -> None:
    repairer = SQLRepairService(generator=SQLGeneratorService())
    plan = QueryPlanPayload(
        query_text="How many students are there?",
        intent_type="metric",
        summary="Metric query",
        clarification_required=False,
        clarification_reason=None,
        candidate_tables=["student"],
        candidate_metrics=["student count"],
        join_hints=[],
        selected_sql_asset_id="sql-legacy",
        selected_sql="SELECT bad_column FROM student",
    )

    payload = repairer.repair(
        plan=plan,
        failed_sql="SELECT bad_column FROM student",
        failure_reason="Unknown column 'bad_column' in 'field list'",
        attempt_number=1,
    )

    assert payload.repaired is True
    assert payload.trigger == "unknown_column"
    assert payload.strategy == "repair_unknown_column"
    assert payload.reason is None
    assert payload.sql == "SELECT COUNT(*) AS total FROM student"


def test_sql_repair_service_declines_permission_conflict() -> None:
    repairer = SQLRepairService(generator=SQLGeneratorService())
    plan = QueryPlanPayload(
        query_text="Show department details",
        intent_type="detail",
        summary="Detail query",
        clarification_required=False,
        clarification_reason=None,
        candidate_tables=["department"],
        candidate_metrics=[],
        join_hints=[],
        selected_sql_asset_id="sql-dept",
        selected_sql="SELECT * FROM department",
    )

    payload = repairer.repair(
        plan=plan,
        failed_sql="SELECT * FROM department",
        failure_reason="Access to tables is not allowed: department",
        attempt_number=1,
    )

    assert payload.repaired is False
    assert payload.trigger == "permission_conflict"
    assert payload.strategy is None
    assert payload.sql is None
    assert payload.reason == "Execution failure is not repairable under current policy"


def test_sql_repair_service_prefers_ranked_candidate_before_generic_regeneration() -> None:
    repairer = SQLRepairService(generator=SQLGeneratorService())
    plan = QueryPlanPayload(
        query_text="按学院统计在校生人数",
        intent_type="metric",
        summary="Complex metric query",
        clarification_required=False,
        clarification_reason=None,
        candidate_tables=["student", "department"],
        candidate_metrics=["在校生人数"],
        join_hints=[],
        selected_sql_asset_id="sql-bad",
        selected_sql="SELECT dept_name, COUNT(*) AS total FROM student GROUP BY dept_name",
        ranked_sql_candidates=[
            {
                "asset_id": "sql-bad",
                "sql": "SELECT dept_name, COUNT(*) AS total FROM student GROUP BY dept_name",
                "compatibility_score": 0.92,
                "selection_reason": "compatible_metric_dimension_time",
            },
            {
                "asset_id": "sql-good",
                "sql": "SELECT department.name AS college_name, COUNT(*) AS total FROM student JOIN department ON student.department_id = department.id GROUP BY department.name",
                "compatibility_score": 0.88,
                "selection_reason": "compatible_metric_dimension",
            },
        ],
    )

    payload = repairer.repair(
        plan=plan,
        failed_sql="SELECT dept_name, COUNT(*) AS total FROM student GROUP BY dept_name",
        failure_reason="Unknown column 'dept_name' in 'field list'",
        attempt_number=1,
    )

    assert payload.repaired is True
    assert payload.trigger == "unknown_column"
    assert payload.strategy == "repair_ranked_candidate"
    assert payload.reason is None
    assert payload.sql == "SELECT department.name AS college_name, COUNT(*) AS total FROM student JOIN department ON student.department_id = department.id GROUP BY department.name"
