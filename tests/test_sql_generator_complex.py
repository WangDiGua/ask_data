from ndea.planning.models import QueryPlanPayload
from ndea.sql_generation import SQLGeneratorService


def test_sql_generator_builds_grouped_join_query_from_structured_plan() -> None:
    generator = SQLGeneratorService()
    plan = QueryPlanPayload(
        query_text="按学院统计2024学年在校生人数",
        intent_type="metric",
        summary="Structured multi-table metric query",
        clarification_required=False,
        clarification_reason=None,
        candidate_tables=["student", "department"],
        candidate_metrics=["在校生人数"],
        join_hints=[],
        selected_sql_asset_id=None,
        selected_sql=None,
        metric_id="metric-enrolled-students",
        entity_scope="student",
        dimensions=[
            {
                "dimension_id": "college",
                "name": "学院",
                "expression": "department.name",
                "output_alias": "college_name",
            }
        ],
        filters=[
            {
                "filter_id": "active_status",
                "expression": "student.status = 'active'",
                "source": "metric_default",
            }
        ],
        time_scope={
            "scope_type": "academic_year",
            "field": "student.academic_year",
            "value": "2024",
            "label": "2024学年",
        },
        join_plan=[
            {
                "join_id": "student_department",
                "join_sql": "JOIN department ON student.department_id = department.id",
                "left_table": "student",
                "right_table": "department",
            }
        ],
        chosen_strategy="metric_contract",
        resolved_metric={
            "metric_id": "metric-enrolled-students",
            "name": "在校生人数",
            "base_table": "student",
            "measure_expression": "COUNT(*)",
            "default_filters": ["student.status = 'active'"],
        },
    )

    payload = generator.generate(plan)

    assert payload.generated is True
    assert payload.strategy == "structured_metric_contract"
    assert payload.reason is None
    assert payload.sql == (
        "SELECT department.name AS college_name, COUNT(*) AS total "
        "FROM student "
        "JOIN department ON student.department_id = department.id "
        "WHERE student.status = 'active' AND student.academic_year = '2024' "
        "GROUP BY department.name "
        "ORDER BY total DESC"
    )
