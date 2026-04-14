from ndea.planning import QueryPlanPayload
from ndea.response.assembler import ResponseAssemblerService


def test_response_assembler_builds_line_chart_for_trend_results() -> None:
    service = ResponseAssemblerService()
    plan = QueryPlanPayload(
        query_text="Student count trend by month",
        intent_type="trend",
        summary="Identified trend query",
        clarification_required=False,
        clarification_reason=None,
        candidate_tables=["student"],
        candidate_metrics=["student count"],
        join_hints=[],
        selected_sql_asset_id="sql-1",
        selected_sql="SELECT month, total FROM monthly_student_counts",
    )
    execution = {
        "allowed": True,
        "summary": {"summary": "Returned 2 rows from campus", "details": None},
        "table": {
            "columns": ["month", "total"],
            "rows": [
                {"month": "2026-01", "total": 100},
                {"month": "2026-02", "total": 120},
            ],
            "total_rows": 2,
        },
    }

    payload = service.assemble(plan, execution)

    assert payload.text.summary == "Returned 2 rows from campus"
    assert payload.table is not None
    assert payload.chart is not None
    assert payload.chart.renderer == "echarts"
    assert payload.chart.option["series"][0]["type"] == "line"
    assert payload.chart.source[1]["total"] == 120


def test_response_assembler_uses_plan_summary_when_execution_is_missing() -> None:
    service = ResponseAssemblerService()
    plan = QueryPlanPayload(
        query_text="Compare graduation rates this year and last year",
        intent_type="comparison",
        summary="Need more semantic grounding before planning SQL",
        clarification_required=True,
        clarification_reason="Need more semantic grounding before planning SQL",
        candidate_tables=[],
        candidate_metrics=[],
        join_hints=[],
        selected_sql_asset_id=None,
        selected_sql=None,
    )

    payload = service.assemble(plan, None)

    assert payload.text.summary == "Need more semantic grounding before planning SQL"
    assert payload.table is None
    assert payload.chart is None
