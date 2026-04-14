from ndea.planning import QueryPlanPayload
from ndea.sql_generation import SQLGeneratorService


def test_sql_generator_builds_count_query_for_metric_plan() -> None:
    generator = SQLGeneratorService()
    plan = QueryPlanPayload(
        query_text="How many active students are there?",
        intent_type="metric",
        summary="Metric query",
        clarification_required=False,
        clarification_reason=None,
        candidate_tables=["student"],
        candidate_metrics=["active student count"],
        join_hints=[],
        selected_sql_asset_id=None,
        selected_sql=None,
    )

    payload = generator.generate(plan)

    assert payload.generated is True
    assert payload.strategy == "count_metric"
    assert payload.reason is None
    assert payload.sql == "SELECT COUNT(*) AS total FROM student"


def test_sql_generator_declines_trend_plan_without_reusable_sql() -> None:
    generator = SQLGeneratorService()
    plan = QueryPlanPayload(
        query_text="Student count trend by month",
        intent_type="trend",
        summary="Trend query",
        clarification_required=False,
        clarification_reason=None,
        candidate_tables=["student"],
        candidate_metrics=["student count"],
        join_hints=[],
        selected_sql_asset_id=None,
        selected_sql=None,
    )

    payload = generator.generate(plan)

    assert payload.generated is False
    assert payload.sql is None
    assert payload.strategy is None
    assert payload.reason == "Planner needs richer time semantics for trend/comparison SQL generation"
