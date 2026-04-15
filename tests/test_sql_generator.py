from ndea.planning import QueryPlanPayload
from ndea.planning.models import LookupIdentifierPayload, ResolvedDimensionPayload, ResolvedFilterPayload
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


def test_sql_generator_builds_attribute_lookup_query() -> None:
    generator = SQLGeneratorService()
    plan = QueryPlanPayload(
        query_text="工号是87024的职称",
        intent_type="attribute_lookup",
        summary="Attribute lookup query",
        clarification_required=False,
        clarification_reason=None,
        candidate_tables=["t_bsdt_jzgygcg", "dcemp"],
        candidate_metrics=[],
        join_hints=[],
        selected_sql_asset_id=None,
        selected_sql=None,
        filters=[
            ResolvedFilterPayload(
                filter_id="staff_no_lookup",
                expression="t_bsdt_jzgygcg.ZGH = '87024'",
                source="identifier_lookup",
            )
        ],
        lookup_identifier=LookupIdentifierPayload(
            identifier_type="staff_no",
            label="工号",
            table="t_bsdt_jzgygcg",
            column="ZGH",
            expression="t_bsdt_jzgygcg.ZGH",
            value="87024",
        ),
        lookup_attributes=[
            ResolvedDimensionPayload(
                dimension_id="title_name",
                name="职称",
                expression="t_bsdt_jzgygcg.ZC",
                output_alias="title_name",
                table="t_bsdt_jzgygcg",
            )
        ],
    )

    payload = generator.generate(plan)

    assert payload.generated is True
    assert payload.strategy == "attribute_lookup"
    assert payload.reason is None
    assert payload.sql == (
        "SELECT DISTINCT t_bsdt_jzgygcg.ZC AS title_name "
        "FROM t_bsdt_jzgygcg "
        "WHERE t_bsdt_jzgygcg.ZGH = '87024' "
        "LIMIT 20"
    )
