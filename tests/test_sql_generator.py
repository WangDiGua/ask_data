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


def test_sql_generator_builds_record_lookup_query() -> None:
    generator = SQLGeneratorService()
    plan = QueryPlanPayload(
        query_text="\u5de5\u53f787024\u7684\u51fa\u56fd\u8bb0\u5f55",
        intent_type="record_lookup",
        summary="Record lookup query",
        clarification_required=False,
        clarification_reason=None,
        candidate_tables=["t_bsdt_jzgygcg"],
        candidate_metrics=[],
        join_hints=[],
        selected_sql_asset_id=None,
        selected_sql=None,
        filters=[
            ResolvedFilterPayload(
                filter_id="staff_no_record_lookup",
                expression="t_bsdt_jzgygcg.ZGH = '87024'",
                source="identifier_record_lookup",
            )
        ],
        lookup_identifier=LookupIdentifierPayload(
            identifier_type="staff_no",
            label="\u5de5\u53f7",
            table="t_bsdt_jzgygcg",
            column="ZGH",
            expression="t_bsdt_jzgygcg.ZGH",
            value="87024",
        ),
        lookup_attributes=[
            ResolvedDimensionPayload(
                dimension_id="name",
                name="\u59d3\u540d",
                expression="t_bsdt_jzgygcg.XM",
                output_alias="name",
                table="t_bsdt_jzgygcg",
            ),
            ResolvedDimensionPayload(
                dimension_id="country_region",
                name="\u51fa\u8bbf\u56fd\u5bb6\u5730\u533a",
                expression="t_bsdt_jzgygcg.CFGJHDQ",
                output_alias="country_region",
                table="t_bsdt_jzgygcg",
            ),
        ],
        lookup_record_label="\u51fa\u56fd\u8bb0\u5f55",
    )

    payload = generator.generate(plan)

    assert payload.generated is True
    assert payload.strategy == "record_lookup"
    assert payload.reason is None
    assert payload.sql == (
        "SELECT t_bsdt_jzgygcg.XM AS name, t_bsdt_jzgygcg.CFGJHDQ AS country_region "
        "FROM t_bsdt_jzgygcg "
        "WHERE t_bsdt_jzgygcg.ZGH = '87024' "
        "ORDER BY CASE WHEN t_bsdt_jzgygcg.CFNF IS NULL OR t_bsdt_jzgygcg.CFNF = '' "
        "THEN 1 ELSE 0 END, t_bsdt_jzgygcg.CFNF DESC "
        "LIMIT 50"
    )


def test_sql_generator_builds_projection_query() -> None:
    generator = SQLGeneratorService()
    plan = QueryPlanPayload(
        query_text="列出烟台研究院在岗教师名单",
        intent_type="roster",
        answer_mode="roster",
        summary="Roster query",
        clarification_required=False,
        clarification_reason=None,
        candidate_tables=["dcemp"],
        candidate_metrics=[],
        join_hints=[],
        selected_sql_asset_id=None,
        selected_sql=None,
        filters=[
            ResolvedFilterPayload(
                filter_id="default_1",
                expression="dcemp.RYZTMC = '在岗'",
                source="core_table_default",
            ),
            ResolvedFilterPayload(
                filter_id="value_org_name",
                expression="dcemp.SZDWMC = '烟台研究院'",
                source="core_table_value_match",
            ),
        ],
        lookup_attributes=[
            ResolvedDimensionPayload(
                dimension_id="staff_no",
                name="工号",
                expression="dcemp.XGH",
                output_alias="staff_no",
                table="dcemp",
            ),
            ResolvedDimensionPayload(
                dimension_id="name",
                name="姓名",
                expression="dcemp.XM",
                output_alias="name",
                table="dcemp",
            ),
        ],
        result_limit=50,
        sort_expressions=["dcemp.SZDWMC ASC", "dcemp.XM ASC"],
    )

    payload = generator.generate(plan)

    assert payload.generated is True
    assert payload.strategy == "projection_query"
    assert payload.sql == (
        "SELECT dcemp.XGH AS staff_no, dcemp.XM AS name "
        "FROM dcemp "
        "WHERE dcemp.RYZTMC = '在岗' AND dcemp.SZDWMC = '烟台研究院' "
        "ORDER BY dcemp.SZDWMC ASC, dcemp.XM ASC "
        "LIMIT 50"
    )
