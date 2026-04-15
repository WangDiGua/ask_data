from ndea.planning.query_router import (
    build_identifier_clarification_plan,
    build_registry_metric_plan,
    build_roster_or_detail_plan,
    rewrite_query_text,
)


def test_rewrite_query_text_reuses_previous_identifier() -> None:
    rewritten = rewrite_query_text(
        "我要他出国的记录",
        {"recent_user_messages": ["工号87024的姓名", "我要他出国的记录"]},
    )

    assert rewritten == "工号87024的出国记录"


def test_build_registry_metric_plan_for_student_political_status() -> None:
    payload = build_registry_metric_plan("按政治面貌统计在校学生人数")

    assert payload is not None
    assert payload.answer_mode == "aggregate"
    assert payload.metric_id == "registry:dcstu:count"
    assert payload.candidate_tables == ["dcstu"]
    assert payload.dimensions[0].dimension_id == "political_status_name"
    assert payload.filters[0].expression == "dcstu.SFZX = '是'"


def test_build_identifier_clarification_plan_for_bare_identifier() -> None:
    payload = build_identifier_clarification_plan("查一下87024的信息")

    assert payload is not None
    assert payload.answer_mode == "clarification"
    assert payload.clarification_required is True
    assert "工号或学号" in payload.clarification_questions[0]
    assert payload.resolved_entities == [
        {"type": "identifier_value", "value": "87024", "label": "编号"}
    ]


def test_build_roster_or_detail_plan_for_staff_roster() -> None:
    payload = build_roster_or_detail_plan("列出烟台研究院在岗教师名单")

    assert payload is not None
    assert payload.intent_type == "roster"
    assert payload.answer_mode == "roster"
    assert payload.candidate_tables == ["dcemp"]
    assert any(item.expression == "dcemp.RYZTMC = '在岗'" for item in payload.filters)
    assert any(item.expression == "dcemp.SZDWMC = '烟台研究院'" for item in payload.filters)


def test_build_roster_or_detail_plan_for_visiting_expert_year() -> None:
    payload = build_roster_or_detail_plan("列出2024年来访专家名单")

    assert payload is not None
    assert payload.candidate_tables == ["t_gjc_lfzj"]
    assert payload.intent_type == "roster"
    assert any(item.expression == "t_gjc_lfzj.ND = '2024'" for item in payload.filters)
