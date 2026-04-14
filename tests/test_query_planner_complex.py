from ndea.planning import QueryPlannerService


class FakeVectorLocatorService:
    def __init__(self, payload) -> None:
        self.payload = payload
        self.calls: list[dict[str, object]] = []

    def locate(
        self,
        query_text: str,
        query_vector: list[float],
        asset_types: list[str] | None = None,
        limit: int | None = None,
    ):
        self.calls.append(
            {
                "query_text": query_text,
                "query_vector": query_vector,
                "asset_types": asset_types,
                "limit": limit,
            }
        )
        return self.payload


class FakeSQLRAGService:
    def __init__(self, payload) -> None:
        self.payload = payload

    def retrieve(
        self,
        query_text: str,
        query_vector: list[float],
        limit: int | None = None,
    ):
        return self.payload


def test_query_planner_builds_structured_multi_table_plan_from_metric_contracts() -> None:
    vector_locator = FakeVectorLocatorService(
        {
            "matches": [],
            "metric_contracts": [
                {
                    "metric_id": "metric-enrolled-students",
                    "name": "在校生人数",
                    "score": 0.97,
                    "aliases": ["在校生人数", "学生人数"],
                    "business_definition": "截至统计时间在校的学生人数",
                    "base_table": "student",
                    "measure_expression": "COUNT(*)",
                    "default_filters": ["student.status = 'active'"],
                    "available_dimensions": ["college"],
                    "time_field": "student.academic_year",
                    "supported_time_grains": ["academic_year", "semester"],
                    "join_path_ids": ["student_department"],
                }
            ],
            "dimension_contracts": [
                {
                    "dimension_id": "college",
                    "name": "学院",
                    "aliases": ["学院", "院系"],
                    "table": "department",
                    "column": "name",
                    "expression": "department.name",
                    "groupable": True,
                    "output_alias": "college_name",
                }
            ],
            "join_path_contracts": [
                {
                    "join_id": "student_department",
                    "left_table": "student",
                    "right_table": "department",
                    "join_type": "INNER",
                    "join_condition": "student.department_id = department.id",
                    "join_sql": "JOIN department ON student.department_id = department.id",
                    "cardinality": "many_to_one",
                }
            ],
            "time_semantics_catalog": [
                {
                    "semantic_id": "academic_year",
                    "name": "学年",
                    "aliases": ["学年"],
                    "field": "student.academic_year",
                    "supported_grains": ["academic_year", "semester"],
                    "default_grain": "academic_year",
                    "comparison_modes": ["yoy"],
                }
            ],
        }
    )
    planner = QueryPlannerService(
        vector_locator=vector_locator,
        sql_rag=FakeSQLRAGService({"candidates": []}),
    )

    payload = planner.plan(
        query_text="按学院统计2024学年在校生人数",
        query_vector=[0.2, 0.4],
    )

    assert vector_locator.calls == [
        {
            "query_text": "按学院统计2024学年在校生人数",
            "query_vector": [0.2, 0.4],
            "asset_types": [
                "metric",
                "schema",
                "join_path",
                "metric_contract",
                "dimension_contract",
                "time_semantics",
            ],
            "limit": None,
        }
    ]
    assert payload.metric_id == "metric-enrolled-students"
    assert payload.clarification_required is False
    assert payload.entity_scope == "student"
    assert payload.candidate_tables == ["student", "department"]
    assert payload.dimensions[0].dimension_id == "college"
    assert payload.dimensions[0].expression == "department.name"
    assert payload.filters[0].expression == "student.status = 'active'"
    assert payload.time_scope is not None
    assert payload.time_scope.value == "2024"
    assert payload.time_scope.scope_type == "academic_year"
    assert payload.join_plan[0].join_id == "student_department"
    assert payload.join_plan[0].join_sql == "JOIN department ON student.department_id = department.id"
    assert payload.chosen_strategy == "metric_contract"
    assert payload.confidence == 0.97


def test_query_planner_returns_clarification_questions_when_entity_scope_is_ambiguous() -> None:
    planner = QueryPlannerService(
        vector_locator=FakeVectorLocatorService(
            {
                "matches": [],
                "metric_contracts": [
                    {
                        "metric_id": "metric-campus-population",
                        "name": "校园人数",
                        "aliases": ["学校有多少人", "校园人数", "总人数"],
                        "business_definition": "校园在册人数",
                        "base_table": "student",
                        "measure_expression": "COUNT(*)",
                        "default_filters": ["student.status = 'active'"],
                        "entity_scope_options": ["student", "faculty", "all_people"],
                        "requires_entity_scope": True,
                    }
                ],
                "dimension_contracts": [],
                "join_path_contracts": [],
                "time_semantics_catalog": [],
            }
        ),
        sql_rag=FakeSQLRAGService({"candidates": []}),
    )

    payload = planner.plan(
        query_text="我们学校有多少人",
        query_vector=[0.3, 0.5],
    )

    assert payload.metric_id == "metric-campus-population"
    assert payload.clarification_required is True
    assert payload.entity_scope is None
    assert payload.error_code == "clarification_required"
    assert payload.clarification_questions == [
        "你想查询学生、教职工，还是全体在册人员？"
    ]


def test_query_planner_uses_request_context_planning_answers_to_resolve_previous_clarification() -> None:
    planner = QueryPlannerService(
        vector_locator=FakeVectorLocatorService(
            {
                "matches": [],
                "metric_contracts": [
                    {
                        "metric_id": "metric-campus-population",
                        "name": "校园人数",
                        "aliases": ["学校有多少人", "校园人数", "总人数"],
                        "business_definition": "校园在册人数",
                        "base_table": "student",
                        "measure_expression": "COUNT(*)",
                        "default_filters": ["student.status = 'active'"],
                        "entity_scope_options": ["student", "faculty", "all_people"],
                        "requires_entity_scope": True,
                    }
                ],
                "dimension_contracts": [],
                "join_path_contracts": [],
                "time_semantics_catalog": [],
            }
        ),
        sql_rag=FakeSQLRAGService({"candidates": []}),
    )

    payload = planner.plan(
        query_text="我们学校有多少人",
        query_vector=[0.3, 0.5],
        request_context={"planning_context": {"entity_scope": "student"}},
    )

    assert payload.metric_id == "metric-campus-population"
    assert payload.clarification_required is False
    assert payload.entity_scope == "student"
    assert payload.error_code is None
    assert payload.clarification_questions == []


def test_query_planner_reranks_sql_candidates_by_metric_and_dimension_compatibility() -> None:
    planner = QueryPlannerService(
        vector_locator=FakeVectorLocatorService(
            {
                "matches": [],
                "metric_contracts": [
                    {
                        "metric_id": "metric-enrolled-students",
                        "name": "在校生人数",
                        "score": 0.97,
                        "aliases": ["在校生人数", "学生人数"],
                        "business_definition": "截至统计时间在校的学生人数",
                        "base_table": "student",
                        "measure_expression": "COUNT(*)",
                        "default_filters": ["student.status = 'active'"],
                        "available_dimensions": ["college"],
                        "time_field": "student.academic_year",
                        "supported_time_grains": ["academic_year", "semester"],
                        "join_path_ids": ["student_department"],
                    }
                ],
                "dimension_contracts": [
                    {
                        "dimension_id": "college",
                        "name": "学院",
                        "aliases": ["学院", "院系"],
                        "table": "department",
                        "column": "name",
                        "expression": "department.name",
                        "groupable": True,
                        "output_alias": "college_name",
                    }
                ],
                "join_path_contracts": [
                    {
                        "join_id": "student_department",
                        "left_table": "student",
                        "right_table": "department",
                        "join_type": "INNER",
                        "join_condition": "student.department_id = department.id",
                        "join_sql": "JOIN department ON student.department_id = department.id",
                        "cardinality": "many_to_one",
                    }
                ],
                "time_semantics_catalog": [
                    {
                        "semantic_id": "academic_year",
                        "name": "学年",
                        "aliases": ["学年"],
                        "field": "student.academic_year",
                        "supported_grains": ["academic_year", "semester"],
                        "default_grain": "academic_year",
                        "comparison_modes": ["yoy"],
                    }
                ],
            }
        ),
        sql_rag=FakeSQLRAGService(
            {
                "candidates": [
                    {
                        "asset_id": "sql-generic",
                        "question": "How many students are there?",
                        "sql": "SELECT COUNT(*) AS total FROM student",
                        "score": 0.95,
                        "tables": ["student"],
                        "metadata": {
                            "metric_id": "metric-campus-population",
                            "dimensions": [],
                            "time_grains": [],
                        },
                    },
                    {
                        "asset_id": "sql-structured",
                        "question": "按学院统计2024学年在校生人数",
                        "sql": "SELECT department.name AS college_name, COUNT(*) AS total FROM student JOIN department ON student.department_id = department.id WHERE student.status = 'active' AND student.academic_year = '2024' GROUP BY department.name ORDER BY total DESC",
                        "score": 0.84,
                        "tables": ["student", "department"],
                        "metadata": {
                            "metric_id": "metric-enrolled-students",
                            "dimensions": ["college"],
                            "time_grains": ["academic_year"],
                        },
                    },
                ]
            }
        ),
    )

    payload = planner.plan(
        query_text="按学院统计2024学年在校生人数",
        query_vector=[0.2, 0.4],
    )

    assert payload.selected_sql_asset_id == "sql-structured"
    assert payload.selected_candidate_reason == "compatible_metric_dimension_time"
    assert payload.ranked_sql_candidates[0].asset_id == "sql-structured"
    assert payload.ranked_sql_candidates[0].compatibility_score > payload.ranked_sql_candidates[1].compatibility_score
