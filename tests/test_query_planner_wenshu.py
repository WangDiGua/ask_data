from ndea.planning import QueryPlannerService


class FakeVectorLocatorService:
    def __init__(self, payload) -> None:
        self.payload = payload

    def locate(self, query_text: str, query_vector: list[float], asset_types=None, limit=None):
        return self.payload


class FakeSQLRAGService:
    def __init__(self, payload) -> None:
        self.payload = payload

    def retrieve(self, query_text: str, query_vector: list[float], limit=None):
        return self.payload


def test_query_planner_resolves_dimension_value_filter_without_dimension_alias() -> None:
    planner = QueryPlannerService(
        vector_locator=FakeVectorLocatorService(
            {
                "matches": [],
                "metric_contracts": [
                    {
                        "metric_id": "active_student_count",
                        "name": "在校学生人数",
                        "aliases": ["学生人数"],
                        "base_table": "dcstu",
                        "measure_expression": "COUNT(*)",
                        "default_filters": ["dcstu.SFZX = '是'"],
                        "available_dimensions": ["student_college_name"],
                    }
                ],
                "dimension_contracts": [
                    {
                        "dimension_id": "student_college_name",
                        "name": "学生学院",
                        "aliases": ["学院", "院系"],
                        "table": "dcstu",
                        "column": "YXMC",
                        "expression": "dcstu.YXMC",
                        "output_alias": "college_name",
                        "sample_values": ["烟台研究院", "农学院"],
                    }
                ],
                "join_path_contracts": [],
                "time_semantics_catalog": [],
            }
        ),
        sql_rag=FakeSQLRAGService(
            {
                "candidates": [
                    {
                        "asset_id": "golden_sql:active_students_total",
                        "sql": "SELECT COUNT(*) AS total FROM dcstu WHERE dcstu.SFZX = '是'",
                        "metadata": {"metric_id": "active_student_count", "dimensions": [], "entity_scope": "student"},
                        "tables": ["dcstu"],
                        "score": 0.95,
                    }
                ]
            }
        ),
    )

    payload = planner.plan("烟台研究院在校学生人数", [0.1, 0.2])

    assert payload.metric_id == "active_student_count"
    assert payload.selected_sql is None
    assert [item.expression for item in payload.filters] == [
        "dcstu.SFZX = '是'",
        "dcstu.YXMC = '烟台研究院'",
    ]


def test_query_planner_avoids_rag_sql_when_query_has_specific_year_filter() -> None:
    planner = QueryPlannerService(
        vector_locator=FakeVectorLocatorService(
            {
                "matches": [],
                "metric_contracts": [
                    {
                        "metric_id": "teacher_outbound_count",
                        "name": "教职工因公出国记录数",
                        "aliases": ["教职工出访次数"],
                        "base_table": "t_bsdt_jzgygcg",
                        "measure_expression": "COUNT(*)",
                        "default_filters": [],
                        "available_dimensions": ["teacher_outbound_year"],
                        "time_field": "t_bsdt_jzgygcg.NF",
                        "supported_time_grains": ["year"],
                    }
                ],
                "dimension_contracts": [
                    {
                        "dimension_id": "teacher_outbound_year",
                        "name": "教职工出访年度",
                        "aliases": ["年度", "年份"],
                        "table": "t_bsdt_jzgygcg",
                        "column": "NF",
                        "expression": "t_bsdt_jzgygcg.NF",
                        "output_alias": "year",
                        "sample_values": ["2025", "2024"],
                    }
                ],
                "join_path_contracts": [],
                "time_semantics_catalog": [
                    {
                        "semantic_id": "teacher_outbound_year",
                        "name": "教职工出访年度",
                        "aliases": ["年度", "年份"],
                        "field": "t_bsdt_jzgygcg.NF",
                        "supported_grains": ["year"],
                        "default_grain": "year",
                        "comparison_modes": ["yoy"],
                    }
                ],
            }
        ),
        sql_rag=FakeSQLRAGService(
            {
                "candidates": [
                    {
                        "asset_id": "golden_sql:teacher_outbound_total",
                        "sql": "SELECT COUNT(*) AS total FROM t_bsdt_jzgygcg",
                        "metadata": {"metric_id": "teacher_outbound_count", "dimensions": [], "entity_scope": "faculty"},
                        "tables": ["t_bsdt_jzgygcg"],
                        "score": 0.95,
                    }
                ]
            }
        ),
    )

    payload = planner.plan("2025年教职工因公出国记录数", [0.1, 0.2])

    assert payload.metric_id == "teacher_outbound_count"
    assert payload.selected_sql is None
    assert payload.time_scope is not None
    assert payload.time_scope.value == "2025"
