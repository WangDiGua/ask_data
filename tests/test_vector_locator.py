from ndea.config import Settings
from ndea.vector.locator import VectorLocatorService


class FakeVectorStore:
    def __init__(self, hits: list[dict[str, object]]) -> None:
        self._hits = hits
        self.calls: list[dict[str, object]] = []

    def search(
        self,
        query_vector: list[float],
        asset_types: list[str] | None,
        limit: int,
    ) -> list[dict[str, object]]:
        self.calls.append(
            {
                "query_vector": query_vector,
                "asset_types": asset_types,
                "limit": limit,
            }
        )
        return list(self._hits)


def test_vector_locator_normalizes_and_sorts_matches() -> None:
    store = FakeVectorStore(
        [
            {
                "id": "schema-1",
                "distance": 0.61,
                "entity": {
                    "asset_id": "schema-1",
                    "asset_type": "schema",
                    "title": "student table",
                    "text": "Student enrollment facts",
                    "source": "schema",
                },
            },
            {
                "id": "metric-1",
                "distance": 0.93,
                "entity": {
                    "asset_id": "metric-1",
                    "asset_type": "metric",
                    "title": "student count",
                    "text": "Count of active students",
                    "source": "metric_catalog",
                    "metadata": {"grain": "daily"},
                },
            },
        ]
    )
    service = VectorLocatorService(
        Settings(qdrant_search_limit=7, qdrant_hybrid_enabled=False),
        store=store,
    )

    payload = service.locate(
        query_text="active student count",
        query_vector=[0.1, 0.2],
        asset_types=["metric", "schema"],
        limit=2,
    )

    assert store.calls == [
        {
            "query_vector": [0.1, 0.2],
            "asset_types": ["metric", "schema"],
            "limit": 2,
        }
    ]
    assert payload.query_text == "active student count"
    assert payload.asset_types == ["metric", "schema"]
    assert payload.total_matches == 2
    assert payload.summary == "Found 2 semantic matches"
    assert payload.matches[0].asset_id == "metric-1"
    assert payload.matches[0].score == 0.93
    assert payload.matches[0].metadata == {"grain": "daily"}
    assert payload.matches[1].asset_type == "schema"


def test_vector_locator_uses_default_limit_and_handles_empty_results() -> None:
    store = FakeVectorStore([])
    service = VectorLocatorService(
        Settings(qdrant_search_limit=3, qdrant_hybrid_enabled=False),
        store=store,
    )

    payload = service.locate(
        query_text="faculty status",
        query_vector=[0.5, 0.1],
    )

    assert store.calls == [
        {
            "query_vector": [0.5, 0.1],
            "asset_types": None,
            "limit": 3,
        }
    ]
    assert payload.asset_types == []
    assert payload.total_matches == 0
    assert payload.summary == "No semantic matches found"
    assert payload.matches == []


def test_vector_locator_extracts_structured_semantic_contracts() -> None:
    store = FakeVectorStore(
        [
            {
                "id": "metric-contract-1",
                "distance": 0.97,
                "entity": {
                    "asset_id": "metric-contract-1",
                    "asset_type": "metric_contract",
                    "title": "enrolled student count",
                    "text": "count of enrolled students",
                    "metadata": {
                        "metric_contract": {
                            "metric_id": "metric-enrolled-students",
                            "name": "enrolled student count",
                            "aliases": ["active student count", "student population"],
                            "business_definition": "students currently enrolled at the reporting snapshot",
                            "base_table": "student",
                            "measure_expression": "COUNT(*)",
                            "default_filters": ["student.status = 'active'"],
                            "available_dimensions": ["college"],
                            "time_field": "student.academic_year",
                            "supported_time_grains": ["academic_year", "semester"],
                            "join_path_ids": ["student_department"],
                        }
                    },
                },
            },
            {
                "id": "dimension-contract-1",
                "distance": 0.93,
                "entity": {
                    "asset_id": "dimension-contract-1",
                    "asset_type": "dimension_contract",
                    "title": "college",
                    "text": "college dimension",
                    "metadata": {
                        "dimension_contract": {
                            "dimension_id": "college",
                            "name": "college",
                            "aliases": ["college", "department"],
                            "table": "department",
                            "column": "name",
                            "expression": "department.name",
                            "groupable": True,
                            "output_alias": "college_name",
                        }
                    },
                },
            },
            {
                "id": "join-1",
                "distance": 0.9,
                "entity": {
                    "asset_id": "join-1",
                    "asset_type": "join_path",
                    "title": "student to department",
                    "text": "student.department_id = department.id",
                    "metadata": {
                        "join_path_contract": {
                            "join_id": "student_department",
                            "left_table": "student",
                            "right_table": "department",
                            "join_type": "INNER",
                            "join_condition": "student.department_id = department.id",
                            "join_sql": "JOIN department ON student.department_id = department.id",
                            "cardinality": "many_to_one",
                        }
                    },
                },
            },
            {
                "id": "time-1",
                "distance": 0.89,
                "entity": {
                    "asset_id": "time-1",
                    "asset_type": "time_semantics",
                    "title": "academic year",
                    "text": "academic year time semantics",
                    "metadata": {
                        "time_semantics": {
                            "semantic_id": "academic_year",
                            "name": "academic year",
                            "aliases": ["academic year", "school year"],
                            "field": "student.academic_year",
                            "supported_grains": ["academic_year", "semester"],
                            "default_grain": "academic_year",
                            "comparison_modes": ["yoy"],
                        }
                    },
                },
            },
        ]
    )
    service = VectorLocatorService(Settings(qdrant_search_limit=5), store=store)

    payload = service.locate(
        query_text="enrolled student count by college for 2024 academic year",
        query_vector=[0.2, 0.4],
    )

    assert payload.metric_contracts[0].metric_id == "metric-enrolled-students"
    assert payload.metric_contracts[0].base_table == "student"
    assert payload.dimension_contracts[0].dimension_id == "college"
    assert payload.dimension_contracts[0].output_alias == "college_name"
    assert payload.join_path_contracts[0].join_id == "student_department"
    assert payload.join_path_contracts[0].join_sql == "JOIN department ON student.department_id = department.id"
    assert payload.time_semantics_catalog[0].semantic_id == "academic_year"
    assert payload.time_semantics_catalog[0].field == "student.academic_year"


def test_vector_locator_hybrid_reranks_semantic_contract_by_alias_overlap() -> None:
    store = FakeVectorStore(
        [
            {
                "id": "schema-1",
                "distance": 0.94,
                "entity": {
                    "asset_id": "schema-1",
                    "asset_type": "schema",
                    "title": "student table",
                    "text": "Student base records",
                    "source": "schema",
                    "metadata": {"table_name": "student"},
                },
            },
            {
                "id": "metric-contract-1",
                "distance": 0.82,
                "entity": {
                    "asset_id": "metric-contract-1",
                    "asset_type": "metric_contract",
                    "title": "enrolled student count",
                    "text": "count of enrolled students",
                    "source": "metric_catalog",
                    "metadata": {
                        "metric_contract": {
                            "metric_id": "metric-enrolled-students",
                            "name": "enrolled student count",
                            "aliases": ["student population", "active student count"],
                            "business_definition": "total number of enrolled students",
                            "base_table": "student",
                            "measure_expression": "COUNT(*)",
                        }
                    },
                },
            },
        ]
    )
    service = VectorLocatorService(
        Settings(
            qdrant_search_limit=2,
            qdrant_hybrid_enabled=True,
            qdrant_hybrid_overfetch_limit=10,
        ),
        store=store,
    )

    payload = service.locate(
        query_text="active student count",
        query_vector=[0.4, 0.6],
        asset_types=["schema", "metric_contract"],
    )

    assert store.calls == [
        {
            "query_vector": [0.4, 0.6],
            "asset_types": ["schema", "metric_contract"],
            "limit": 10,
        }
    ]
    assert payload.matches[0].asset_id == "metric-contract-1"
    assert payload.matches[0].hybrid_score is not None
    assert payload.matches[0].keyword_score is not None
    assert payload.matches[0].hybrid_score > payload.matches[0].score
