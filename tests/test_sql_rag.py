from ndea.config import Settings
from ndea.vector.sql_rag import SQLRAGService


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


def test_sql_rag_service_normalizes_and_sorts_candidates() -> None:
    store = FakeVectorStore(
        [
            {
                "id": "sql-1",
                "distance": 0.72,
                "entity": {
                    "asset_id": "sql-1",
                    "question": "How many active students are there?",
                    "sql": "SELECT COUNT(*) AS total FROM student WHERE status = 'active'",
                    "notes": "Uses active status filter",
                    "source": "golden_sql",
                    "tables": ["student"],
                },
            },
            {
                "id": "sql-2",
                "distance": 0.91,
                "entity": {
                    "asset_id": "sql-2",
                    "question": "How many enrolled students by department?",
                    "sql": "SELECT department, COUNT(*) AS total FROM student GROUP BY department",
                    "notes": "Department grouping pattern",
                    "source": "golden_sql",
                    "tables": ["student", "department"],
                    "metadata": {"pattern": "group_by"},
                },
            },
        ]
    )
    service = SQLRAGService(
        Settings(qdrant_search_limit=6, qdrant_hybrid_enabled=False),
        store=store,
    )

    payload = service.retrieve(
        query_text="active students by department",
        query_vector=[0.2, 0.3],
        limit=2,
    )

    assert store.calls == [
        {
            "query_vector": [0.2, 0.3],
            "asset_types": ["golden_sql"],
            "limit": 2,
        }
    ]
    assert payload.query_text == "active students by department"
    assert payload.total_candidates == 2
    assert payload.summary == "Found 2 golden SQL candidates"
    assert payload.candidates[0].asset_id == "sql-2"
    assert payload.candidates[0].score == 0.91
    assert payload.candidates[0].tables == ["student", "department"]
    assert payload.candidates[0].metadata == {"pattern": "group_by"}
    assert payload.candidates[1].question == "How many active students are there?"


def test_sql_rag_service_handles_empty_results_with_default_limit() -> None:
    store = FakeVectorStore([])
    service = SQLRAGService(
        Settings(qdrant_search_limit=4, qdrant_hybrid_enabled=False),
        store=store,
    )

    payload = service.retrieve(
        query_text="graduation rate trend",
        query_vector=[0.5, 0.6],
    )

    assert store.calls == [
        {
            "query_vector": [0.5, 0.6],
            "asset_types": ["golden_sql"],
            "limit": 4,
        }
    ]
    assert payload.total_candidates == 0
    assert payload.summary == "No golden SQL candidates found"
    assert payload.candidates == []


def test_sql_rag_service_hybrid_reranks_candidate_with_better_keyword_overlap() -> None:
    store = FakeVectorStore(
        [
            {
                "id": "sql-1",
                "distance": 0.94,
                "entity": {
                    "asset_id": "sql-1",
                    "question": "Show the student roster",
                    "sql": "SELECT * FROM student",
                    "notes": "Student detail listing",
                    "source": "golden_sql",
                    "tables": ["student"],
                },
            },
            {
                "id": "sql-2",
                "distance": 0.82,
                "entity": {
                    "asset_id": "sql-2",
                    "question": "How many active students are there by department?",
                    "sql": "SELECT department.name, COUNT(*) AS total FROM student JOIN department ON student.department_id = department.id WHERE student.status = 'active' GROUP BY department.name",
                    "notes": "Department grouping pattern for active students",
                    "source": "golden_sql",
                    "tables": ["student", "department"],
                    "metadata": {"business_terms": ["active students", "department"]},
                },
            },
        ]
    )
    service = SQLRAGService(
        Settings(
            qdrant_search_limit=2,
            qdrant_hybrid_enabled=True,
            qdrant_hybrid_overfetch_limit=10,
        ),
        store=store,
    )

    payload = service.retrieve(
        query_text="active students by department",
        query_vector=[0.2, 0.3],
        limit=2,
    )

    assert store.calls == [
        {
            "query_vector": [0.2, 0.3],
            "asset_types": ["golden_sql"],
            "limit": 10,
        }
    ]
    assert payload.candidates[0].asset_id == "sql-2"
    assert payload.candidates[0].hybrid_score is not None
    assert payload.candidates[0].keyword_score is not None
    assert payload.candidates[0].hybrid_score > payload.candidates[0].score

