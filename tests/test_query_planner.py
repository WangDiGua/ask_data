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
        self.calls: list[dict[str, object]] = []

    def retrieve(
        self,
        query_text: str,
        query_vector: list[float],
        limit: int | None = None,
    ):
        self.calls.append(
            {
                "query_text": query_text,
                "query_vector": query_vector,
                "limit": limit,
            }
        )
        return self.payload


def test_query_planner_builds_plan_from_retrieval_signals() -> None:
    vector_payload = {
        "matches": [
            {
                "asset_id": "metric-1",
                "asset_type": "metric",
                "title": "active student count",
                "text": "Count of active students",
                "score": 0.95,
                "source": "metric_catalog",
                "metadata": {},
            },
            {
                "asset_id": "schema-1",
                "asset_type": "schema",
                "title": "student table",
                "text": "Student master records",
                "score": 0.88,
                "source": "schema",
                "metadata": {"table_name": "student"},
            },
            {
                "asset_id": "join-1",
                "asset_type": "join_path",
                "title": "student to department",
                "text": "student.department_id = department.id",
                "score": 0.81,
                "source": "join_guide",
                "metadata": {},
            },
        ]
    }
    sql_payload = {
        "candidates": [
            {
                "asset_id": "sql-1",
                "question": "How many active students are there?",
                "sql": "SELECT COUNT(*) AS total FROM student WHERE status = 'active'",
                "notes": "Uses active status filter",
                "score": 0.91,
                "source": "golden_sql",
                "tables": ["student"],
                "metadata": {},
            }
        ]
    }
    vector_locator = FakeVectorLocatorService(vector_payload)
    sql_rag = FakeSQLRAGService(sql_payload)
    planner = QueryPlannerService(vector_locator=vector_locator, sql_rag=sql_rag)

    payload = planner.plan(
        query_text="How many active students are there?",
        query_vector=[0.1, 0.2],
    )

    assert vector_locator.calls == [
        {
            "query_text": "How many active students are there?",
            "query_vector": [0.1, 0.2],
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
    assert sql_rag.calls == [
        {
            "query_text": "How many active students are there?",
            "query_vector": [0.1, 0.2],
            "limit": None,
        }
    ]
    assert payload.intent_type == "metric"
    assert payload.clarification_required is False
    assert payload.candidate_tables == ["student"]
    assert payload.candidate_metrics == ["active student count"]
    assert payload.join_hints == ["student to department"]
    assert payload.selected_sql_asset_id == "sql-1"
    assert payload.selected_sql == "SELECT COUNT(*) AS total FROM student WHERE status = 'active'"


def test_query_planner_requests_clarification_when_evidence_is_missing() -> None:
    planner = QueryPlannerService(
        vector_locator=FakeVectorLocatorService({"matches": []}),
        sql_rag=FakeSQLRAGService({"candidates": []}),
    )

    payload = planner.plan(
        query_text="Compare graduation rates this year and last year",
        query_vector=[0.3, 0.4],
    )

    assert payload.intent_type == "comparison"
    assert payload.clarification_required is True
    assert payload.clarification_reason == "Need more semantic grounding before planning SQL"
    assert payload.selected_sql is None
    assert payload.candidate_tables == []


def test_query_planner_marks_response_degraded_when_retrieval_backends_fail() -> None:
    class FailingVectorLocatorService:
        def locate(self, query_text: str, query_vector: list[float], asset_types=None, limit=None):
            raise RuntimeError("qdrant unavailable")

    class FailingSQLRAGService:
        def retrieve(self, query_text: str, query_vector: list[float], limit=None):
            raise RuntimeError("qdrant unavailable")

    planner = QueryPlannerService(
        vector_locator=FailingVectorLocatorService(),
        sql_rag=FailingSQLRAGService(),
    )

    payload = planner.plan(
        query_text="How many students are there?",
        query_vector=[0.4, 0.5],
    )

    assert payload.degraded is True
    assert payload.error_code == "planner_degraded"
    assert payload.clarification_required is True
    assert payload.selected_sql is None


def test_query_planner_routes_identifier_attribute_lookup_before_semantic_retrieval() -> None:
    vector_locator = FakeVectorLocatorService({"matches": []})
    sql_rag = FakeSQLRAGService({"candidates": []})
    planner = QueryPlannerService(vector_locator=vector_locator, sql_rag=sql_rag)

    payload = planner.plan(
        query_text="工号是87024的职称和姓名",
        query_vector=[0.8, 0.1],
    )

    assert vector_locator.calls == []
    assert sql_rag.calls == []
    assert payload.intent_type == "attribute_lookup"
    assert payload.clarification_required is False
    assert payload.lookup_identifier is not None
    assert payload.lookup_identifier.table == "t_bsdt_jzgygcg"
    assert payload.lookup_identifier.column == "ZGH"
    assert payload.lookup_identifier.value == "87024"
    assert [item.name for item in payload.lookup_attributes] == ["姓名", "职称"]
    assert payload.filters[0].expression == "t_bsdt_jzgygcg.ZGH = '87024'"


def test_query_planner_requests_clarification_when_identifier_attributes_span_multiple_tables() -> None:
    planner = QueryPlannerService(
        vector_locator=FakeVectorLocatorService({"matches": []}),
        sql_rag=FakeSQLRAGService({"candidates": []}),
    )

    payload = planner.plan(
        query_text="工号87024的所在单位和职称",
        query_vector=[0.8, 0.1],
    )

    assert payload.intent_type == "attribute_lookup"
    assert payload.clarification_required is True
    assert payload.clarification_reason == "Requested attributes are not available from a single authoritative table"


def test_query_planner_routes_identifier_record_lookup_before_semantic_retrieval() -> None:
    vector_locator = FakeVectorLocatorService({"matches": []})
    sql_rag = FakeSQLRAGService({"candidates": []})
    planner = QueryPlannerService(vector_locator=vector_locator, sql_rag=sql_rag)

    payload = planner.plan(
        query_text="\u5de5\u53f787024\u7684\u51fa\u56fd\u8bb0\u5f55",
        query_vector=[0.9, 0.2],
    )

    assert vector_locator.calls == []
    assert sql_rag.calls == []
    assert payload.intent_type == "record_lookup"
    assert payload.clarification_required is False
    assert payload.lookup_identifier is not None
    assert payload.lookup_identifier.table == "t_bsdt_jzgygcg"
    assert payload.lookup_identifier.column == "ZGH"
    assert payload.lookup_identifier.value == "87024"
    assert payload.candidate_tables == ["t_bsdt_jzgygcg"]
    assert payload.lookup_record_label == "\u51fa\u56fd\u8bb0\u5f55"
    assert payload.filters[0].expression == "t_bsdt_jzgygcg.ZGH = '87024'"


def test_query_planner_clarifies_bare_identifier_lookup() -> None:
    vector_locator = FakeVectorLocatorService({"matches": []})
    sql_rag = FakeSQLRAGService({"candidates": []})
    planner = QueryPlannerService(vector_locator=vector_locator, sql_rag=sql_rag)

    payload = planner.plan(
        query_text="查一下87024的信息",
        query_vector=[0.2, 0.3],
    )

    assert vector_locator.calls == []
    assert sql_rag.calls == []
    assert payload.answer_mode == "clarification"
    assert payload.clarification_required is True
    assert "工号或学号" in payload.clarification_questions[0]

