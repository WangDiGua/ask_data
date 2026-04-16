from ndea.config import Settings
from ndea.generation import CandidateSQLGenerator
from ndea.interaction import InteractionService
from ndea.learning import MilvusLearningSync, MySQLLearningStore
from ndea.metadata.models import ColumnSchema, TableSchemaDetail, TableSchemaSummary
from ndea.planning.candidate_plan_builder import CandidatePlanBuilder
from ndea.portal.service import PortalQueryService
from ndea.query_v2 import (
    ClarificationPayload,
    InteractionResult,
    PlanCandidate,
    QueryIR,
    QueryInterpretationPayload,
    QueryResponseV2,
    SQLCandidate,
)
from ndea.protocol import TablePayload, TextPayload
from ndea.ranking import CandidateRanker
from ndea.resolution import SchemaResolver
from ndea.semantic.campus_semantic_resolver import CampusSemanticResolver
from ndea.understanding import IntentParser
from ndea.verification import SQLVerifier


class FakeSchemaRepository:
    def list_tables(self, database: str) -> list[TableSchemaSummary]:
        return [
            TableSchemaSummary(name="student_profile", comment="学生基础信息"),
            TableSchemaSummary(name="faculty_profile", comment="教师基础信息"),
            TableSchemaSummary(name="t_bsdt_jzgygcg", comment="教师因公出国记录"),
        ]

    def describe_table(self, database: str, table_name: str) -> TableSchemaDetail:
        if table_name == "student_profile":
            return TableSchemaDetail(
                database=database,
                table_name=table_name,
                columns=[
                    ColumnSchema(name="id", data_type="varchar", column_type="varchar(32)", is_nullable=False, comment="主键"),
                    ColumnSchema(name="college_name", data_type="varchar", column_type="varchar(64)", is_nullable=True, comment="学院名称"),
                ],
            )
        if table_name == "t_bsdt_jzgygcg":
            return TableSchemaDetail(
                database=database,
                table_name=table_name,
                columns=[
                    ColumnSchema(name="ZGH", data_type="varchar", column_type="varchar(32)", is_nullable=True, comment="工号"),
                    ColumnSchema(name="XM", data_type="varchar", column_type="varchar(64)", is_nullable=True, comment="姓名"),
                    ColumnSchema(name="NF", data_type="varchar", column_type="varchar(8)", is_nullable=True, comment="年份"),
                    ColumnSchema(name="CFGJHDQ", data_type="varchar", column_type="varchar(64)", is_nullable=True, comment="国家地区"),
                ],
            )
        return TableSchemaDetail(
            database=database,
            table_name=table_name,
            columns=[ColumnSchema(name="teacher_name", data_type="varchar", column_type="varchar(64)", is_nullable=True, comment="教师姓名")],
        )


def test_intent_parser_recognizes_campus_terms() -> None:
    payload = IntentParser().parse("按学院统计2024学年在校学生人数前10")

    assert payload.intent_type == "ranking"
    assert payload.entity_scope == "student"
    assert payload.metric == "count"
    assert payload.dimensions == ["college"]
    assert payload.filters == ["在校"]
    assert payload.time_scope == {
        "scope_type": "academic_year",
        "field": None,
        "value": "2024",
        "label": "2024学年",
    }
    assert payload.limit == 10


def test_intent_parser_extracts_identifier_and_record_term() -> None:
    payload = IntentParser().parse("工号10001 出访记录")

    assert payload.intent_type == "detail"
    assert payload.answer_mode == "detail"
    assert payload.entity_scope == "faculty"
    assert payload.identifiers == [{"type": "工号", "value": "10001"}]
    assert "teacher_outbound" in payload.campus_terms


def test_campus_semantic_resolver_maps_student_scope() -> None:
    ir = QueryIR(
        intent_type="metric",
        entity_scope="student",
        metric="count",
        dimensions=["college"],
        filters=["在校"],
        answer_mode="aggregate",
        confidence=0.8,
    )

    payload = CampusSemanticResolver().resolve(ir)

    assert payload.base_table == "dcstu"
    assert "dcstu" in payload.candidate_tables
    assert payload.filters[-1] == "dcstu.SFZX = '是'"


def test_campus_semantic_resolver_maps_teacher_outbound_record() -> None:
    ir = QueryIR(
        intent_type="detail",
        entity_scope="faculty",
        answer_mode="detail",
        campus_terms=["faculty", "teacher_outbound"],
        identifiers=[{"type": "工号", "value": "10001"}],
        confidence=0.8,
    )

    payload = CampusSemanticResolver().resolve(ir)

    assert payload.base_table == "t_bsdt_jzgygcg"
    assert "t_bsdt_jzgygcg.ZGH = '10001'" in payload.filters


def test_schema_resolver_can_fallback_from_live_metadata() -> None:
    ir = QueryIR(intent_type="metric", metric="count", dimensions=["college"], answer_mode="aggregate", confidence=0.3)
    payload = SchemaResolver(FakeSchemaRepository()).resolve("campus", ir, "按学院统计学生人数")

    assert payload.base_table == "student_profile"
    assert payload.candidate_tables == ["student_profile"]
    assert payload.dimensions[0]["dimension_id"] == "college_name"


def test_candidate_sql_generator_produces_multiple_candidates() -> None:
    plan = PlanCandidate(
        candidate_id="plan-1",
        intent_type="metric",
        answer_mode="aggregate",
        source="semantic-first",
        base_table="dcstu",
        candidate_tables=["dcstu"],
        dimensions=[{"dimension_id": "college_name", "expression": "dcstu.YXMC", "output_alias": "college_name"}],
        filters=["dcstu.SFZX = '是'"],
        confidence=0.88,
    )

    payload = CandidateSQLGenerator().generate(
        query_text="按学院统计在校学生人数",
        ir=QueryIR(intent_type="metric", metric="count", answer_mode="aggregate", confidence=0.8),
        plans=[plan],
    )

    assert len(payload) >= 2
    assert payload[0].sql.startswith("SELECT")
    assert any(item.source == "template-fallback" for item in payload)


def test_candidate_sql_generator_uses_default_projection_for_teacher_roster() -> None:
    plan = PlanCandidate(
        candidate_id="plan-1",
        intent_type="detail",
        answer_mode="detail",
        source="semantic-first",
        base_table="dcemp",
        candidate_tables=["dcemp"],
        filters=["dcemp.RYZTMC = '在岗'"],
        confidence=0.88,
    )

    payload = CandidateSQLGenerator().generate(
        query_text="列出教师名单",
        ir=QueryIR(intent_type="detail", entity_scope="faculty", answer_mode="detail", confidence=0.8),
        plans=[plan],
    )

    structured_sql = next(item.sql for item in payload if item.source == "semantic-first")
    assert "dcemp.XGH AS staff_no" in structured_sql
    assert "dcemp.SZDWMC AS org_name" in structured_sql
    assert "SELECT *" not in structured_sql


def test_sql_verifier_rejects_unknown_columns() -> None:
    plan = PlanCandidate(
        candidate_id="plan-1",
        intent_type="metric",
        answer_mode="aggregate",
        source="schema-first",
        base_table="student_profile",
        candidate_tables=["student_profile"],
        confidence=0.7,
    )
    candidate = SQLCandidate(
        candidate_id="sql-1",
        plan_candidate_id="plan-1",
        source="schema-first",
        sql="SELECT student_profile.bad_column AS total FROM student_profile",
        score=0.7,
    )

    payload = SQLVerifier(schema_repository=FakeSchemaRepository()).verify("campus", plan, candidate)

    assert payload.allowed is False
    assert any(issue.code == "unknown_column" for issue in payload.issues)


def test_candidate_ranker_selects_best_verified_candidate() -> None:
    plan = PlanCandidate(
        candidate_id="plan-1",
        intent_type="metric",
        answer_mode="aggregate",
        source="semantic-first",
        base_table="dcstu",
        candidate_tables=["dcstu"],
        confidence=0.9,
    )
    low = SQLCandidate(candidate_id="sql-low", plan_candidate_id="plan-1", source="template-fallback", sql="SELECT 1", score=0.3)
    high = SQLCandidate(candidate_id="sql-high", plan_candidate_id="plan-1", source="semantic-first", sql="SELECT COUNT(*) AS total FROM dcstu", score=0.9)
    verifier = SQLVerifier()
    low_report = verifier.verify(None, plan, low)
    high_report = verifier.verify(None, plan, high)

    decision = CandidateRanker().rank([plan], [low, high], [low_report, high_report])

    assert decision.selected_sql_candidate_id == "sql-high"
    assert decision.confidence > 0.8


def test_learning_store_bootstrap_and_sync() -> None:
    settings = Settings()
    store = MySQLLearningStore(settings=settings)
    statements = store.bootstrap_schema()
    response = QueryResponseV2(
        session_id="session-1",
        interpretation=QueryInterpretationPayload(
            interaction=InteractionResult(
                query_text="按学院统计学生人数",
                normalized_query_text="按学院统计学生人数",
                rewritten_query_text="按学院统计学生人数",
            ),
            ir=QueryIR(intent_type="metric", metric="count", answer_mode="aggregate", confidence=0.9),
        ),
        answer=TextPayload(summary="ok"),
        sql="SELECT COUNT(*) AS total FROM dcstu",
        audit={},
        confidence=0.95,
        clarification=ClarificationPayload(required=False),
        learning_trace_id="learn-session-1",
        executed=True,
    )

    events, promotions = store.record(response)
    sync_payload = MilvusLearningSync().sync(promotions)

    assert any("query_session" in statement for statement in statements)
    assert events[0].event_type == "query_session_recorded"
    assert promotions[0].promotion_type == "sql_case"
    assert sync_payload[0]["promotion_type"] == "sql_case"


def test_interaction_service_resolves_reference_from_recent_identifier() -> None:
    payload = InteractionService().process(
        "这个老师的出访记录",
        request_context={"recent_user_messages": ["工号 10001", "刚才那个老师"]},
    )

    assert payload.references_resolved is True
    assert payload.rewritten_query_text == "工号10001 出访记录"


def test_portal_service_skips_visualization_for_detail_results() -> None:
    response = QueryResponseV2(
        session_id="session-3",
        interpretation=QueryInterpretationPayload(
            interaction=InteractionResult(
                query_text="列出教师名单",
                normalized_query_text="列出教师名单",
                rewritten_query_text="列出教师名单",
            ),
            ir=QueryIR(intent_type="detail", entity_scope="faculty", answer_mode="detail", confidence=0.9),
        ),
        answer=TextPayload(summary="已返回教师名单"),
        table=TablePayload(
            columns=["staff_no", "name", "org_name", "score"],
            rows=[
                {"staff_no": "10001", "name": "张三", "org_name": "计算机学院", "score": None},
                {"staff_no": "10002", "name": "李四", "org_name": "外国语学院", "score": None},
            ],
            total_rows=2,
        ),
        sql="SELECT dcemp.XGH AS staff_no FROM dcemp",
        audit={},
        confidence=0.9,
        clarification=ClarificationPayload(required=False),
        executed=True,
    )

    service = PortalQueryService(query_service=type("StaticService", (), {"run": lambda self, request: response})())
    payload = service.query("列出教师名单", database="campus")

    assert payload.visualization is None


def test_learning_store_persists_multiple_learning_tables() -> None:
    class FakeCursor:
        def __init__(self, recorder: list[tuple[str, tuple[object, ...] | None]]) -> None:
            self._recorder = recorder

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def execute(self, statement: str, params=None) -> None:
            self._recorder.append((statement, params))

    class FakeConnection:
        def __init__(self, recorder: list[tuple[str, tuple[object, ...] | None]]) -> None:
            self._recorder = recorder
            self.commits = 0

        def cursor(self):
            return FakeCursor(self._recorder)

        def commit(self) -> None:
            self.commits += 1

        def close(self) -> None:
            return None

    statements: list[tuple[str, tuple[object, ...] | None]] = []
    admin = FakeConnection(statements)
    learning = FakeConnection(statements)
    response = QueryResponseV2(
        session_id="session-2",
        interpretation=QueryInterpretationPayload(
            interaction=InteractionResult(
                query_text="按学院统计学生人数",
                normalized_query_text="按学院统计学生人数",
                rewritten_query_text="按学院统计学生人数",
            ),
            ir=QueryIR(intent_type="metric", metric="count", dimensions=["college"], answer_mode="aggregate", confidence=0.92),
            selected_plan=PlanCandidate(
                candidate_id="plan-1",
                intent_type="metric",
                answer_mode="aggregate",
                source="semantic-first",
                base_table="dcstu",
                candidate_tables=["dcstu"],
                confidence=0.93,
            ),
            selected_sql=SQLCandidate(
                candidate_id="sql-1",
                plan_candidate_id="plan-1",
                source="semantic-first",
                sql="SELECT COUNT(*) AS total FROM dcstu",
                score=0.93,
            ),
        ),
        answer=TextPayload(summary="ok"),
        sql="SELECT COUNT(*) AS total FROM dcstu",
        audit={"allowed": True},
        confidence=0.95,
        clarification=ClarificationPayload(required=False),
        learning_trace_id="learn-session-2",
        executed=True,
    )
    store = MySQLLearningStore(
        settings=Settings(),
        connection_factory=lambda database: admin if database == "wenshu_db" else learning,
    )

    events, promotions = store.record(response)
    store.persist_response(response, events=events, promotions=promotions)

    sql_text = "\n".join(statement for statement, _ in statements)
    assert "CREATE DATABASE IF NOT EXISTS" in sql_text
    assert "`query_session`" in sql_text
    assert "`interaction_turn`" in sql_text
    assert "`ir_snapshot`" in sql_text
    assert "`plan_candidate`" in sql_text
    assert "`sql_candidate`" in sql_text
    assert "`execution_result`" in sql_text
    assert "`feedback_event`" in sql_text
    assert "`promotion_queue`" in sql_text
    assert "`sql_case_memory`" in sql_text
    assert "`alias_memory`" in sql_text
