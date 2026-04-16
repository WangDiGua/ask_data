from __future__ import annotations

from typing import Any

from ndea.adapters.langfuse_tracer import LangfuseTracer
from ndea.config import Settings
from ndea.execution import QueryExecutorV2
from ndea.generation import CandidateSQLGenerator
from ndea.interaction import InteractionService
from ndea.learning import LearningStore, MilvusLearningSync
from ndea.metadata.models import ColumnSchema, TableSchemaDetail, TableSchemaSummary
from ndea.orchestration.query_graph_v2 import QueryGraphV2
from ndea.planning.candidate_plan_builder import CandidatePlanBuilder
from ndea.query_v2 import LearningEvent, PromotionCandidate, QueryRequestV2, QueryResponseV2
from ndea.ranking import CandidateRanker
from ndea.resolution import SchemaResolver
from ndea.semantic.campus_semantic_resolver import CampusSemanticResolver
from ndea.understanding import IntentParser
from ndea.verification import SQLVerifier


class FakeSchemaRepository:
    def list_tables(self, database: str) -> list[TableSchemaSummary]:
        return [
            TableSchemaSummary(name="dcstu", comment="学生基础信息"),
            TableSchemaSummary(name="dcemp", comment="教职工基础信息"),
            TableSchemaSummary(name="dcorg", comment="组织机构"),
            TableSchemaSummary(name="t_bsdt_jzgygcg", comment="教师因公出国记录"),
        ]

    def describe_table(self, database: str, table_name: str) -> TableSchemaDetail:
        columns_by_table = {
            "dcstu": ["XGH", "XM", "YXMC", "ZYMC", "PYCCMC", "SFZX", "XBMC", "ZZMMMC", "XSLBMC"],
            "dcemp": ["XGH", "XM", "SZDWMC", "RYZTMC", "XB"],
            "dcorg": ["xndwdm", "dwmc", "sfsy"],
            "t_bsdt_jzgygcg": ["ZGH", "XM", "NF", "PCDW", "ZC", "XZZW", "CFRWLX", "CFGJHDQ", "CFNF", "CJSJ", "RJSJ", "PJH"],
        }
        return TableSchemaDetail(
            database=database,
            table_name=table_name,
            columns=[
                ColumnSchema(
                    name=column,
                    data_type="varchar",
                    column_type="varchar(255)",
                    is_nullable=True,
                    comment=column,
                )
                for column in columns_by_table[table_name]
            ],
        )


class FakeGuardedQueryService:
    def execute_query(
        self,
        database: str,
        sql: str,
        request_context: dict[str, object] | None = None,
        policy_context: dict[str, object] | None = None,
    ) -> dict[str, Any]:
        lowered = sql.lower()
        if "from dcstu" in lowered and "group by" in lowered:
            return {
                "allowed": True,
                "summary": {"summary": "已按学院返回在校学生人数"},
                "table": {
                    "columns": ["college_name", "total"],
                    "rows": [
                        {"college_name": "计算机学院", "total": 1200},
                        {"college_name": "外国语学院", "total": 800},
                    ],
                    "total_rows": 2,
                },
                "audit": {"database": database, "sql": sql},
            }
        if "from dcemp" in lowered:
            return {
                "allowed": True,
                "summary": {"summary": "已返回教师名单"},
                "table": {
                    "columns": ["name", "org_name"],
                    "rows": [
                        {"name": "张三", "org_name": "计算机学院"},
                        {"name": "李四", "org_name": "外国语学院"},
                    ],
                    "total_rows": 2,
                },
                "audit": {"database": database, "sql": sql},
            }
        if "from t_bsdt_jzgygcg" in lowered and "zgh = '10001'" in lowered:
            return {
                "allowed": True,
                "summary": {"summary": "已返回工号 10001 的出访记录"},
                "table": {
                    "columns": ["name", "year", "country_region", "start_date", "return_date"],
                    "rows": [
                        {
                            "name": "张三",
                            "year": "2024",
                            "country_region": "日本",
                            "start_date": "2024-06-01",
                            "return_date": "2024-06-07",
                        }
                    ],
                    "total_rows": 1,
                },
                "audit": {"database": database, "sql": sql},
            }
        return {
            "allowed": True,
            "summary": {"summary": "查询执行成功"},
            "table": None,
            "audit": {"database": database, "sql": sql},
        }


class RecordingLearningStore(LearningStore):
    def __init__(self) -> None:
        self.record_calls: list[QueryResponseV2] = []
        self.persist_calls: list[QueryResponseV2] = []

    def record(
        self,
        response: QueryResponseV2,
        feedback_events: list[LearningEvent] | None = None,
    ) -> tuple[list[LearningEvent], list[PromotionCandidate]]:
        self.record_calls.append(response)
        promotions: list[PromotionCandidate] = []
        if response.sql:
            promotions.append(
                PromotionCandidate(
                    promotion_type="sql_case",
                    session_id=response.session_id,
                    confidence=response.confidence,
                    payload={"sql": response.sql},
                )
            )
        return [LearningEvent(event_type="recorded", session_id=response.session_id)], promotions

    def persist_response(
        self,
        response: QueryResponseV2,
        events: list[LearningEvent] | None = None,
        promotions: list[PromotionCandidate] | None = None,
    ) -> None:
        self.persist_calls.append(response)


def build_graph(learning_store: RecordingLearningStore | None = None) -> QueryGraphV2:
    schema_repository = FakeSchemaRepository()
    return QueryGraphV2(
        interaction_service=InteractionService(),
        intent_parser=IntentParser(),
        semantic_resolver=CampusSemanticResolver(),
        schema_resolver=SchemaResolver(schema_repository),
        plan_builder=CandidatePlanBuilder(),
        sql_generator=CandidateSQLGenerator(),
        sql_verifier=SQLVerifier(schema_repository=schema_repository),
        ranker=CandidateRanker(),
        executor=QueryExecutorV2(FakeGuardedQueryService()),
        learning_store=learning_store,
        milvus_sync=MilvusLearningSync(),
        tracer=LangfuseTracer(Settings()),
    )


def test_query_graph_v2_handles_student_aggregate_and_persists_learning() -> None:
    learning_store = RecordingLearningStore()
    graph = build_graph(learning_store)

    response = graph.run(
        QueryRequestV2(
            query_text="按学院统计在校学生人数",
            database="campus",
            options={"debug": True},
        )
    )

    assert response.executed is True
    assert response.answer.summary == "已按学院返回在校学生人数"
    assert response.table is not None
    assert response.table.columns == ["college_name", "total"]
    assert response.sql is not None and "FROM dcstu" in response.sql
    assert "dcstu.SFZX = '是' AND dcstu.SFZX = '是'" not in response.sql
    assert learning_store.record_calls
    assert learning_store.persist_calls


def test_query_graph_v2_handles_teacher_roster() -> None:
    graph = build_graph()

    response = graph.run(QueryRequestV2(query_text="教师名单", database="campus"))

    assert response.executed is True
    assert response.answer.summary == "已返回教师名单"
    assert response.table is not None
    assert response.table.columns == ["name", "org_name"]


def test_query_graph_v2_handles_teacher_outbound_record_with_identifier_context() -> None:
    graph = build_graph()

    response = graph.run(
        QueryRequestV2(
            query_text="这个老师的出访记录",
            database="campus",
            request_context={"recent_user_messages": ["工号 10001", "刚才那个老师"]},
            options={"debug": True},
        )
    )

    assert response.executed is True
    assert response.answer.summary == "已返回工号 10001 的出访记录"
    assert response.sql is not None and "FROM t_bsdt_jzgygcg" in response.sql
    assert "ZGH = '10001'" in response.sql
    assert response.table is not None
    assert response.table.rows[0]["country_region"] == "日本"


def test_query_graph_v2_returns_clarification_for_ambiguous_population_question() -> None:
    learning_store = RecordingLearningStore()
    graph = build_graph(learning_store)

    response = graph.run(
        QueryRequestV2(
            query_text="我们学校有多少人",
            database="campus",
            options={"debug": True},
        )
    )

    assert response.executed is False
    assert response.clarification.required is True
    assert "学生" in (response.clarification.question or "")
    assert learning_store.record_calls
