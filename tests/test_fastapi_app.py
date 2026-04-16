from fastapi.testclient import TestClient

from ndea.http.api import create_http_app
from ndea.observability import DependencyHealth, ServiceHealthReport
from ndea.query_v2 import (
    ClarificationPayload,
    InteractionResult,
    PlanCandidate,
    QueryIR,
    QueryInterpretationPayload,
    QueryRequestV2,
    QueryResponseV2,
    SQLCandidate,
    VerificationReport,
)
from ndea.protocol import TablePayload, TextPayload


class FakeQueryServiceV2:
    def run(self, request: QueryRequestV2) -> QueryResponseV2:
        return build_response(request.query_text, request.database)

    def stream(self, request: QueryRequestV2):
        yield {"type": "node", "node": "interaction", "payload": {"query_text": request.query_text}}
        yield {"type": "node", "node": "rank_candidates", "payload": {"confidence": 0.91}}
        yield {"type": "final", "payload": build_response(request.query_text, request.database).model_dump(mode="json")}


class FakeHealthService:
    def liveness(self) -> ServiceHealthReport:
        return ServiceHealthReport(service="NDEA", liveness=True, readiness=True, dependencies=[])

    def readiness(self) -> ServiceHealthReport:
        return ServiceHealthReport(
            service="NDEA",
            liveness=True,
            readiness=True,
            dependencies=[
                DependencyHealth(name="mysql", healthy=True, required=True, details="ok"),
                DependencyHealth(name="milvus", healthy=True, required=True, details="ok"),
            ],
        )


def build_response(query_text: str, database: str | None) -> QueryResponseV2:
    plan = PlanCandidate(
        candidate_id="plan-1",
        intent_type="metric",
        answer_mode="aggregate",
        source="semantic-first",
        base_table="dcstu",
        candidate_tables=["dcstu"],
        confidence=0.92,
    )
    sql_candidate = SQLCandidate(
        candidate_id="sql-1",
        plan_candidate_id="plan-1",
        source="semantic-first",
        sql="SELECT COUNT(*) AS total FROM dcstu",
        score=0.92,
    )
    verification = VerificationReport(
        sql_candidate_id="sql-1",
        allowed=True,
        score=0.95,
        effective_sql="SELECT COUNT(*) AS total FROM dcstu",
    )
    return QueryResponseV2(
        session_id="session-1",
        interpretation=QueryInterpretationPayload(
            interaction=InteractionResult(
                query_text=query_text,
                normalized_query_text=query_text,
                rewritten_query_text=query_text,
            ),
            ir=QueryIR(intent_type="metric", metric="count", answer_mode="aggregate", confidence=0.8),
            selected_plan=plan,
            selected_sql=sql_candidate,
            verification=verification,
        ),
        answer=TextPayload(summary="全校总人数为 32000"),
        table=TablePayload(columns=["total"], rows=[{"total": 32000}], total_rows=1),
        sql="SELECT COUNT(*) AS total FROM dcstu",
        audit={"allowed": True},
        confidence=0.91,
        clarification=ClarificationPayload(required=False),
        learning_trace_id="learn-session-1",
        tool_trace=["interaction", "intent_parse", "respond"],
        executed=True,
    )


def test_fastapi_app_exposes_health_query_v2_and_stream() -> None:
    app = create_http_app(query_service=FakeQueryServiceV2(), health_service=FakeHealthService())
    client = TestClient(app)

    liveness = client.get("/health/liveness")
    assert liveness.status_code == 200
    assert liveness.json()["service"] == "NDEA"

    readiness = client.get("/health/readiness")
    assert readiness.status_code == 200
    assert readiness.json()["dependencies"][0]["name"] == "mysql"

    workflow = client.post(
        "/api/v2/query",
        json={
            "query_text": "我们学校有多少人",
            "database": "campus",
            "options": {"debug": True},
        },
    )
    assert workflow.status_code == 200
    assert workflow.json()["executed"] is True
    assert workflow.json()["answer"]["summary"] == "全校总人数为 32000"
    assert workflow.json()["sql"] == "SELECT COUNT(*) AS total FROM dcstu"

    with client.stream(
        "POST",
        "/api/v2/query/stream",
        json={"query_text": "我们学校有多少人", "database": "campus"},
    ) as response:
        body = "".join(response.iter_text())

    assert response.status_code == 200
    assert "event: interaction" in body
    assert "event: rank_candidates" in body
    assert "event: final" in body

    mounted_paths = {route.path for route in app.routes}
    assert "/mcp" in mounted_paths
    assert "/mcp-portal" in mounted_paths
