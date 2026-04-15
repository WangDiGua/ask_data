from fastapi.testclient import TestClient

from ndea.http.api import create_http_app
from ndea.observability import DependencyHealth, ServiceHealthReport


class FakeWorkflowService:
    def run(
        self,
        query_text: str,
        query_vector: list[float],
        database: str | None = None,
        execute: bool = False,
        request_context: dict[str, object] | None = None,
        policy_context: dict[str, object] | None = None,
    ) -> dict[str, object]:
        return {
            "trace_id": "trace-http",
            "request_id": "request-http",
            "query_text": query_text,
            "database": database,
            "executed": execute,
            "tool_trace": [
                "langgraph_planner",
                "sql_advisor",
                "langgraph_executor",
                "response_assembler",
            ],
            "degraded": False,
            "error_code": None,
            "audit_id": "audit-http",
            "policy_summary": {"allowed_tables": ["student"]},
            "plan": {
                "query_text": query_text,
                "intent_type": "metric",
                "summary": "Planned a metric query",
                "clarification_required": False,
            },
            "response_text": {"summary": "校园总人数为 32000", "details": None},
            "response_table": {
                "columns": ["total"],
                "rows": [{"total": 32000}],
                "total_rows": 1,
            },
            "response_chart": None,
        }

    def stream(
        self,
        query_text: str,
        query_vector: list[float],
        database: str | None = None,
        execute: bool = False,
        request_context: dict[str, object] | None = None,
        policy_context: dict[str, object] | None = None,
    ):
        yield {"type": "node", "node": "planner", "payload": {"summary": "Planned a metric query"}}
        yield {"type": "node", "node": "advisor", "payload": {"strategy": "vanna_style_examples"}}
        yield {"type": "final", "payload": self.run(query_text, query_vector, database, execute, request_context, policy_context)}


class FakeHealthService:
    def liveness(self) -> ServiceHealthReport:
        return ServiceHealthReport(
            service="NDEA",
            liveness=True,
            readiness=True,
            dependencies=[],
        )

    def readiness(self) -> ServiceHealthReport:
        return ServiceHealthReport(
            service="NDEA",
            liveness=True,
            readiness=True,
            dependencies=[
                DependencyHealth(name="mysql", healthy=True, required=True, details="ok"),
                DependencyHealth(name="qdrant", healthy=True, required=True, details="ok"),
            ],
        )


def test_fastapi_app_exposes_health_workflow_and_stream() -> None:
    app = create_http_app(
        workflow_service=FakeWorkflowService(),
        health_service=FakeHealthService(),
    )
    client = TestClient(app)

    liveness = client.get("/health/liveness")
    assert liveness.status_code == 200
    assert liveness.json()["service"] == "NDEA"

    readiness = client.get("/health/readiness")
    assert readiness.status_code == 200
    assert readiness.json()["dependencies"][0]["name"] == "mysql"

    workflow = client.post(
        "/api/query-workflow",
        json={
            "query_text": "我们学校有多少人",
            "query_vector": [0.1, 0.2],
            "database": "campus",
            "execute": True,
        },
    )
    assert workflow.status_code == 200
    assert workflow.json()["executed"] is True
    assert workflow.json()["response_text"]["summary"] == "校园总人数为 32000"

    with client.stream(
        "POST",
        "/api/query-workflow/stream",
        json={
            "query_text": "我们学校有多少人",
            "query_vector": [0.1, 0.2],
            "database": "campus",
            "execute": True,
        },
    ) as response:
        body = "".join(response.iter_text())

    assert response.status_code == 200
    assert "event: workflow" in body
    assert '"type":"node"' in body
    assert '"type":"final"' in body

    mounted_paths = {route.path for route in app.routes}
    assert "/mcp" in mounted_paths
    assert "/mcp-portal" in mounted_paths
