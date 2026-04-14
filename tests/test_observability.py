import json

from ndea.config import Settings
from ndea.observability import (
    AuditEvent,
    DependencyHealth,
    HealthService,
    StructuredLoggerAuditSink,
)


def test_structured_logger_audit_sink_writes_json_event() -> None:
    messages: list[str] = []
    sink = StructuredLoggerAuditSink(writer=messages.append)

    sink.emit(
        AuditEvent(
            audit_id="audit-1",
            trace_id="trace-1",
            request_id="request-1",
            actor_id="user-1",
            tenant_id="tenant-a",
            query_text="How many students are there?",
            intent_type="metric",
            tool_trace=["query_planner", "safe_executor"],
            selected_sql="SELECT COUNT(*) AS total FROM student",
            effective_sql="SELECT COUNT(*) AS total FROM student WHERE tenant_id = 1",
            sql_attempts=[{"attempt_number": 1, "sql": "SELECT COUNT(*) AS total FROM student"}],
            permission_actions={"masked_columns": [], "applied_row_filters": ["student.tenant_id = 1"]},
            degraded=False,
            final_status="succeeded",
            error_code=None,
            latency_ms=120,
            dependency_status={"mysql": "healthy", "qdrant": "healthy"},
        )
    )

    payload = json.loads(messages[0])
    assert payload["audit_id"] == "audit-1"
    assert payload["trace_id"] == "trace-1"
    assert payload["dependency_status"]["mysql"] == "healthy"


def test_health_service_marks_readiness_false_when_required_dependency_is_down() -> None:
    service = HealthService(
        dependency_checkers=[
            lambda: DependencyHealth(
                name="mysql",
                healthy=False,
                required=True,
                details="offline",
            ),
            lambda: DependencyHealth(
                name="qdrant",
                healthy=True,
                required=True,
                details="ok",
            ),
        ]
    )

    report = service.readiness()

    assert report.service == "NDEA"
    assert report.liveness is True
    assert report.readiness is False
    assert report.dependencies[0].details == "offline"


def test_health_service_marks_readiness_false_when_qdrant_collection_is_missing() -> None:
    service = HealthService(
        dependency_checkers=[
            lambda: DependencyHealth(
                name="mysql",
                healthy=True,
                required=True,
                details="ok",
            ),
            lambda: DependencyHealth(
                name="qdrant",
                healthy=False,
                required=True,
                details="collection semantic_assets not found",
            ),
        ]
    )

    report = service.readiness()

    assert report.readiness is False
    assert report.dependencies[1].name == "qdrant"
    assert report.dependencies[1].details == "collection semantic_assets not found"


def test_health_service_uses_qdrant_collection_existence_for_readiness(monkeypatch) -> None:
    class FakeConnection:
        def close(self) -> None:
            return None

    class FakeQdrantClient:
        def collection_exists(self, collection_name: str) -> bool:
            assert collection_name == "semantic_assets"
            return False

        def close(self) -> None:
            return None

    monkeypatch.setattr("ndea.observability.health.open_mysql_connection", lambda settings: FakeConnection())
    monkeypatch.setattr("ndea.observability.health.open_qdrant_client", lambda settings: FakeQdrantClient())

    report = HealthService(Settings()).readiness()

    assert report.readiness is False
    assert report.dependencies[1].name == "qdrant"
    assert report.dependencies[1].healthy is False
    assert report.dependencies[1].details == "collection semantic_assets not found"
