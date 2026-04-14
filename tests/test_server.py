from ndea.observability import DependencyHealth, ServiceHealthReport
from ndea.server import create_mcp
from ndea.tools.system import system_status


def test_create_mcp_returns_named_server() -> None:
    server = create_mcp()
    assert server.name == "NDEA"


def test_system_status_reports_stack_choices() -> None:
    class FakeHealthService:
        def system_status(self) -> ServiceHealthReport:
            return ServiceHealthReport(
                service="NDEA",
                liveness=True,
                readiness=True,
                dependencies=[
                    DependencyHealth(name="mysql", healthy=True, required=True, details="ok"),
                    DependencyHealth(name="qdrant", healthy=True, required=True, details="ok"),
                ],
            )

    import ndea.tools.system as system_module

    system_module.get_health_service = lambda settings=None: FakeHealthService()
    payload = system_status()
    assert payload["service"] == "NDEA"
    assert payload["readiness"] is True
    assert payload["dependencies"][0]["name"] == "mysql"
