from ndea.observability import DependencyHealth, ServiceHealthReport
from ndea.server import create_mcp
from ndea.tools import register_tools
from ndea.tools.system import system_status


def test_create_mcp_returns_named_server() -> None:
    server = create_mcp()
    assert server.name == "NDEA"


def test_register_tools_exposes_mcp_only_surface() -> None:
    class FakeMCP:
        def __init__(self) -> None:
            self.names: list[str] = []

        def tool(self, name: str):
            def decorator(func):
                self.names.append(name)
                return func

            return decorator

    fake = FakeMCP()
    register_tools(fake)

    assert "mcp_query_v2" in fake.names
    assert "execute_guarded_query" in fake.names
    assert "inspect_table_schema" in fake.names
    assert "system_status" in fake.names
    assert "ask_data_query" not in fake.names


def test_system_status_reports_stack_choices() -> None:
    class FakeHealthService:
        def system_status(self) -> ServiceHealthReport:
            return ServiceHealthReport(
                service="NDEA",
                liveness=True,
                readiness=True,
                dependencies=[
                    DependencyHealth(name="mysql", healthy=True, required=True, details="ok"),
                    DependencyHealth(name="milvus", healthy=True, required=True, details="ok"),
                ],
            )

    import ndea.tools.system as system_module

    system_module.get_health_service = lambda settings=None: FakeHealthService()
    payload = system_status()
    assert payload["service"] == "NDEA"
    assert payload["readiness"] is True
    assert payload["dependencies"][0]["name"] == "mysql"
