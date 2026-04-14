from ndea.main import main
from ndea.observability import DependencyHealth, ServiceHealthReport


class FakeHealthService:
    def __init__(self, readiness: bool) -> None:
        self._readiness = readiness

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
            readiness=self._readiness,
            dependencies=[
                DependencyHealth(
                    name="mysql",
                    healthy=self._readiness,
                    required=True,
                    details="ok" if self._readiness else "offline",
                )
            ],
        )


def test_main_liveness_check_returns_zero(monkeypatch, capsys) -> None:
    monkeypatch.setattr("ndea.main.get_health_service", lambda settings=None: FakeHealthService(True))

    exit_code = main(["--check", "liveness"])

    assert exit_code == 0
    assert '"service":"NDEA"' in capsys.readouterr().out


def test_main_readiness_check_returns_nonzero_when_dependency_is_down(
    monkeypatch,
    capsys,
) -> None:
    monkeypatch.setattr("ndea.main.get_health_service", lambda settings=None: FakeHealthService(False))

    exit_code = main(["--check", "readiness"])

    assert exit_code == 1
    assert '"readiness":false' in capsys.readouterr().out.lower()
