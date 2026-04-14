from __future__ import annotations

from collections.abc import Callable

from pydantic import BaseModel, Field

from ndea.config import Settings
from ndea.metadata.mysql_client import open_mysql_connection
from ndea.vector.qdrant_client import open_qdrant_client


class DependencyHealth(BaseModel):
    name: str
    healthy: bool
    required: bool = True
    details: str | None = None


class ServiceHealthReport(BaseModel):
    service: str = "NDEA"
    liveness: bool
    readiness: bool
    dependencies: list[DependencyHealth] = Field(default_factory=list)


class HealthService:
    def __init__(
        self,
        settings: Settings | None = None,
        dependency_checkers: list[Callable[[], DependencyHealth]] | None = None,
    ) -> None:
        self._settings = settings or Settings()
        self._dependency_checkers = dependency_checkers or _default_dependency_checkers(self._settings)

    def liveness(self) -> ServiceHealthReport:
        return ServiceHealthReport(service=self._settings.app_name, liveness=True, readiness=True)

    def readiness(self) -> ServiceHealthReport:
        dependencies = [checker() for checker in self._dependency_checkers]
        readiness = all(dep.healthy or not dep.required for dep in dependencies)
        return ServiceHealthReport(
            service=self._settings.app_name,
            liveness=True,
            readiness=readiness,
            dependencies=dependencies,
        )

    def system_status(self) -> ServiceHealthReport:
        return self.readiness()


def get_health_service(settings: Settings | None = None) -> HealthService:
    resolved = settings or Settings()
    return HealthService(resolved)


def _default_dependency_checkers(settings: Settings) -> list[Callable[[], DependencyHealth]]:
    return [
        lambda: _check_mysql_dependency(settings),
        lambda: _check_qdrant_dependency(settings),
    ]


def _check_mysql_dependency(settings: Settings) -> DependencyHealth:
    if not settings.enable_query_execution:
        return DependencyHealth(name="mysql", healthy=True, required=False, details="disabled")
    try:
        connection = open_mysql_connection(settings)
        connection.close()
        return DependencyHealth(name="mysql", healthy=True, required=True, details="ok")
    except Exception as exc:  # pragma: no cover - exercised via tests with injected services
        return DependencyHealth(name="mysql", healthy=False, required=True, details=str(exc))


def _check_qdrant_dependency(settings: Settings) -> DependencyHealth:
    if not settings.enable_semantic_retrieval:
        return DependencyHealth(name="qdrant", healthy=True, required=False, details="disabled")
    try:
        client = open_qdrant_client(settings)
        if hasattr(client, "collection_exists"):
            exists = client.collection_exists(settings.qdrant_collection)
            if not exists:
                if hasattr(client, "close"):
                    client.close()
                return DependencyHealth(
                    name="qdrant",
                    healthy=False,
                    required=True,
                    details=f"collection {settings.qdrant_collection} not found",
                )
        if hasattr(client, "close"):
            client.close()
        return DependencyHealth(name="qdrant", healthy=True, required=True, details="ok")
    except Exception as exc:  # pragma: no cover - exercised via tests with injected services
        return DependencyHealth(name="qdrant", healthy=False, required=True, details=str(exc))
