from __future__ import annotations

from typing import Any

from ndea.query_v2 import QueryOptions, SQLCandidate


class QueryExecutorV2:
    def __init__(self, guarded_query_service: Any) -> None:
        self._guarded_query_service = guarded_query_service

    def execute(
        self,
        database: str | None,
        candidate: SQLCandidate | None,
        request_context: dict[str, object] | None,
        policy_context: dict[str, object] | None,
        options: QueryOptions,
    ) -> dict[str, Any] | None:
        if candidate is None or database is None or options.dry_run:
            return None
        payload = self._guarded_query_service.execute_query(
            database=database,
            sql=candidate.sql,
            request_context=request_context,
            policy_context=policy_context,
        )
        if hasattr(payload, "model_dump"):
            return payload.model_dump(mode="json")
        return payload
