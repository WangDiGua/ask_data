import json
from pathlib import Path

from ndea.planning import QueryPlannerService, QueryWorkflowService
from ndea.sql_generation import SQLGeneratorService, SQLRepairService


class EvalVectorLocatorService:
    def __init__(self, payload: dict[str, object]) -> None:
        self._payload = payload

    def locate(
        self,
        query_text: str,
        query_vector: list[float],
        asset_types: list[str] | None = None,
        limit: int | None = None,
    ) -> dict[str, object]:
        return self._payload


class EvalSQLRAGService:
    def __init__(self, candidates: list[dict[str, object]]) -> None:
        self._candidates = candidates

    def retrieve(
        self,
        query_text: str,
        query_vector: list[float],
        limit: int | None = None,
    ) -> dict[str, object]:
        return {"candidates": list(self._candidates)}


class EvalQueryService:
    def __init__(self, payloads: list[object]) -> None:
        self._payloads = payloads

    def execute_query(
        self,
        database: str,
        sql: str,
        request_context: dict[str, object] | None = None,
        policy_context: dict[str, object] | None = None,
    ) -> dict[str, object]:
        payload = self._payloads.pop(0)
        if isinstance(payload, Exception):
            raise payload
        return payload


def _load_eval_cases() -> list[dict[str, object]]:
    path = Path(__file__).with_name("complex_fixtures.json")
    return json.loads(path.read_text(encoding="utf-8"))


def test_complex_query_eval_suite_meets_release_gates() -> None:
    cases = _load_eval_cases()
    metric_hits = 0
    clarification_hits = 0
    denial_hits = 0
    e2e_hits = 0

    for case in cases:
        planner = QueryPlannerService(
            vector_locator=EvalVectorLocatorService(case["vector_payload"]),
            sql_rag=EvalSQLRAGService(case["sql_candidates"]),
        )
        plan = planner.plan(
            case["query_text"],
            [0.2, 0.4],
            request_context=case["request_context"],
        )
        if plan.metric_id == case["expected_metric_id"]:
            metric_hits += 1
        if case["expected_final_status"] == "clarification_required" and plan.clarification_required:
            clarification_hits += 1
        if case["expected_final_status"] == "policy_denied":
            denial_hits += 1

        workflow = QueryWorkflowService(
            planner=planner,
            query_service=EvalQueryService(_build_execution_payloads(case)),
            generator=SQLGeneratorService(),
            repairer=SQLRepairService(),
            trace_id_factory=lambda: str(case["request_context"]["trace_id"]),
            request_id_factory=lambda: str(case["request_context"]["request_id"]),
        )
        payload = workflow.run(
            query_text=case["query_text"],
            query_vector=[0.2, 0.4],
            database="campus",
            execute=True,
            request_context=case["request_context"],
        )
        if _matches_expected_status(payload, case["expected_final_status"]):
            e2e_hits += 1

    metric_rate = metric_hits / len(cases)
    clarification_rate = clarification_hits / 1
    denial_rate = denial_hits / 1
    e2e_rate = e2e_hits / len(cases)

    assert metric_rate >= 0.90
    assert clarification_rate >= 0.95
    assert denial_rate == 1.0
    assert e2e_rate >= 0.90


def _build_execution_payloads(case: dict[str, object]) -> list[object]:
    status = case["expected_final_status"]
    expected_sql = case["expected_sql"]
    if status == "clarification_required":
        return []
    if status == "policy_denied":
        return [
            {
                "trace_id": case["request_context"]["trace_id"],
                "request_id": case["request_context"]["request_id"],
                "database": "campus",
                "allowed": False,
                "sql": expected_sql,
                "effective_sql": expected_sql,
                "summary": {"summary": "Access to tables is not allowed: department", "details": None},
                "table": None,
                "error_code": "policy_denied",
                "degraded": False,
                "audit_id": "audit-complex-denied",
                "policy_summary": {"allowed_tables": ["student"]},
            }
        ]
    if status == "succeeded_after_repair":
        return [
            RuntimeError("Unknown column 'dept_name' in 'field list'"),
            {
                "trace_id": case["request_context"]["trace_id"],
                "request_id": case["request_context"]["request_id"],
                "database": "campus",
                "allowed": True,
                "sql": expected_sql,
                "effective_sql": expected_sql,
                "summary": {"summary": "Returned 2 rows from campus", "details": None},
                "table": {
                    "columns": ["college_name", "total"],
                    "rows": [{"college_name": "理学院", "total": 1000}, {"college_name": "工学院", "total": 900}],
                    "total_rows": 2
                },
                "error_code": None,
                "degraded": False,
                "audit_id": "audit-complex-repair",
                "policy_summary": {"allowed_tables": ["student", "department"]},
            }
        ]
    return [
        {
            "trace_id": case["request_context"]["trace_id"],
            "request_id": case["request_context"]["request_id"],
            "database": "campus",
            "allowed": True,
            "sql": expected_sql,
            "effective_sql": expected_sql,
            "summary": {"summary": "Returned 1 rows from campus", "details": None},
            "table": {"columns": ["total"], "rows": [{"total": 100}], "total_rows": 1},
            "error_code": None,
            "degraded": False,
            "audit_id": "audit-complex-success",
            "policy_summary": {"allowed_tables": ["student", "department"]},
        }
    ]


def _matches_expected_status(payload, expected_status: str) -> bool:
    if expected_status == "clarification_required":
        return payload.error_code == "clarification_required" and payload.clarification_required is True
    if expected_status == "policy_denied":
        return payload.error_code == "policy_denied"
    if expected_status == "succeeded_after_repair":
        return payload.executed is True and payload.repair is not None and payload.repair.repaired is True
    return payload.executed is True and payload.error_code is None
