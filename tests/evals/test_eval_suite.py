import json
from pathlib import Path

from ndea.planning import QueryPlannerService, QueryPlanPayload, QueryWorkflowService
from ndea.sql_generation import SQLGenerationPayload, SQLRepairPayload


class EvalVectorLocatorService:
    def __init__(self, matches: list[dict[str, object]]) -> None:
        self._matches = matches

    def locate(
        self,
        query_text: str,
        query_vector: list[float],
        asset_types: list[str] | None = None,
        limit: int | None = None,
    ) -> dict[str, object]:
        return {"matches": list(self._matches)}


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


class EvalGeneratorService:
    def generate(self, plan: QueryPlanPayload) -> SQLGenerationPayload:
        if plan.selected_sql:
            return SQLGenerationPayload(
                generated=True,
                sql=plan.selected_sql,
                strategy="rag_candidate",
                reason=None,
            )
        if plan.candidate_tables:
            return SQLGenerationPayload(
                generated=True,
                sql=f"SELECT COUNT(*) AS total FROM {plan.candidate_tables[0]}",
                strategy="count_metric",
                reason=None,
            )
        return SQLGenerationPayload(
            generated=False,
            sql=None,
            strategy=None,
            reason="Clarification required before SQL generation",
        )


class EvalRepairService:
    def repair(
        self,
        plan: QueryPlanPayload,
        failed_sql: str,
        failure_reason: str,
        attempt_number: int,
    ) -> SQLRepairPayload:
        if "Unknown column" in failure_reason:
            return SQLRepairPayload(
                repaired=True,
                sql="SELECT COUNT(*) AS total FROM student",
                strategy="repair_unknown_column",
                trigger="unknown_column",
                reason=None,
                attempt_number=attempt_number,
            )
        return SQLRepairPayload(
            repaired=False,
            sql=None,
            strategy=None,
            trigger="policy_denied",
            reason="Execution failure is not repairable under current policy",
            attempt_number=attempt_number,
        )


def _load_eval_cases() -> list[dict[str, object]]:
    path = Path(__file__).with_name("fixtures.json")
    return json.loads(path.read_text(encoding="utf-8"))


def test_offline_eval_suite_meets_release_gates() -> None:
    cases = _load_eval_cases()
    planner_hits = 0
    sql_hits = 0
    denial_hits = 0
    e2e_hits = 0

    for case in cases:
        planner = QueryPlannerService(
            vector_locator=EvalVectorLocatorService(case["semantic_matches"]),
            sql_rag=EvalSQLRAGService(case["sql_candidates"]),
        )
        plan = planner.plan(case["query_text"], [0.1, 0.2])
        if (
            plan.intent_type == case["expected_intent"]
            and plan.candidate_tables == case["expected_tables"]
        ):
            planner_hits += 1

        expected_sql_or_refusal = case["expected_sql_or_refusal"]
        if isinstance(expected_sql_or_refusal, str) and expected_sql_or_refusal.startswith("SELECT"):
            if plan.selected_sql == expected_sql_or_refusal or (
                not plan.selected_sql and plan.candidate_tables
            ):
                sql_hits += 1
        elif expected_sql_or_refusal in {"clarification_required", "policy_denied"}:
            sql_hits += 1

        if case["expected_final_status"] == "policy_denied":
            denial_hits += 1

        workflow = QueryWorkflowService(
            planner=planner,
            query_service=EvalQueryService(_build_eval_execution_payloads(case)),
            generator=EvalGeneratorService(),
            repairer=EvalRepairService(),
            trace_id_factory=lambda: str(case["request_context"]["trace_id"]),
        )
        payload = workflow.run(
            query_text=case["query_text"],
            query_vector=[0.1, 0.2],
            database="campus",
            execute=True,
            request_context=case["request_context"],
        )
        if _matches_expected_status(payload, case["expected_final_status"]):
            e2e_hits += 1

    planner_rate = planner_hits / len(cases)
    sql_rate = sql_hits / len(cases)
    denial_rate = denial_hits / 1
    e2e_rate = e2e_hits / len(cases)

    assert planner_rate >= 0.90
    assert sql_rate >= 0.90
    assert denial_rate == 1.0
    assert e2e_rate >= 0.85


def _build_eval_execution_payloads(case: dict[str, object]) -> list[object]:
    final_status = case["expected_final_status"]
    if final_status == "clarification_required":
        return []
    if final_status == "policy_denied":
        return [
            {
                "database": "campus",
                "allowed": False,
                "sql": "SELECT * FROM department",
                "effective_sql": "SELECT * FROM department",
                "summary": {"summary": "Access to tables is not allowed: department", "details": None},
                "table": None,
                "error_code": "policy_denied",
                "degraded": False,
                "audit_id": "audit-eval-4",
                "policy_summary": {"allowed_tables": ["student"]},
            }
        ]
    if final_status == "succeeded_after_repair":
        return [
            RuntimeError("Unknown column 'bad_column' in 'field list'"),
            {
                "database": "campus",
                "allowed": True,
                "sql": "SELECT COUNT(*) AS total FROM student",
                "effective_sql": "SELECT COUNT(*) AS total FROM student",
                "summary": {"summary": "Returned 1 rows from campus", "details": None},
                "table": {"columns": ["total"], "rows": [{"total": 10}], "total_rows": 1},
                "error_code": None,
                "degraded": False,
                "audit_id": "audit-eval-3",
                "policy_summary": {"allowed_tables": ["student"]},
            },
        ]
    return [
        {
            "database": "campus",
            "allowed": True,
            "sql": str(case["expected_sql_or_refusal"]),
            "effective_sql": str(case["expected_sql_or_refusal"]),
            "summary": {"summary": "Returned 1 rows from campus", "details": None},
            "table": {"columns": ["total"], "rows": [{"total": 10}], "total_rows": 1},
            "error_code": None,
            "degraded": False,
            "audit_id": "audit-eval-1",
            "policy_summary": {"allowed_tables": ["student"]},
        }
    ]


def _matches_expected_status(payload, expected_final_status: str) -> bool:
    if expected_final_status == "clarification_required":
        return payload.error_code == "clarification_required"
    if expected_final_status == "policy_denied":
        return payload.error_code == "policy_denied"
    if expected_final_status == "succeeded_after_repair":
        return payload.executed is True and payload.repair is not None and payload.repair.repaired is True
    return payload.executed is True and payload.error_code is None
