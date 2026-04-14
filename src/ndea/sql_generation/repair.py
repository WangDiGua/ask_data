from pydantic import BaseModel

from ndea.planning.models import QueryPlanPayload
from ndea.sql_generation.generator import SQLGeneratorService


class SQLRepairPayload(BaseModel):
    repaired: bool
    sql: str | None = None
    strategy: str | None = None
    trigger: str | None = None
    reason: str | None = None
    attempt_number: int


class SQLRepairService:
    def __init__(self, generator: SQLGeneratorService | None = None) -> None:
        self._generator = generator or SQLGeneratorService()

    def repair(
        self,
        plan: QueryPlanPayload,
        failed_sql: str,
        failure_reason: str,
        attempt_number: int,
    ) -> SQLRepairPayload:
        trigger = self._detect_trigger(failure_reason)
        if trigger is None:
            return SQLRepairPayload(
                repaired=False,
                sql=None,
                strategy=None,
                trigger=None,
                reason="Execution failure did not match a known repair strategy",
                attempt_number=attempt_number,
            )

        if trigger in {"permission_conflict", "safety_limit"}:
            return SQLRepairPayload(
                repaired=False,
                sql=None,
                strategy=None,
                trigger=trigger,
                reason="Execution failure is not repairable under current policy",
                attempt_number=attempt_number,
            )

        ranked_candidate_sql = self._fallback_ranked_candidate_sql(plan, failed_sql)
        if ranked_candidate_sql is not None:
            return SQLRepairPayload(
                repaired=True,
                sql=ranked_candidate_sql,
                strategy="repair_ranked_candidate",
                trigger=trigger,
                reason=None,
                attempt_number=attempt_number,
            )

        repaired_sql = self._build_repair_sql(plan, failed_sql, trigger)
        if repaired_sql is None:
            return SQLRepairPayload(
                repaired=False,
                sql=None,
                strategy=None,
                trigger=trigger,
                reason="No safe repair candidate could be generated",
                attempt_number=attempt_number,
            )

        return SQLRepairPayload(
            repaired=True,
            sql=repaired_sql,
            strategy=f"repair_{trigger}",
            trigger=trigger,
            reason=None,
            attempt_number=attempt_number,
        )

    def _build_repair_sql(
        self,
        plan: QueryPlanPayload,
        failed_sql: str,
        trigger: str,
    ) -> str | None:
        candidates = self._candidate_tables(plan, failed_sql, trigger)
        for table in candidates:
            generated = self._generator.generate_for_table(plan, table)
            if generated.generated and generated.sql and generated.sql != failed_sql:
                return generated.sql
        return None

    def _candidate_tables(
        self,
        plan: QueryPlanPayload,
        failed_sql: str,
        trigger: str,
    ) -> list[str]:
        if trigger == "unknown_table":
            preferred = [
                table for table in plan.candidate_tables if table.lower() not in failed_sql.lower()
            ]
            if preferred:
                return preferred + [
                    table for table in plan.candidate_tables if table.lower() in failed_sql.lower()
                ]
        return list(plan.candidate_tables)

    def _fallback_ranked_candidate_sql(
        self,
        plan: QueryPlanPayload,
        failed_sql: str,
    ) -> str | None:
        for candidate in plan.ranked_sql_candidates:
            if candidate.sql and candidate.sql != failed_sql:
                return candidate.sql
        return None

    def _detect_trigger(self, failure_reason: str) -> str | None:
        reason = failure_reason.lower()
        if "unknown column" in reason:
            return "unknown_column"
        if "unknown table" in reason or "no such table" in reason or "doesn't exist" in reason:
            return "unknown_table"
        if "ambiguous column" in reason:
            return "ambiguous_column"
        if "invalid use of group function" in reason or "group by" in reason or "aggregate" in reason:
            return "invalid_aggregate"
        if "enum" in reason:
            return "enum_mismatch"
        if "join" in reason and ("mismatch" in reason or "path" in reason):
            return "join_path_mismatch"
        if "not allowed" in reason or "access denied" in reason or "permission" in reason:
            return "permission_conflict"
        if "requires explain approval" in reason or "exceeding limit" in reason:
            return "safety_limit"
        return None
