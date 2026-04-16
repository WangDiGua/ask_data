from __future__ import annotations

from ndea.query_v2 import PlanCandidate, RankingDecision, SQLCandidate, VerificationReport


class CandidateRanker:
    def rank(
        self,
        plans: list[PlanCandidate],
        sql_candidates: list[SQLCandidate],
        verification_reports: list[VerificationReport],
    ) -> RankingDecision:
        plan_map = {plan.candidate_id: plan for plan in plans}
        verification_map = {report.sql_candidate_id: report for report in verification_reports}
        scoreboard: list[dict[str, object]] = []
        best_row: dict[str, object] | None = None

        for candidate in sql_candidates:
            plan = plan_map.get(candidate.plan_candidate_id)
            report = verification_map.get(candidate.candidate_id)
            score = round(
                (candidate.score * 0.35)
                + ((plan.confidence if plan is not None else 0.0) * 0.35)
                + ((report.score if report is not None else 0.0) * 0.30),
                2,
            )
            row = {
                "sql_candidate_id": candidate.candidate_id,
                "plan_candidate_id": candidate.plan_candidate_id,
                "source": candidate.source,
                "score": score,
                "allowed": bool(report.allowed) if report is not None else False,
                "issues": [issue.message for issue in report.issues] if report is not None else [],
            }
            scoreboard.append(row)
            if row["allowed"] and (best_row is None or float(row["score"]) > float(best_row["score"])):
                best_row = row

        if best_row is None:
            return RankingDecision(
                selected_plan_candidate_id=None,
                selected_sql_candidate_id=None,
                confidence=0.0,
                reason="No verified SQL candidate passed ranking",
                scoreboard=scoreboard,
            )

        return RankingDecision(
            selected_plan_candidate_id=str(best_row["plan_candidate_id"]),
            selected_sql_candidate_id=str(best_row["sql_candidate_id"]),
            confidence=float(best_row["score"]),
            reason=f"Selected {best_row['source']} candidate with highest verified score",
            scoreboard=scoreboard,
        )
