from __future__ import annotations

from sqlglot import parse_one
from sqlglot import expressions as exp

from ndea.query_v2 import PlanCandidate, SQLCandidate, VerificationIssue, VerificationReport
from ndea.resolution import SchemaResolverRepository
from ndea.security.permission import TablePermissionChecker


class SQLVerifier:
    def __init__(self, schema_repository: SchemaResolverRepository | None = None) -> None:
        self._schema_repository = schema_repository

    def verify(
        self,
        database: str | None,
        plan: PlanCandidate,
        candidate: SQLCandidate,
        allowed_tables: set[str] | None = None,
    ) -> VerificationReport:
        issues: list[VerificationIssue] = []
        try:
            expression = parse_one(candidate.sql, read="mysql")
        except Exception as exc:
            return VerificationReport(
                sql_candidate_id=candidate.candidate_id,
                allowed=False,
                score=0.0,
                effective_sql=candidate.sql,
                issues=[VerificationIssue(code="parse_error", severity="error", message=str(exc) or "SQL parse failed")],
            )

        referenced_tables = sorted(
            {table.name.lower() for table in expression.find_all(exp.Table) if getattr(table, "name", None)}
        )
        referenced_columns = sorted(
            {
                f"{(column.table or '').lower()}.{column.name.lower()}".strip(".")
                for column in expression.find_all(exp.Column)
                if getattr(column, "name", None)
            }
        )

        if allowed_tables:
            verdict = TablePermissionChecker(allowed_tables=allowed_tables).check(candidate.sql)
            if not verdict.allowed:
                issues.append(
                    VerificationIssue(
                        code="policy_denied",
                        severity="error",
                        message=verdict.reason or "SQL references disallowed tables or columns",
                    )
                )

        if database and self._schema_repository is not None:
            table_map = {}
            for table in referenced_tables:
                try:
                    table_map[table] = self._schema_repository.describe_table(database, table)
                except Exception:
                    issues.append(
                        VerificationIssue(
                            code="unknown_table",
                            severity="error",
                            message=f"Table `{table}` does not exist in `{database}`",
                        )
                    )
            columns_by_table = {
                table: {column.name.lower() for column in detail.columns}
                for table, detail in table_map.items()
            }
            for qualified_column in referenced_columns:
                if "." not in qualified_column:
                    continue
                table_name, column_name = qualified_column.split(".", 1)
                if table_name and table_name in columns_by_table and column_name not in columns_by_table[table_name]:
                    issues.append(
                        VerificationIssue(
                            code="unknown_column",
                            severity="error",
                            message=f"Column `{column_name}` does not exist on `{table_name}`",
                        )
                    )

        if plan.answer_mode in {"aggregate", "metric"} and "total" not in candidate.sql.lower():
            issues.append(
                VerificationIssue(
                    code="shape_mismatch",
                    severity="warning",
                    message="Aggregate candidate does not expose `total` metric alias",
                )
            )

        score = 1.0
        for issue in issues:
            score -= 0.45 if issue.severity == "error" else 0.15
        return VerificationReport(
            sql_candidate_id=candidate.candidate_id,
            allowed=not any(issue.severity == "error" for issue in issues),
            score=round(max(0.0, min(1.0, score)), 2),
            effective_sql=candidate.sql,
            issues=issues,
            referenced_tables=referenced_tables,
            referenced_columns=referenced_columns,
        )
