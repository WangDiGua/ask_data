from pydantic import BaseModel
from sqlglot import parse
from sqlglot import expressions as exp
from sqlglot.errors import ParseError


class SQLGuardVerdict(BaseModel):
    allowed: bool
    reason: str | None = None
    rejection_code: str | None = None
    statement_count: int = 0
    statement_type: str | None = None
    needs_explain: bool = False


class SQLGuard:
    def validate(self, sql: str) -> SQLGuardVerdict:
        try:
            expressions = parse(sql, read="mysql")
        except ParseError:
            return SQLGuardVerdict(
                allowed=False,
                reason="SQL could not be parsed",
                rejection_code="parse_error",
                statement_count=0,
            )

        if not expressions:
            return SQLGuardVerdict(
                allowed=False,
                reason="SQL must contain a statement",
                rejection_code="empty_statement",
                statement_count=0,
            )

        if len(expressions) != 1:
            return SQLGuardVerdict(
                allowed=False,
                reason="Only a single SQL statement is allowed",
                rejection_code="multiple_statements",
                statement_count=len(expressions),
            )

        expression = expressions[0]
        statement_type = self._statement_type(expression)

        if not self._is_read_only_query(expression):
            return SQLGuardVerdict(
                allowed=False,
                reason="Only read-only SELECT statements are allowed",
                rejection_code="unsupported_statement",
                statement_count=1,
                statement_type=statement_type,
            )

        return SQLGuardVerdict(
            allowed=True,
            reason=None,
            rejection_code=None,
            statement_count=1,
            statement_type=statement_type,
            needs_explain=self._needs_explain(expression),
        )

    def _is_read_only_query(self, expression: exp.Expression) -> bool:
        return isinstance(expression, (exp.Select, exp.Union, exp.Except, exp.Intersect))

    def _statement_type(self, expression: exp.Expression) -> str:
        key = getattr(expression, "key", None)
        if isinstance(key, str):
            return key.lower()
        return expression.__class__.__name__.lower()

    def _needs_explain(self, expression: exp.Expression) -> bool:
        has_joins = expression.find(exp.Join) is not None
        has_grouping = expression.find(exp.Group) is not None
        has_nested_query = expression.find(exp.Subquery) is not None
        has_set_operation = any(
            isinstance(expression, kind) or expression.find(kind) is not None
            for kind in (exp.Union, exp.Except, exp.Intersect)
        )
        return has_joins or has_grouping or has_nested_query or has_set_operation
