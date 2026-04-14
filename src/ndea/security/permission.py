from __future__ import annotations

from collections.abc import Iterable

from sqlglot import parse_one
from sqlglot import expressions as exp

from ndea.context import (
    PolicyContext,
    ResolvedPolicyContext,
    coerce_policy_context as _coerce_policy_context,
    combine_policy_contexts as _combine_policy_contexts,
    normalize_column_policy,
    normalize_row_filters,
    normalize_tables,
)
from ndea.security.safe_executor import PermissionCheckVerdict


QueryPolicyContext = PolicyContext


class TablePermissionChecker:
    def __init__(
        self,
        allowed_tables: Iterable[str] | None = None,
        blocked_columns: dict[str, Iterable[str]] | None = None,
        masked_columns: dict[str, Iterable[str]] | None = None,
        row_filters: dict[str, str] | None = None,
        actor_id: str | None = None,
    ) -> None:
        self._allowed_tables = normalize_tables(allowed_tables)
        self._blocked_columns = normalize_column_policy(blocked_columns)
        self._masked_columns = normalize_column_policy(masked_columns)
        self._row_filters = normalize_row_filters(row_filters)
        self._actor_id = actor_id

    @classmethod
    def from_policy_context(cls, context: PolicyContext | ResolvedPolicyContext) -> "TablePermissionChecker":
        return cls(
            allowed_tables=context.allowed_tables,
            blocked_columns=context.blocked_columns,
            masked_columns=context.masked_columns,
            row_filters=context.row_filters,
            actor_id=getattr(context, "actor_id", None),
        )

    def check(self, sql: str) -> PermissionCheckVerdict:
        expression = parse_one(sql, read="mysql")
        table_nodes = [
            table for table in expression.find_all(exp.Table) if getattr(table, "name", None)
        ]
        tables = {table.name.lower() for table in table_nodes if table.name}
        alias_map = self._build_alias_map(table_nodes)

        blocked = sorted(table for table in tables if table not in self._allowed_tables)
        if self._allowed_tables and blocked:
            return PermissionCheckVerdict(
                allowed=False,
                reason=f"Access to tables is not allowed: {', '.join(blocked)}",
                actor_id=self._actor_id,
            )

        blocked_columns = self._find_blocked_columns(expression, tables, alias_map)
        if blocked_columns:
            return PermissionCheckVerdict(
                allowed=False,
                reason=f"Access to columns is not allowed: {', '.join(blocked_columns)}",
                blocked_columns=blocked_columns,
                actor_id=self._actor_id,
            )

        rewritten_sql = sql
        applied_row_filters: list[str] = []
        if self._row_filters:
            rewritten_sql, applied_row_filters = self._apply_row_filters(
                expression=expression,
                table_nodes=table_nodes,
            )

        masked_columns = sorted(
            f"{table}.{column}"
            for table in sorted(tables)
            for column in sorted(self._masked_columns.get(table, set()))
        )
        return PermissionCheckVerdict(
            allowed=True,
            reason=None,
            rewritten_sql=rewritten_sql if rewritten_sql != sql else None,
            applied_row_filters=applied_row_filters,
            masked_columns=masked_columns,
            blocked_columns=[],
            actor_id=self._actor_id,
        )

    def _build_alias_map(self, table_nodes: list[exp.Table]) -> dict[str, str]:
        alias_map: dict[str, str] = {}
        for table in table_nodes:
            name = table.name.lower()
            alias = getattr(table, "alias_or_name", None) or name
            alias_map[str(alias).lower()] = name
            alias_map[name] = name
        return alias_map

    def _find_blocked_columns(
        self,
        expression: exp.Expression,
        tables: set[str],
        alias_map: dict[str, str],
    ) -> list[str]:
        blocked: set[str] = set()
        for column in expression.find_all(exp.Column):
            column_name = (column.name or "").strip().lower()
            if not column_name:
                continue

            qualifier = (column.table or "").strip().lower()
            if qualifier:
                table_name = alias_map.get(qualifier, qualifier)
                if column_name in self._blocked_columns.get(table_name, set()):
                    blocked.add(f"{table_name}.{column_name}")
                continue

            for table_name in tables:
                if column_name in self._blocked_columns.get(table_name, set()):
                    blocked.add(f"{table_name}.{column_name}")

        return sorted(blocked)

    def _apply_row_filters(
        self,
        expression: exp.Expression,
        table_nodes: list[exp.Table],
    ) -> tuple[str, list[str]]:
        if not isinstance(expression, exp.Select):
            return expression.sql(dialect="mysql"), []

        applied: list[str] = []
        for table in table_nodes:
            table_name = table.name.lower()
            row_filter = self._row_filters.get(table_name)
            if not row_filter:
                continue

            alias = str(getattr(table, "alias_or_name", None) or table_name)
            qualified_filter = row_filter.replace("{table}", alias)
            predicate = parse_one(qualified_filter, read="mysql")
            existing_where = expression.args.get("where")
            if existing_where is None:
                expression.set("where", exp.Where(this=predicate))
            else:
                expression.set("where", exp.Where(this=exp.and_(existing_where.this, predicate)))
            applied.append(qualified_filter)

        return expression.sql(dialect="mysql"), applied


def parse_allowed_tables(raw_value: str) -> set[str]:
    return normalize_tables(raw_value.split(","))


def parse_column_policy(raw_value: str) -> dict[str, set[str]]:
    return normalize_column_policy(raw_value)


def parse_row_filters(raw_value: str) -> dict[str, str]:
    return normalize_row_filters(raw_value)


def coerce_policy_context_alias(value):
    return _coerce_policy_context(value)


def combine_policy_contexts_alias(base, override):
    return _combine_policy_contexts(base, override)


coerce_policy_context = coerce_policy_context_alias
combine_policy_contexts = combine_policy_contexts_alias
