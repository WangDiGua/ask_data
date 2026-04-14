from __future__ import annotations

from collections.abc import Iterable
from typing import Any
from uuid import uuid4

from pydantic import BaseModel, Field


class IdentityContext(BaseModel):
    actor_id: str | None = None
    tenant_id: str | None = None
    roles: list[str] = Field(default_factory=list)
    selected_model: str | None = None


class PolicyContext(BaseModel):
    allowed_tables: set[str] = Field(default_factory=set)
    blocked_columns: dict[str, set[str]] = Field(default_factory=dict)
    masked_columns: dict[str, set[str]] = Field(default_factory=dict)
    row_filters: dict[str, str] = Field(default_factory=dict)


class RequestContext(BaseModel):
    trace_id: str
    request_id: str
    actor_id: str | None = None
    tenant_id: str | None = None
    roles: list[str] = Field(default_factory=list)
    selected_model: str | None = None
    policy: PolicyContext = Field(default_factory=PolicyContext)


class ResolvedPolicyContext(PolicyContext):
    actor_id: str | None = None
    tenant_id: str | None = None
    roles: list[str] = Field(default_factory=list)
    selected_model: str | None = None

    def summary(self) -> dict[str, Any]:
        return {
            "actor_id": self.actor_id,
            "tenant_id": self.tenant_id,
            "roles": list(self.roles),
            "allowed_tables": sorted(self.allowed_tables),
            "blocked_columns": {
                table: sorted(columns)
                for table, columns in sorted(self.blocked_columns.items())
            },
            "masked_columns": {
                table: sorted(columns)
                for table, columns in sorted(self.masked_columns.items())
            },
            "row_filters": dict(sorted(self.row_filters.items())),
        }


def coerce_request_context(
    value: RequestContext | dict[str, Any] | None,
    trace_id_factory=None,
    request_id_factory=None,
) -> RequestContext:
    if isinstance(value, RequestContext):
        return value

    payload = dict(value or {})
    legacy_policy = payload.pop("policy_context", None)
    policy_value = payload.get("policy")
    if policy_value is None and legacy_policy is not None:
        policy_value = legacy_policy

    trace_factory = trace_id_factory or (lambda: uuid4().hex)
    request_factory = request_id_factory or (lambda: uuid4().hex)
    trace_id = str(payload.get("trace_id") or trace_factory())
    request_id = str(payload.get("request_id") or request_factory())

    roles_value = payload.get("roles")
    roles = [
        str(role)
        for role in (roles_value if isinstance(roles_value, list) else [])
        if str(role).strip()
    ]
    return RequestContext(
        trace_id=trace_id,
        request_id=request_id,
        actor_id=_optional_text(payload.get("actor_id")),
        tenant_id=_optional_text(payload.get("tenant_id")),
        roles=roles,
        selected_model=_optional_text(payload.get("selected_model")),
        policy=coerce_policy_context(policy_value),
    )


def coerce_policy_context(
    value: PolicyContext | dict[str, Any] | None,
) -> PolicyContext:
    if isinstance(value, PolicyContext):
        return value
    if value is None:
        return PolicyContext()
    return PolicyContext(
        allowed_tables=normalize_tables(value.get("allowed_tables")),
        blocked_columns=normalize_column_policy(value.get("blocked_columns")),
        masked_columns=normalize_column_policy(value.get("masked_columns")),
        row_filters=normalize_row_filters(value.get("row_filters")),
    )


def combine_policy_contexts(
    base: PolicyContext | dict[str, Any] | None,
    override: PolicyContext | dict[str, Any] | None,
) -> PolicyContext:
    base_context = coerce_policy_context(base)
    override_context = coerce_policy_context(override)
    return PolicyContext(
        allowed_tables=_combine_allowed_tables(
            base_context.allowed_tables,
            override_context.allowed_tables,
        ),
        blocked_columns=_merge_column_policies(
            base_context.blocked_columns,
            override_context.blocked_columns,
        ),
        masked_columns=_merge_column_policies(
            base_context.masked_columns,
            override_context.masked_columns,
        ),
        row_filters=_merge_row_filters(
            base_context.row_filters,
            override_context.row_filters,
        ),
    )


def normalize_tables(value: Iterable[str] | object | None) -> set[str]:
    if value is None:
        return set()
    if isinstance(value, str):
        candidates = value.split(",")
    else:
        candidates = value
    return {
        str(table).strip().lower()
        for table in candidates
        if str(table).strip()
    }


def normalize_column_policy(
    value: dict[str, Iterable[str]] | object | None,
) -> dict[str, set[str]]:
    if value is None:
        return {}
    if isinstance(value, str):
        return _parse_column_policy(value)

    normalized: dict[str, set[str]] = {}
    for table, columns in value.items():
        table_name = str(table).strip().lower()
        if not table_name:
            continue
        normalized_columns = normalize_tables(columns)
        if normalized_columns:
            normalized[table_name] = normalized_columns
    return normalized


def normalize_row_filters(value: dict[str, str] | object | None) -> dict[str, str]:
    if value is None:
        return {}
    if isinstance(value, str):
        return _parse_row_filters(value)

    normalized: dict[str, str] = {}
    for table, predicate in value.items():
        table_name = str(table).strip().lower()
        predicate_text = str(predicate).strip()
        if table_name and predicate_text:
            normalized[table_name] = predicate_text
    return normalized


def _combine_allowed_tables(base: set[str], override: set[str]) -> set[str]:
    if base and override:
        return base & override
    if override:
        return set(override)
    return set(base)


def _merge_column_policies(
    base: dict[str, set[str]],
    override: dict[str, set[str]],
) -> dict[str, set[str]]:
    merged = {table: set(columns) for table, columns in base.items()}
    for table, columns in override.items():
        merged.setdefault(table, set()).update(columns)
    return merged


def _merge_row_filters(
    base: dict[str, str],
    override: dict[str, str],
) -> dict[str, str]:
    merged = dict(base)
    for table, predicate in override.items():
        if table in merged:
            merged[table] = f"({merged[table]}) AND ({predicate})"
        else:
            merged[table] = predicate
    return merged


def _parse_column_policy(raw_value: str) -> dict[str, set[str]]:
    entries: dict[str, set[str]] = {}
    for item in raw_value.split(";"):
        table, separator, columns = item.partition(":")
        if not separator:
            continue
        table_name = table.strip().lower()
        if not table_name:
            continue
        values = {
            column.strip().lower()
            for column in columns.split(",")
            if column.strip()
        }
        if values:
            entries[table_name] = values
    return entries


def _parse_row_filters(raw_value: str) -> dict[str, str]:
    entries: dict[str, str] = {}
    for item in raw_value.split(";"):
        table, separator, predicate = item.partition(":")
        if not separator:
            continue
        table_name = table.strip().lower()
        predicate_text = predicate.strip()
        if table_name and predicate_text:
            entries[table_name] = predicate_text
    return entries


def _optional_text(value: object | None) -> str | None:
    if value in (None, ""):
        return None
    return str(value)
