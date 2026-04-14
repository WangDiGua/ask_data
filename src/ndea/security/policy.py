from __future__ import annotations

from ndea.context import (
    PolicyContext,
    RequestContext,
    ResolvedPolicyContext,
    coerce_policy_context,
    coerce_request_context,
    combine_policy_contexts,
)


class PolicyResolver:
    def __init__(self, default_policy: PolicyContext | dict | None = None) -> None:
        self._default_policy = coerce_policy_context(default_policy)

    def resolve(
        self,
        request_context: RequestContext | dict | None,
        legacy_policy_context: PolicyContext | dict | None = None,
    ) -> ResolvedPolicyContext:
        context = coerce_request_context(request_context)
        combined = combine_policy_contexts(self._default_policy, context.policy)
        combined = combine_policy_contexts(combined, legacy_policy_context)
        return ResolvedPolicyContext(
            actor_id=context.actor_id,
            tenant_id=context.tenant_id,
            roles=list(context.roles),
            selected_model=context.selected_model,
            allowed_tables=set(combined.allowed_tables),
            blocked_columns={table: set(columns) for table, columns in combined.blocked_columns.items()},
            masked_columns={table: set(columns) for table, columns in combined.masked_columns.items()},
            row_filters=dict(combined.row_filters),
        )
