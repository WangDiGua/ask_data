from ndea.context import PolicyContext, RequestContext
from ndea.security.policy import PolicyResolver


def test_policy_resolver_combines_default_and_request_policies_with_tightening() -> None:
    resolver = PolicyResolver(
        PolicyContext(
            allowed_tables={"student", "department"},
            masked_columns={"student": {"phone"}},
            row_filters={"student": "{table}.tenant_id = 7"},
        )
    )
    request_context = RequestContext(
        trace_id="trace-1",
        request_id="request-1",
        actor_id="user-7",
        tenant_id="tenant-7",
        roles=["analyst"],
        selected_model="gpt-5.4",
        policy=PolicyContext(
            allowed_tables={"student"},
            blocked_columns={"student": {"ssn"}},
            masked_columns={"student": {"email"}},
            row_filters={"student": "{table}.status = 'active'"},
        ),
    )

    resolved = resolver.resolve(request_context)

    assert resolved.actor_id == "user-7"
    assert resolved.tenant_id == "tenant-7"
    assert resolved.allowed_tables == {"student"}
    assert resolved.blocked_columns == {"student": {"ssn"}}
    assert resolved.masked_columns == {"student": {"phone", "email"}}
    assert resolved.row_filters == {
        "student": "({table}.tenant_id = 7) AND ({table}.status = 'active')"
    }


def test_policy_resolver_treats_legacy_policy_context_as_additional_restriction() -> None:
    resolver = PolicyResolver(
        PolicyContext(
            allowed_tables={"student", "department"},
            blocked_columns={"student": {"ssn"}},
        )
    )
    request_context = RequestContext(
        trace_id="trace-2",
        request_id="request-2",
        actor_id="user-2",
        tenant_id=None,
        roles=[],
        selected_model=None,
        policy=PolicyContext(allowed_tables={"student", "department"}),
    )

    resolved = resolver.resolve(
        request_context,
        legacy_policy_context={
            "allowed_tables": ["student"],
            "masked_columns": {"student": ["email"]},
        },
    )

    assert resolved.allowed_tables == {"student"}
    assert resolved.blocked_columns == {"student": {"ssn"}}
    assert resolved.masked_columns == {"student": {"email"}}
