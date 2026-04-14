from ndea.context import PolicyContext, RequestContext, coerce_request_context


def test_coerce_request_context_promotes_legacy_policy_context() -> None:
    payload = {
        "trace_id": "trace-1",
        "request_id": "request-1",
        "actor_id": "user-1",
        "tenant_id": "tenant-a",
        "roles": ["analyst"],
        "selected_model": "gpt-5.4",
        "policy_context": {
            "allowed_tables": ["student"],
            "masked_columns": {"student": ["ssn"]},
        },
    }

    context = coerce_request_context(payload)

    assert context.trace_id == "trace-1"
    assert context.request_id == "request-1"
    assert context.actor_id == "user-1"
    assert context.tenant_id == "tenant-a"
    assert context.roles == ["analyst"]
    assert context.selected_model == "gpt-5.4"
    assert context.policy.allowed_tables == {"student"}
    assert context.policy.masked_columns == {"student": {"ssn"}}


def test_coerce_request_context_generates_ids_when_missing() -> None:
    context = coerce_request_context(
        {"actor_id": "user-2"},
        trace_id_factory=lambda: "trace-generated",
        request_id_factory=lambda: "request-generated",
    )

    assert context.trace_id == "trace-generated"
    assert context.request_id == "request-generated"
    assert context.actor_id == "user-2"
    assert context.tenant_id is None
    assert context.roles == []
    assert context.selected_model is None
    assert context.policy == PolicyContext()


def test_request_context_model_keeps_policy_shape() -> None:
    context = RequestContext(
        trace_id="trace-3",
        request_id="request-3",
        actor_id="user-3",
        tenant_id="tenant-z",
        roles=["admin"],
        selected_model="gpt-5.4",
        policy=PolicyContext(
            allowed_tables={"student"},
            blocked_columns={"student": {"ssn"}},
            masked_columns={"student": {"email"}},
            row_filters={"student": "{table}.tenant_id = 3"},
        ),
    )

    dumped = context.model_dump()

    assert dumped["trace_id"] == "trace-3"
    assert dumped["request_id"] == "request-3"
    assert dumped["policy"]["allowed_tables"] == {"student"}
    assert dumped["policy"]["blocked_columns"] == {"student": {"ssn"}}
