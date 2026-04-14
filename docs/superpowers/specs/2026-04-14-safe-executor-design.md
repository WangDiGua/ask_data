# Safe Executor Design

## Summary

This iteration upgrades NDEA's security layer from a minimal read-only SQL check into a guarded execution boundary.

The goal is to keep the implementation small while making it useful for the next phase of development. We will enrich the static SQL verdict, block multi-statement input, introduce an explain/cost guard abstraction for complex queries, and add a dependency-injected execution wrapper that can later host permission enforcement and real database execution.

## Current State

NDEA already has:

- a FastMCP server shell
- typed settings and response payload models
- a MySQL metadata introspector and MCP tool wrapper
- a starter `SQLGuard` that allows `SELECT` and rejects non-`SELECT`

The current `SQLGuard` is intentionally narrow. It does not expose enough detail for later planner/orchestrator stages, does not distinguish parse failures from policy failures, does not block multi-statement input explicitly, and is not yet wrapped in an execution boundary.

## Goals

- Preserve the existing small `sql_guard.py` module as the home of static SQL validation
- Expand the validation output so later layers can make decisions without reparsing SQL
- Reject multi-statement SQL before any execution hook is reached
- Mark complex read-only queries as requiring explain/cost inspection
- Introduce a `SafeExecutor` boundary that coordinates guard checks, optional explain checks, optional permission checks, and the final query runner
- Keep the first implementation testable without a live database by injecting dependencies

## Non-Goals

- No real row-level security injection in this iteration
- No portal-facing response assembly changes in this iteration
- No planner or ReAct orchestration changes in this iteration
- No direct MySQL execution implementation tied into FastMCP yet
- No attempt to support writes, DDL, or administrative SQL

## Approaches Considered

### Option A: Extend `SQLGuard` only

Keep all new logic in `sql_guard.py`, including explain and execution decisions.

Pros:

- smallest initial diff
- minimal file count

Cons:

- mixes static parsing with runtime execution policy
- makes later permission hooks harder to add cleanly
- encourages a single growing security file

### Option B: Split validation from execution boundary

Keep `SQLGuard` focused on static analysis, then add a `SafeExecutor` module that consumes the guard verdict and coordinates runtime checks.

Pros:

- clearer boundaries
- easy to test with fake explain and execution hooks
- aligns with the ledger's "guarded execution layer" goal

Cons:

- one additional module and result model set

### Option C: Only add result models now

Define interfaces and verdict models but do not build an executor flow yet.

Pros:

- very low implementation risk

Cons:

- low practical value
- delays the first usable guarded execution path

## Chosen Design

Use Option B.

This keeps static SQL analysis small and deterministic while adding the next real control point NDEA needs. It also gives future planner and orchestration work a clean execution boundary to call into.

## Proposed Module Boundaries

### `src/ndea/security/sql_guard.py`

Responsibility:

- parse SQL
- reject unsupported or unsafe statement shapes
- classify the statement
- flag whether explain/cost inspection is required

Planned behavior:

- use `sqlglot.parse` so multi-statement input can be detected from the parsed statement list
- reject parse errors with a specific rejection code
- reject zero statements
- reject more than one statement
- allow only read-only `SELECT`
- mark a query as `needs_explain=True` when it contains higher-risk read patterns such as joins, set operations, grouping, or nested queries

### `src/ndea/security/safe_executor.py`

Responsibility:

- own the runtime guarded execution flow
- call the static SQL guard
- optionally call permission and explain hooks
- call the query runner only after all checks pass

This module will not know about MySQL connector details directly. It will depend on injected callables or small interfaces.

### `src/ndea/security/__init__.py`

Responsibility:

- export the new security layer types cleanly for later imports

## Planned Data Contracts

### `SQLGuardVerdict`

Fields:

- `allowed: bool`
- `reason: str | None`
- `rejection_code: str | None`
- `statement_count: int`
- `statement_type: str | None`
- `needs_explain: bool`

Notes:

- `reason` stays human-readable for logs and operator debugging
- `rejection_code` stays stable for programmatic branching
- `statement_type` is expected to be `"select"` for allowed queries

### `ExplainCheckVerdict`

Fields:

- `allowed: bool`
- `reason: str | None`
- `estimated_cost: float | None = None`

This keeps the abstraction generic enough for a future MySQL `EXPLAIN` implementation without locking the interface too early.

### `PermissionCheckVerdict`

Fields:

- `allowed: bool`
- `reason: str | None`

This stays intentionally small in the first iteration. It only expresses allow or reject and leaves field-level and row-level detail for later policy work.

### `SafeExecutionResult`

Fields:

- `allowed: bool`
- `reason: str | None`
- `guard: SQLGuardVerdict`
- `explain: ExplainCheckVerdict | None = None`
- `rows: list[dict[str, object]] | None = None`

This first version is intentionally narrow. It is enough to express reject, pass-without-explain, pass-with-explain, and executed result cases.

## Runtime Flow

The new guarded execution flow should be:

1. `SafeExecutor.execute(sql, query_runner, explain_checker=None, permission_checker=None)`
2. Run `SQLGuard.validate(sql)`
3. If guard rejects, return `SafeExecutionResult` without calling any downstream hook
4. If a permission checker is provided and it rejects, return rejection
5. If the guard flags `needs_explain=True`:
   - if no explain checker is provided, reject with an explain-required reason
   - otherwise run the explain checker and reject if it fails
6. Call the injected `query_runner(sql)` only after all checks pass
7. Return a successful `SafeExecutionResult` containing the guard verdict and returned rows

Supporting callable contracts:

- `query_runner(sql: str) -> list[dict[str, object]]`
- `explain_checker(sql: str) -> ExplainCheckVerdict`
- `permission_checker(sql: str) -> PermissionCheckVerdict`

## Error Handling

- Parse failures return a stable rejection code such as `parse_error`
- Multiple statements return a stable rejection code such as `multiple_statements`
- Non-read-only statements return a stable rejection code such as `unsupported_statement`
- Missing explain support for a complex query returns a stable rejection code such as `explain_required`
- Permission hook rejection should preserve the hook's reason

## Testing Strategy

Implementation should be test-first and should add focused tests for:

- allowing a simple `SELECT`
- rejecting `DELETE`
- rejecting parse-invalid SQL
- rejecting multiple statements such as `SELECT 1; SELECT 2`
- flagging complex `SELECT` statements as `needs_explain=True`
- ensuring `SafeExecutor` does not call the runner when the guard rejects
- ensuring `SafeExecutor` rejects complex queries when no explain checker exists
- ensuring `SafeExecutor` calls the explain checker before the runner
- ensuring `SafeExecutor` returns rows when all checks pass

## Risks and Mitigations

### Risk: Complex-query detection becomes too clever too early

Mitigation:

Use a small, explicit rule set in this iteration and prefer false positives over false negatives.

### Risk: Execution API gets overdesigned

Mitigation:

Keep the first executor interface to a single `execute` method with injected callables.

### Risk: Future permission design needs richer context

Mitigation:

Keep permission checking as an optional injected boundary so later iterations can widen the interface without rewriting the static guard.

## Success Criteria

This iteration is successful when:

- `SQLGuard` can distinguish parse failure, multi-statement input, and non-`SELECT` input
- complex read-only SQL is explicitly marked for explain checking
- `SafeExecutor` exists and enforces the correct call order
- the new behavior is covered by automated tests
- the ledger can be updated from "starter read-only guard" toward a real guarded execution foundation
