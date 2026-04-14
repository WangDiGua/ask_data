# Safe Executor Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build NDEA's first guarded execution foundation by enriching SQL validation and adding a dependency-injected safe execution wrapper.

**Architecture:** Keep static SQL parsing and risk classification inside `src/ndea/security/sql_guard.py`, then add a new `src/ndea/security/safe_executor.py` module that coordinates guard, optional permission checks, optional explain checks, and the final query runner. The implementation stays testable without a live database by using injected callables and Pydantic result models.

**Tech Stack:** Python 3.14 via `.\.venv\Scripts\python.exe`, SQLGlot, Pydantic, pytest

---

> Current workspace note: this directory is not a Git repository, so commit steps are intentionally omitted for now. If Git is initialized later, preserve the task boundaries below as commit boundaries.

## File Structure

| Path | Responsibility |
|------|----------------|
| `src/ndea/security/sql_guard.py` | Static SQL parsing, multi-statement blocking, statement classification, explain-needed flag |
| `src/ndea/security/safe_executor.py` | Runtime guarded execution flow and execution result models |
| `src/ndea/security/__init__.py` | Export the expanded security layer interfaces |
| `tests/test_sql_guard.py` | Guard behavior coverage for parse errors, multiple statements, and complex reads |
| `tests/test_safe_executor.py` | Safe executor flow coverage for guard rejection, permission rejection, explain ordering, and success |
| `NDEA_PROGRESS_LEDGER.md` | Project status update after implementation and verification |

### Task 1: Enrich SQL Guard Verdicts and Detection

**Files:**
- Modify: `tests/test_sql_guard.py`
- Modify: `src/ndea/security/sql_guard.py`

- [ ] **Step 1: Write the failing SQL guard tests**

Replace `tests/test_sql_guard.py` with:

```python
from ndea.security.sql_guard import SQLGuard


def test_sql_guard_allows_simple_select() -> None:
    verdict = SQLGuard().validate("SELECT 1")
    assert verdict.allowed is True
    assert verdict.reason is None
    assert verdict.rejection_code is None
    assert verdict.statement_count == 1
    assert verdict.statement_type == "select"
    assert verdict.needs_explain is False


def test_sql_guard_blocks_delete() -> None:
    verdict = SQLGuard().validate("DELETE FROM student")
    assert verdict.allowed is False
    assert verdict.reason == "Only read-only SELECT statements are allowed"
    assert verdict.rejection_code == "unsupported_statement"
    assert verdict.statement_count == 1
    assert verdict.statement_type == "delete"
    assert verdict.needs_explain is False


def test_sql_guard_blocks_invalid_sql() -> None:
    verdict = SQLGuard().validate("SELEC 1")
    assert verdict.allowed is False
    assert verdict.reason == "SQL could not be parsed"
    assert verdict.rejection_code == "parse_error"
    assert verdict.statement_count == 0
    assert verdict.statement_type is None


def test_sql_guard_blocks_multiple_statements() -> None:
    verdict = SQLGuard().validate("SELECT 1; SELECT 2")
    assert verdict.allowed is False
    assert verdict.reason == "Only a single SQL statement is allowed"
    assert verdict.rejection_code == "multiple_statements"
    assert verdict.statement_count == 2
    assert verdict.statement_type is None


def test_sql_guard_marks_grouped_query_for_explain() -> None:
    verdict = SQLGuard().validate(
        "SELECT department, COUNT(*) AS total FROM student GROUP BY department"
    )
    assert verdict.allowed is True
    assert verdict.reason is None
    assert verdict.statement_count == 1
    assert verdict.statement_type == "select"
    assert verdict.needs_explain is True
```

- [ ] **Step 2: Run the SQL guard tests to verify they fail**

Run:

```powershell
.\.venv\Scripts\python.exe -m pytest tests/test_sql_guard.py -v
```

Expected: FAIL because `SQLGuardVerdict` does not expose the new fields, multiple statements are not blocked explicitly, and complex reads are not marked for explain checks.

- [ ] **Step 3: Write the minimal SQL guard implementation**

Replace `src/ndea/security/sql_guard.py` with:

```python
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
```

- [ ] **Step 4: Run the SQL guard tests to verify they pass**

Run:

```powershell
.\.venv\Scripts\python.exe -m pytest tests/test_sql_guard.py -v
```

Expected: PASS with `5 passed`.

### Task 2: Add the Safe Executor Boundary

**Files:**
- Create: `tests/test_safe_executor.py`
- Create: `src/ndea/security/safe_executor.py`
- Modify: `src/ndea/security/__init__.py`

- [ ] **Step 1: Write the failing safe executor tests**

Create `tests/test_safe_executor.py` with:

```python
from ndea.security.safe_executor import (
    ExplainCheckVerdict,
    PermissionCheckVerdict,
    SafeExecutor,
)


def test_safe_executor_blocks_runner_when_guard_rejects() -> None:
    calls: list[str] = []

    def runner(sql: str) -> list[dict[str, object]]:
        calls.append(sql)
        return []

    result = SafeExecutor().execute("DELETE FROM student", runner)

    assert result.allowed is False
    assert result.reason == "Only read-only SELECT statements are allowed"
    assert calls == []


def test_safe_executor_rejects_complex_query_without_explain_checker() -> None:
    calls: list[str] = []

    def runner(sql: str) -> list[dict[str, object]]:
        calls.append(sql)
        return []

    sql = "SELECT department, COUNT(*) AS total FROM student GROUP BY department"
    result = SafeExecutor().execute(sql, runner)

    assert result.allowed is False
    assert result.reason == "Complex queries require explain approval"
    assert result.guard.needs_explain is True
    assert calls == []


def test_safe_executor_blocks_runner_on_permission_rejection() -> None:
    calls: list[str] = []

    def runner(sql: str) -> list[dict[str, object]]:
        calls.append("runner")
        return []

    def permission_checker(sql: str) -> PermissionCheckVerdict:
        calls.append("permission")
        return PermissionCheckVerdict(
            allowed=False,
            reason="Query is outside policy scope",
        )

    result = SafeExecutor().execute(
        "SELECT 1",
        runner,
        permission_checker=permission_checker,
    )

    assert result.allowed is False
    assert result.reason == "Query is outside policy scope"
    assert calls == ["permission"]


def test_safe_executor_runs_explain_before_query_and_returns_rows() -> None:
    calls: list[str] = []

    def runner(sql: str) -> list[dict[str, object]]:
        calls.append("runner")
        return [{"department": "math", "total": 3}]

    def explain_checker(sql: str) -> ExplainCheckVerdict:
        calls.append("explain")
        return ExplainCheckVerdict(
            allowed=True,
            reason=None,
            estimated_cost=12.5,
        )

    sql = "SELECT department, COUNT(*) AS total FROM student GROUP BY department"
    result = SafeExecutor().execute(
        sql,
        runner,
        explain_checker=explain_checker,
    )

    assert result.allowed is True
    assert result.reason is None
    assert calls == ["explain", "runner"]
    assert result.explain is not None
    assert result.explain.estimated_cost == 12.5
    assert result.rows == [{"department": "math", "total": 3}]
```

- [ ] **Step 2: Run the safe executor tests to verify they fail**

Run:

```powershell
.\.venv\Scripts\python.exe -m pytest tests/test_safe_executor.py -v
```

Expected: FAIL because `safe_executor.py` does not exist yet and `src/ndea/security/__init__.py` does not export the new types.

- [ ] **Step 3: Write the minimal safe executor implementation**

Create `src/ndea/security/safe_executor.py` with:

```python
from collections.abc import Callable

from pydantic import BaseModel

from ndea.security.sql_guard import SQLGuard, SQLGuardVerdict


class ExplainCheckVerdict(BaseModel):
    allowed: bool
    reason: str | None = None
    estimated_cost: float | None = None


class PermissionCheckVerdict(BaseModel):
    allowed: bool
    reason: str | None = None


class SafeExecutionResult(BaseModel):
    allowed: bool
    reason: str | None = None
    guard: SQLGuardVerdict
    explain: ExplainCheckVerdict | None = None
    rows: list[dict[str, object]] | None = None


class SafeExecutor:
    def __init__(self, guard: SQLGuard | None = None) -> None:
        self._guard = guard or SQLGuard()

    def execute(
        self,
        sql: str,
        query_runner: Callable[[str], list[dict[str, object]]],
        explain_checker: Callable[[str], ExplainCheckVerdict] | None = None,
        permission_checker: Callable[[str], PermissionCheckVerdict] | None = None,
    ) -> SafeExecutionResult:
        guard_verdict = self._guard.validate(sql)
        if not guard_verdict.allowed:
            return SafeExecutionResult(
                allowed=False,
                reason=guard_verdict.reason,
                guard=guard_verdict,
            )

        if permission_checker is not None:
            permission_verdict = permission_checker(sql)
            if not permission_verdict.allowed:
                return SafeExecutionResult(
                    allowed=False,
                    reason=permission_verdict.reason,
                    guard=guard_verdict,
                )

        explain_verdict: ExplainCheckVerdict | None = None
        if guard_verdict.needs_explain:
            if explain_checker is None:
                return SafeExecutionResult(
                    allowed=False,
                    reason="Complex queries require explain approval",
                    guard=guard_verdict,
                )

            explain_verdict = explain_checker(sql)
            if not explain_verdict.allowed:
                return SafeExecutionResult(
                    allowed=False,
                    reason=explain_verdict.reason,
                    guard=guard_verdict,
                    explain=explain_verdict,
                )

        rows = query_runner(sql)
        return SafeExecutionResult(
            allowed=True,
            reason=None,
            guard=guard_verdict,
            explain=explain_verdict,
            rows=rows,
        )
```

Replace `src/ndea/security/__init__.py` with:

```python
from ndea.security.safe_executor import (
    ExplainCheckVerdict,
    PermissionCheckVerdict,
    SafeExecutionResult,
    SafeExecutor,
)
from ndea.security.sql_guard import SQLGuard, SQLGuardVerdict

__all__ = [
    "ExplainCheckVerdict",
    "PermissionCheckVerdict",
    "SafeExecutionResult",
    "SafeExecutor",
    "SQLGuard",
    "SQLGuardVerdict",
]
```

- [ ] **Step 4: Run the safe executor tests to verify they pass**

Run:

```powershell
.\.venv\Scripts\python.exe -m pytest tests/test_safe_executor.py -v
```

Expected: PASS with `4 passed`.

### Task 3: Sync the Progress Ledger and Re-verify the Repository

**Files:**
- Modify: `NDEA_PROGRESS_LEDGER.md`

- [ ] **Step 1: Update the current overall status and module tracker**

In `NDEA_PROGRESS_LEDGER.md`, replace the current status block with:

```markdown
## 4. Current Overall Status

- Overall project state: `IN_PROGRESS`
- Current phase: guarded execution foundation implemented
- Current focus: connect the safe executor to live MySQL explain and permission policy
- Current blocker level: low
```

Update the `Safe executor` row in the module tracker table to:

```markdown
| Safe executor | SQL validation, permission enforcement, guarded execution | IN_PROGRESS | SQLGlot-backed guard verdicts, multi-statement blocking, complexity flagging, dependency-injected safe executor wrapper | Real explain/cost integration, live execution adapter, policy enforcement implementation | Security-critical; guarded foundation now exists |
```

- [ ] **Step 2: Update the session outcome and next-task sections**

Append these bullets under `### 7.1 Completed This Session`:

```markdown
- Expanded the SQLGlot guard into a richer verdict model with rejection codes and complexity detection
- Added a dependency-injected safe executor with explain and permission hooks
- Added automated tests for guarded execution flow control
```

Append these bullets under `### 7.2 Files Created This Session`:

```markdown
- `docs/superpowers/specs/2026-04-14-safe-executor-design.md`
- `docs/superpowers/plans/2026-04-14-safe-executor.md`
- `src/ndea/security/safe_executor.py`
- `tests/test_safe_executor.py`
```

Append these bullets under `### 7.3 Files Modified This Session`:

```markdown
- `NDEA_PROGRESS_LEDGER.md`
- `src/ndea/security/__init__.py`
- `src/ndea/security/sql_guard.py`
- `tests/test_sql_guard.py`
```

Replace `## 9. Immediate Next Task Recommendation` with:

```markdown
## 9. Immediate Next Task Recommendation

If a future AI session wants the most useful next step, start here:

### Recommended next task

Connect `SafeExecutor` to a live MySQL execution adapter, including:

- a MySQL-backed query runner
- a real `EXPLAIN` checker for complex queries
- the first permission policy integration point
- tests that verify the adapter boundary with fake connectors

### Why this next

Because NDEA now has a guarded execution boundary, and the next leverage point is connecting that boundary to real read-only execution behavior.
```

- [ ] **Step 3: Run the full test suite to verify the repository stays green**

Run:

```powershell
.\.venv\Scripts\python.exe -m pytest -q
```

Expected: PASS with `22 passed`.

- [ ] **Step 4: Record the verification result in the ledger**

Set the `### 7.4 Verification` section to:

```markdown
### 7.4 Verification

- `.\.venv\Scripts\python.exe -m pytest -q`
- Result: `22 passed`
```
