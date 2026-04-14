# NDEA DB Inspector Foundation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the first live-metadata foundation for NDEA by adding a MySQL-backed DB inspector service, normalized schema models, and an MCP inspection tool contract.

**Architecture:** Keep the inspector split into three small units: connection bootstrap, metadata normalization models, and an introspection service. Expose one thin MCP tool wrapper that calls the service and returns structured schema payloads without mixing connection details into the FastMCP server module.

**Tech Stack:** Python 3.14 via `py`, FastMCP, Pydantic, mysql-connector-python, pytest

---

> Current workspace note: this directory is not a Git repository, so commit steps are intentionally omitted for now. If Git is initialized later, preserve the task boundaries below as commit boundaries.

## File Structure

| Path | Responsibility |
|------|----------------|
| `pyproject.toml` | Add MySQL connector dependency |
| `.env.example` | Add MySQL timeout-related settings |
| `src/ndea/config.py` | Add MySQL timeout settings |
| `src/ndea/metadata/models.py` | Schema/table/column payload normalization |
| `src/ndea/metadata/introspector.py` | Live metadata service for tables and table schema |
| `src/ndea/metadata/mysql_client.py` | MySQL connection kwargs and connector factory |
| `src/ndea/metadata/__init__.py` | Export metadata service types |
| `src/ndea/tools/db_inspector.py` | Thin MCP-facing wrapper |
| `src/ndea/tools/__init__.py` | Register DB inspector tool |
| `tests/test_metadata_models.py` | Enum parsing and schema normalization tests |
| `tests/test_metadata_introspector.py` | Inspector service tests with fake connection |
| `tests/test_db_inspector_tool.py` | Tool wrapper test |

### Task 1: MySQL Connector and Config Support

**Files:**
- Modify: `pyproject.toml`
- Modify: `.env.example`
- Modify: `src/ndea/config.py`
- Create: `tests/test_mysql_config.py`

- [ ] **Step 1: Write the failing config test**

```python
from ndea.config import Settings


def test_mysql_timeout_defaults_exist():
    settings = Settings()
    assert settings.mysql_connect_timeout == 5
    assert settings.mysql_read_timeout == 30
```

- [ ] **Step 2: Run the test to verify it fails**

Run:

```powershell
.\.venv\Scripts\python.exe -m pytest tests/test_mysql_config.py -v
```

Expected: FAIL because the timeout fields do not exist yet.

- [ ] **Step 3: Add dependency and settings**

Update `pyproject.toml` to add:

```toml
"mysql-connector-python==9.6.0",
```

Update `.env.example` to add:

```env
NDEA_MYSQL_CONNECT_TIMEOUT=5
NDEA_MYSQL_READ_TIMEOUT=30
```

Update `src/ndea/config.py` to add:

```python
mysql_connect_timeout: int = 5
mysql_read_timeout: int = 30
```

- [ ] **Step 4: Install dependencies and rerun the test**

Run:

```powershell
.\.venv\Scripts\python.exe -m pip install -e ".[dev]"
.\.venv\Scripts\python.exe -m pytest tests/test_mysql_config.py -v
```

Expected: PASS.

### Task 2: Metadata Models

**Files:**
- Create: `src/ndea/metadata/models.py`
- Modify: `src/ndea/metadata/__init__.py`
- Create: `tests/test_metadata_models.py`

- [ ] **Step 1: Write the failing metadata model tests**

```python
from ndea.metadata.models import ColumnSchema, parse_mysql_enum_values


def test_parse_mysql_enum_values_returns_clean_values():
    assert parse_mysql_enum_values("enum('teacher','student')") == ["teacher", "student"]


def test_column_schema_extracts_enum_values_from_column_type():
    column = ColumnSchema(
        name="role",
        data_type="enum",
        column_type="enum('teacher','student')",
        is_nullable=False,
        comment="Role",
    )
    assert column.enum_values == ["teacher", "student"]
```

- [ ] **Step 2: Run the test to verify it fails**

Run:

```powershell
.\.venv\Scripts\python.exe -m pytest tests/test_metadata_models.py -v
```

Expected: FAIL because the models do not exist yet.

- [ ] **Step 3: Write the minimal implementation**

Create:

```python
import re

from pydantic import BaseModel, Field, model_validator


def parse_mysql_enum_values(column_type: str) -> list[str]:
    matches = re.findall(r"'((?:[^'\\\\]|\\\\.)*)'", column_type)
    return [match.replace("\\\\'", "'") for match in matches]


class ColumnSchema(BaseModel):
    name: str
    data_type: str
    column_type: str
    is_nullable: bool
    comment: str = ""
    enum_values: list[str] = Field(default_factory=list)

    @model_validator(mode="after")
    def fill_enum_values(self) -> "ColumnSchema":
        if not self.enum_values and self.data_type.lower() == "enum":
            self.enum_values = parse_mysql_enum_values(self.column_type)
        return self
```

- [ ] **Step 4: Run the test to verify it passes**

Run:

```powershell
.\.venv\Scripts\python.exe -m pytest tests/test_metadata_models.py -v
```

Expected: PASS.

### Task 3: Metadata Introspector Service

**Files:**
- Create: `src/ndea/metadata/introspector.py`
- Modify: `src/ndea/metadata/mysql_client.py`
- Modify: `src/ndea/metadata/__init__.py`
- Create: `tests/test_metadata_introspector.py`

- [ ] **Step 1: Write the failing inspector tests**

```python
from ndea.metadata.introspector import MetadataIntrospector


class FakeCursor:
    def __init__(self, rows):
        self.rows = rows
        self.executed = []

    def execute(self, query, params=None):
        self.executed.append((query, params))

    def fetchall(self):
        return self.rows

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


class FakeConnection:
    def __init__(self, table_rows=None, column_rows=None):
        self.table_rows = table_rows or []
        self.column_rows = column_rows or []
        self.calls = 0

    def cursor(self, dictionary=True):
        self.calls += 1
        rows = self.table_rows if self.calls == 1 else self.column_rows
        return FakeCursor(rows)

    def close(self):
        return None


def test_list_tables_normalizes_rows():
    inspector = MetadataIntrospector(lambda: FakeConnection(table_rows=[
        {"table_name": "student", "table_comment": "Students"},
    ]))
    tables = inspector.list_tables("campus")
    assert tables[0].name == "student"
    assert tables[0].comment == "Students"


def test_describe_table_returns_columns():
    inspector = MetadataIntrospector(lambda: FakeConnection(
        column_rows=[
            {
                "column_name": "role",
                "data_type": "enum",
                "column_type": "enum('teacher','student')",
                "is_nullable": "NO",
                "column_comment": "Role",
            }
        ]
    ))
    schema = inspector.describe_table("campus", "user_role")
    assert schema.table_name == "user_role"
    assert schema.columns[0].enum_values == ["teacher", "student"]
```

- [ ] **Step 2: Run the test to verify it fails**

Run:

```powershell
.\.venv\Scripts\python.exe -m pytest tests/test_metadata_introspector.py -v
```

Expected: FAIL because the introspector does not exist yet.

- [ ] **Step 3: Write the minimal implementation**

Implement a small inspector with:

- `TableSchemaSummary`
- `TableSchemaDetail`
- `MetadataIntrospector`
- parameterized `information_schema.tables` query
- parameterized `information_schema.columns` query
- normalization into Pydantic models

- [ ] **Step 4: Run the test to verify it passes**

Run:

```powershell
.\.venv\Scripts\python.exe -m pytest tests/test_metadata_introspector.py -v
```

Expected: PASS.

### Task 4: MCP Tool Wrapper

**Files:**
- Create: `src/ndea/tools/db_inspector.py`
- Modify: `src/ndea/tools/__init__.py`
- Create: `tests/test_db_inspector_tool.py`

- [ ] **Step 1: Write the failing tool test**

```python
from ndea.tools.db_inspector import inspect_table_schema


class FakeInspector:
    def describe_table(self, database, table_name):
        return {
            "database": database,
            "table_name": table_name,
            "columns": [],
        }


def test_inspect_table_schema_uses_injected_inspector(monkeypatch):
    monkeypatch.setattr(
        "ndea.tools.db_inspector.get_metadata_introspector",
        lambda: FakeInspector(),
    )
    payload = inspect_table_schema("campus", "student")
    assert payload["database"] == "campus"
    assert payload["table_name"] == "student"
```

- [ ] **Step 2: Run the test to verify it fails**

Run:

```powershell
.\.venv\Scripts\python.exe -m pytest tests/test_db_inspector_tool.py -v
```

Expected: FAIL because the tool wrapper does not exist yet.

- [ ] **Step 3: Write the minimal implementation**

Implement:

- `get_metadata_introspector()`
- `inspect_table_schema(database: str, table_name: str)`
- tool registration in `src/ndea/tools/__init__.py`

- [ ] **Step 4: Run the test to verify it passes**

Run:

```powershell
.\.venv\Scripts\python.exe -m pytest tests/test_db_inspector_tool.py -v
```

Expected: PASS.

### Task 5: Full Verification and Ledger Update

**Files:**
- Modify: `NDEA_PROGRESS_LEDGER.md`

- [ ] **Step 1: Update the progress ledger**

Update:

- `DB inspector` -> `IN_PROGRESS` or `DONE` depending on final scope
- current session outcome
- files created
- verification command
- next recommended task after DB inspector

- [ ] **Step 2: Run the full test suite**

Run:

```powershell
.\.venv\Scripts\python.exe -m pytest -v
```

Expected: PASS with the new DB inspector tests included.
