# NDEA Bootstrap Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the first executable Python scaffold for NDEA with FastMCP, typed config/protocol models, placeholder tool wiring, and starter integration modules for MySQL, Milvus, and SQLGlot.

**Architecture:** Use a small `src/ndea` package with focused modules: config, protocol, server, tools, vector, metadata, and security. Keep the first milestone intentionally narrow: produce an importable FastMCP service, validated settings, structured response models, and placeholder integration components that future tasks can extend safely.

**Tech Stack:** Python 3.14 via `py`, FastMCP, Pydantic, pydantic-settings, pytest, SQLGlot, pymilvus

---

> Current workspace note: this directory is not a Git repository, so commit steps are intentionally omitted for now. If Git is initialized later, preserve the task boundaries below as commit boundaries.

## File Structure

| Path | Responsibility |
|------|----------------|
| `pyproject.toml` | Project metadata, dependencies, pytest settings |
| `.gitignore` | Ignore Python cache, local envs, coverage, IDE junk |
| `.env.example` | Baseline local configuration keys |
| `src/ndea/__init__.py` | Package marker and version export |
| `src/ndea/config.py` | Typed environment-backed settings |
| `src/ndea/protocol.py` | Request/response models and chart/table payloads |
| `src/ndea/server.py` | FastMCP app factory and tool registration |
| `src/ndea/main.py` | Service entrypoint |
| `src/ndea/tools/__init__.py` | Tool registration boundary |
| `src/ndea/tools/system.py` | Starter diagnostic tool for service readiness |
| `src/ndea/vector/milvus_client.py` | Milvus config and placeholder client factory |
| `src/ndea/metadata/mysql_client.py` | MySQL config and placeholder connector |
| `src/ndea/security/sql_guard.py` | SQLGlot-backed read-only guard starter |
| `tests/test_config.py` | Config behavior tests |
| `tests/test_protocol.py` | Protocol model tests |
| `tests/test_server.py` | App factory and tool registration tests |
| `tests/test_sql_guard.py` | SQL guard behavior tests |

### Task 1: Project Scaffold

**Files:**
- Create: `pyproject.toml`
- Create: `.gitignore`
- Create: `.env.example`
- Create: `src/ndea/__init__.py`

- [ ] **Step 1: Write the failing scaffold test**

```python
from pathlib import Path


def test_package_init_exists():
    assert Path("src/ndea/__init__.py").exists()
```

Save to `tests/test_scaffold.py`.

- [ ] **Step 2: Run test to verify it fails**

Run:

```powershell
py -m pytest tests/test_scaffold.py -v
```

Expected: FAIL because the package file and pytest setup do not exist yet.

- [ ] **Step 3: Write the minimal scaffold files**

`pyproject.toml`

```toml
[build-system]
requires = ["setuptools>=80", "wheel"]
build-backend = "setuptools.build_meta"

[project]
name = "ndea"
version = "0.1.0"
description = "Nexus Data Expert Agent"
readme = "NDEA_MASTER_CONTROL.md"
requires-python = ">=3.14"
dependencies = [
  "fastmcp==3.2.4",
  "pydantic==2.13.0",
  "pydantic-settings==2.13.1",
  "pymilvus==2.6.12",
  "sqlglot==30.4.3",
]

[project.optional-dependencies]
dev = [
  "pytest==9.0.3",
]

[tool.pytest.ini_options]
pythonpath = ["src"]
testpaths = ["tests"]
```

`.gitignore`

```gitignore
__pycache__/
.pytest_cache/
.venv/
*.pyc
*.pyo
*.pyd
.idea/
.vscode/
```

`.env.example`

```env
NDEA_APP_NAME=NDEA
NDEA_ENV=development
NDEA_LOG_LEVEL=INFO
NDEA_MILVUS_URI=http://localhost:19530
NDEA_MILVUS_TOKEN=
NDEA_MILVUS_DATABASE=default
NDEA_MYSQL_HOST=127.0.0.1
NDEA_MYSQL_PORT=3306
NDEA_MYSQL_USER=root
NDEA_MYSQL_PASSWORD=
NDEA_MYSQL_DATABASE=
```

`src/ndea/__init__.py`

```python
__all__ = ["__version__"]

__version__ = "0.1.0"
```

- [ ] **Step 4: Install dependencies and rerun the test**

Run:

```powershell
py -m venv .venv
.\.venv\Scripts\python.exe -m pip install --upgrade pip
.\.venv\Scripts\python.exe -m pip install -e ".[dev]"
.\.venv\Scripts\python.exe -m pytest tests/test_scaffold.py -v
```

Expected: PASS.

### Task 2: Settings and Protocol Models

**Files:**
- Create: `src/ndea/config.py`
- Create: `src/ndea/protocol.py`
- Create: `tests/test_config.py`
- Create: `tests/test_protocol.py`

- [ ] **Step 1: Write the failing config and protocol tests**

`tests/test_config.py`

```python
from ndea.config import Settings


def test_settings_have_expected_defaults():
    settings = Settings()
    assert settings.app_name == "NDEA"
    assert settings.mysql_port == 3306
    assert settings.milvus_database == "default"
```

`tests/test_protocol.py`

```python
from ndea.protocol import ChartPayload, TablePayload, TextPayload


def test_chart_payload_uses_echarts_renderer():
    payload = ChartPayload(title="Demo", option={"xAxis": {}, "yAxis": {}, "series": []}, source=[])
    assert payload.renderer == "echarts"


def test_table_payload_keeps_columns_and_rows():
    payload = TablePayload(columns=["name"], rows=[{"name": "Alice"}])
    assert payload.columns == ["name"]
    assert payload.rows[0]["name"] == "Alice"


def test_text_payload_keeps_summary():
    payload = TextPayload(summary="done")
    assert payload.summary == "done"
```

- [ ] **Step 2: Run tests to verify they fail**

Run:

```powershell
.\.venv\Scripts\python.exe -m pytest tests/test_config.py tests/test_protocol.py -v
```

Expected: FAIL because modules do not exist yet.

- [ ] **Step 3: Write the minimal implementation**

`src/ndea/config.py`

```python
from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_prefix="NDEA_", extra="ignore")

    app_name: str = "NDEA"
    env: str = "development"
    log_level: str = "INFO"
    milvus_uri: str = "http://localhost:19530"
    milvus_token: str = ""
    milvus_database: str = "default"
    mysql_host: str = "127.0.0.1"
    mysql_port: int = 3306
    mysql_user: str = "root"
    mysql_password: str = ""
    mysql_database: str = ""
```

`src/ndea/protocol.py`

```python
from typing import Any

from pydantic import BaseModel, Field


class TextPayload(BaseModel):
    summary: str
    details: str | None = None


class TablePayload(BaseModel):
    columns: list[str]
    rows: list[dict[str, Any]]
    total_rows: int | None = None


class ChartPayload(BaseModel):
    renderer: str = "echarts"
    title: str
    option: dict[str, Any]
    source: list[dict[str, Any]]
    description: str | None = None
```

- [ ] **Step 4: Run tests to verify they pass**

Run:

```powershell
.\.venv\Scripts\python.exe -m pytest tests/test_config.py tests/test_protocol.py -v
```

Expected: PASS.

### Task 3: FastMCP App Factory and Starter Tool

**Files:**
- Create: `src/ndea/server.py`
- Create: `src/ndea/main.py`
- Create: `src/ndea/tools/__init__.py`
- Create: `src/ndea/tools/system.py`
- Create: `tests/test_server.py`

- [ ] **Step 1: Write the failing server test**

`tests/test_server.py`

```python
from ndea.server import create_mcp
from ndea.tools.system import system_status


def test_create_mcp_returns_named_server():
    server = create_mcp()
    assert server.name == "NDEA"


def test_system_status_reports_stack_choices():
    payload = system_status()
    assert payload["service"] == "NDEA"
    assert payload["vector_backend"] == "Milvus"
    assert payload["database"] == "MySQL"
```

- [ ] **Step 2: Run the test to verify it fails**

Run:

```powershell
.\.venv\Scripts\python.exe -m pytest tests/test_server.py -v
```

Expected: FAIL because the modules do not exist yet.

- [ ] **Step 3: Write the minimal implementation**

`src/ndea/tools/system.py`

```python
def system_status() -> dict[str, str]:
    return {
        "service": "NDEA",
        "vector_backend": "Milvus",
        "database": "MySQL",
        "sql_guard": "SQLGlot",
        "renderer": "Apache ECharts",
    }
```

`src/ndea/tools/__init__.py`

```python
from fastmcp import FastMCP

from ndea.tools.system import system_status


def register_tools(mcp: FastMCP) -> None:
    mcp.tool(name="system_status")(system_status)
```

`src/ndea/server.py`

```python
from fastmcp import FastMCP

from ndea.config import Settings
from ndea.tools import register_tools


def create_mcp(settings: Settings | None = None) -> FastMCP:
    resolved = settings or Settings()
    mcp = FastMCP(resolved.app_name)
    register_tools(mcp)
    return mcp
```

`src/ndea/main.py`

```python
from ndea.server import create_mcp


mcp = create_mcp()
app = mcp
```

- [ ] **Step 4: Run the server tests to verify they pass**

Run:

```powershell
.\.venv\Scripts\python.exe -m pytest tests/test_server.py -v
```

Expected: PASS.

### Task 4: Placeholder Integration Modules

**Files:**
- Create: `src/ndea/vector/milvus_client.py`
- Create: `src/ndea/metadata/mysql_client.py`
- Create: `src/ndea/security/sql_guard.py`
- Create: `tests/test_sql_guard.py`

- [ ] **Step 1: Write the failing integration tests**

`tests/test_sql_guard.py`

```python
from ndea.security.sql_guard import SQLGuard


def test_sql_guard_allows_simple_select():
    verdict = SQLGuard().validate("SELECT 1")
    assert verdict.allowed is True


def test_sql_guard_blocks_delete():
    verdict = SQLGuard().validate("DELETE FROM student")
    assert verdict.allowed is False
    assert verdict.reason == "Only read-only SELECT statements are allowed"
```

- [ ] **Step 2: Run the test to verify it fails**

Run:

```powershell
.\.venv\Scripts\python.exe -m pytest tests/test_sql_guard.py -v
```

Expected: FAIL because the module does not exist yet.

- [ ] **Step 3: Write the minimal implementation**

`src/ndea/vector/milvus_client.py`

```python
from pydantic import BaseModel

from ndea.config import Settings


class MilvusConnectionInfo(BaseModel):
    uri: str
    token: str
    database: str


def build_milvus_connection_info(settings: Settings) -> MilvusConnectionInfo:
    return MilvusConnectionInfo(
        uri=settings.milvus_uri,
        token=settings.milvus_token,
        database=settings.milvus_database,
    )
```

`src/ndea/metadata/mysql_client.py`

```python
from pydantic import BaseModel

from ndea.config import Settings


class MySQLConnectionInfo(BaseModel):
    host: str
    port: int
    user: str
    password: str
    database: str


def build_mysql_connection_info(settings: Settings) -> MySQLConnectionInfo:
    return MySQLConnectionInfo(
        host=settings.mysql_host,
        port=settings.mysql_port,
        user=settings.mysql_user,
        password=settings.mysql_password,
        database=settings.mysql_database,
    )
```

`src/ndea/security/sql_guard.py`

```python
from pydantic import BaseModel
from sqlglot import parse_one
from sqlglot.errors import ParseError
from sqlglot.expressions import Select


class SQLGuardVerdict(BaseModel):
    allowed: bool
    reason: str | None = None


class SQLGuard:
    def validate(self, sql: str) -> SQLGuardVerdict:
        try:
            expression = parse_one(sql, read="mysql")
        except ParseError:
            return SQLGuardVerdict(allowed=False, reason="SQL could not be parsed")

        if isinstance(expression, Select):
            return SQLGuardVerdict(allowed=True, reason=None)

        return SQLGuardVerdict(
            allowed=False,
            reason="Only read-only SELECT statements are allowed",
        )
```

- [ ] **Step 4: Run tests to verify they pass**

Run:

```powershell
.\.venv\Scripts\python.exe -m pytest tests/test_sql_guard.py -v
```

Expected: PASS.

### Task 5: Full Baseline Verification and Ledger Update

**Files:**
- Modify: `NDEA_PROGRESS_LEDGER.md`

- [ ] **Step 1: Update progress ledger**

Set these updates in `NDEA_PROGRESS_LEDGER.md`:

- `Project bootstrap` -> `DONE`
- `MCP server core` -> `DONE`
- `Safe executor` remains `NOT_STARTED`
- `Vector locator` remains `NOT_STARTED`
- add the newly created scaffold files to the current session record
- update the immediate next task recommendation to implement the live DB inspector first

- [ ] **Step 2: Run the full test suite**

Run:

```powershell
.\.venv\Scripts\python.exe -m pytest -v
```

Expected: PASS with all scaffold tests green.

- [ ] **Step 3: Record actual verification output in the ledger session notes**

Add a short verification note under the current session outcome with the exact command that passed.
