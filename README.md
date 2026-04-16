# NDEA

NDEA (`Nexus Data Expert Agent`) 是一套面向校园问数场景的服务端系统。当前版本已经完成 v2 主链路重构，核心方向是：

- 校园语义解析优先
- 知识库不足时由 schema 解析兜底
- 多候选 SQL 生成、验证和重排
- 学习闭环落库到 `ndea_learning`
- 同时提供 `HTTP`、`MCP`、`Portal` 三种入口

## 技术栈

- 编排：`LangGraph`
- 协议：`FastMCP`
- HTTP：`FastAPI + SSE`
- 向量检索：`Milvus`
- 数据库：`MySQL`
- SQL 校验：`SQLGlot`
- 配置：`Pydantic Settings`
- 可选 NL2SQL：`LlamaIndex`
- 可选观测：`Langfuse`

## 运行时要求

- Python：`3.11` 到 `3.13`
- 不建议使用 `Python 3.14`
  - 当前上游 `langchain_core`、`pymilvus` 仍可能出现兼容性告警
  - 代码里已经做了运行时降噪，但生产环境仍建议回到 `3.11-3.13`

## 安装

Windows PowerShell:

```powershell
py -3.13 -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -U pip
python -m pip install -e .[dev]
```

如果你要启用可选能力：

```powershell
python -m pip install -e .[dev,nl2sql,observability]
```

## 环境变量

示例见 [.env.example](.env.example)。

关键配置：

- `NDEA_MYSQL_HOST` / `NDEA_MYSQL_PORT` / `NDEA_MYSQL_USER` / `NDEA_MYSQL_PASSWORD`
- `NDEA_MYSQL_DATABASE`
  - 业务查询库，例如 `wenshu_db`
- `NDEA_LEARNING_MYSQL_DATABASE`
  - 学习闭环库，默认 `ndea_learning`
- `NDEA_MILVUS_URI`
- `NDEA_MILVUS_COLLECTION`
  - 主语义资产集合，默认 `semantic_assets`
- `NDEA_MILVUS_COLLECTION_SQL_CASES`
- `NDEA_MILVUS_COLLECTION_QUERY_MEMORY`
- `NDEA_EMBEDDING_BASE_URL`
- `NDEA_EMBEDDING_MODEL`
- `NDEA_NL2SQL_ENGINE`
  - 默认 `llamaindex`
- `NDEA_LLAMAINDEX_ENGINE_FACTORY`
  - 形如 `package.module:factory`
- `NDEA_LANGFUSE_PUBLIC_KEY`
- `NDEA_LANGFUSE_SECRET_KEY`
- `NDEA_LANGFUSE_HOST`
- `NDEA_ENABLE_LEGACY_TOOLS`
  - 默认 `false`

## 一键启动

项目根目录提供：

- `start.ps1`
- `start.cmd`

最常用方式：

```powershell
.\start.cmd
```

特点：

- 以前台方式启动
- 日志持续打印在当前终端
- 默认地址：`http://127.0.0.1:8001`
- 停止方式：`Ctrl+C`

热重载：

```powershell
.\start.ps1 -Reload
```

自定义监听地址或端口：

```powershell
.\start.ps1 -ListenHost 0.0.0.0 -Port 8001
```

## 启动模式

### HTTP / FastAPI

```powershell
.\.venv\Scripts\uvicorn.exe ndea.main:http_app --host 0.0.0.0 --port 8000
```

主要接口：

- `GET /health/liveness`
- `GET /health/readiness`
- `POST /api/v2/query`
- `POST /api/v2/query/stream`

### MCP / stdio

```powershell
.\.venv\Scripts\fastmcp.exe run src\ndea\main.py:app
```

### MCP / HTTP

```powershell
.\.venv\Scripts\fastmcp.exe run src\ndea\main.py:app -t http --host 127.0.0.1 -p 8000
```

## v2 API

### 同步查询

```json
POST /api/v2/query
{
  "query_text": "按学院统计 2024 学年在校学生人数",
  "database": "wenshu_db",
  "request_context": {
    "trace_id": "trace-1",
    "recent_user_messages": ["刚才那个口径"]
  },
  "policy_context": {},
  "options": {
    "debug": true,
    "max_rows": 100
  }
}
```

说明：

- v2 不再接收 `query_vector`
- embedding 统一由服务端生成

### 流式查询

```text
POST /api/v2/query/stream
```

SSE 事件按节点输出，常见事件名：

- `interaction`
- `intent_parse`
- `semantic_resolve`
- `schema_resolve`
- `build_plan_candidates`
- `generate_sql_candidates`
- `verify_candidates`
- `rank_candidates`
- `confidence_gate`
- `execute`
- `respond`
- `learn`
- `final`

## MCP 工具

默认注册：

- `ask_data_query`
- `mcp_query_v2`
- `execute_guarded_query`
- `inspect_table_schema`
- `system_status`

legacy 工具默认不注册；只有 `NDEA_ENABLE_LEGACY_TOOLS=true` 时才会暴露，并且响应里会显式标注 `legacy=true`。

## 目录说明

当前主要目录：

- `src/ndea/http`
- `src/ndea/orchestration`
- `src/ndea/services`
- `src/ndea/interaction`
- `src/ndea/understanding`
- `src/ndea/semantic`
- `src/ndea/resolution`
- `src/ndea/generation`
- `src/ndea/verification`
- `src/ndea/ranking`
- `src/ndea/execution`
- `src/ndea/learning`
- `src/ndea/query_v2`
- `src/ndea/tools`
- `tests`

## 学习闭环

学习数据不会写回业务库，而是落到独立学习库 `ndea_learning`。

当前已使用的核心表包括：

- `query_session`
- `interaction_turn`
- `ir_snapshot`
- `plan_candidate`
- `sql_candidate`
- `execution_result`
- `feedback_event`
- `promotion_queue`
- `alias_memory`
- `value_synonym_memory`
- `clarification_memory`
- `sql_case_memory`

Milvus 负责可检索资产，不负责学习事实主存储。

## 测试

运行主要测试：

```powershell
.\.venv\Scripts\python.exe -m pytest -q
```

当前项目内已覆盖：

- v2 组件测试
- v2 集成测试
- HTTP 接口测试
- Portal 查询服务测试
- MCP 工具测试
- 运行时与 registry 回归测试

## 当前状态

当前代码已经以 v2 链路为主：

- 主 API：`/api/v2/query`
- 主工作流：`QueryGraphV2`
- 主学习存储：`ndea_learning`
- 主向量库：`Milvus`

仍然保留 legacy 模块文件用于迁移期对照，但不再作为默认生产主路径。
