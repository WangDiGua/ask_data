# NDEA

NDEA（`Nexus Data Expert Agent`）当前已经收口为一个**仅提供 MCP 服务**的校园问数系统。

当前生产形态：
- 对外唯一入口：`MCP`
- 主问数工具：`mcp_query_v2`
- 主工作流：`QueryGraphV2`
- 学习库：`ndea_learning`
- 向量库：`Milvus`

已经移除：
- 独立 HTTP 问数接口
- Portal 查询服务
- Portal 专用 MCP 包装层
- 旧 planner / workflow / sql_generation / sql_rag / vector_locator 链路

## 技术栈

- `FastMCP`
- `LangGraph`
- `MySQL`
- `Milvus`
- `SQLGlot`
- 可选 `LlamaIndex`
- 可选 `Langfuse`

## 运行环境

- Python `3.11` 到 `3.13`
- 不建议使用 Python `3.14`

## 安装

```powershell
py -3.13 -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -U pip
python -m pip install -e .[dev]
```

如果需要可选能力：

```powershell
python -m pip install -e .[dev,nl2sql,observability]
```

## 环境变量

示例见 [.env.example](.env.example)。

关键配置：
- `NDEA_MYSQL_HOST`
- `NDEA_MYSQL_PORT`
- `NDEA_MYSQL_USER`
- `NDEA_MYSQL_PASSWORD`
- `NDEA_MYSQL_DATABASE`
- `NDEA_LEARNING_MYSQL_DATABASE`
- `NDEA_MILVUS_URI`
- `NDEA_MILVUS_COLLECTION`
- `NDEA_MILVUS_COLLECTION_SQL_CASES`
- `NDEA_MILVUS_COLLECTION_QUERY_MEMORY`
- `NDEA_EMBEDDING_BASE_URL`
- `NDEA_EMBEDDING_MODEL`
- `NDEA_NL2SQL_ENGINE`
- `NDEA_LLAMAINDEX_ENGINE_FACTORY`
- `NDEA_LANGFUSE_PUBLIC_KEY`
- `NDEA_LANGFUSE_SECRET_KEY`
- `NDEA_LANGFUSE_HOST`
- `NDEA_ENABLE_QUERY_EXECUTION`
- `NDEA_ENABLE_SEMANTIC_RETRIEVAL`

## 启动方式

以前台实时日志方式启动 MCP 服务：

```powershell
.\start.cmd
```

默认 MCP 地址：

```text
http://127.0.0.1:8001/mcp/
```

热重载：

```powershell
.\start.ps1 -Reload
```

自定义监听地址和端口：

```powershell
.\start.ps1 -ListenHost 0.0.0.0 -Port 8001
```

等价原生命令：

```powershell
.\.venv\Scripts\fastmcp.exe run src\ndea\main.py:app --transport http --host 127.0.0.1 --port 8001
```

如果需要 `stdio` 传输方式：

```powershell
.\.venv\Scripts\fastmcp.exe run src\ndea\main.py:app
```

## MCP 工具

默认注册的工具：
- `mcp_query_v2`
- `execute_guarded_query`
- `inspect_table_schema`
- `system_status`

## 当前问数流程

主工作流节点如下：

1. `interaction`
2. `intent_parse`
3. `semantic_resolve`
4. `schema_resolve`
5. `build_plan_candidates`
6. `generate_sql_candidates`
7. `verify_candidates`
8. `rank_candidates`
9. `confidence_gate`
10. `execute`
11. `respond`
12. `learn`

含义可以概括为：
- 先做上下文改写
- 再做意图和校园语义解析
- 然后做 schema 兜底
- 生成多条 SQL 候选
- 校验、排序、执行
- 最后写入学习库

## 学习闭环

学习数据不会写回业务库，而是写入独立的 `ndea_learning`。

主要表：
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

## 目录说明

当前保留的主目录：
- `src/ndea/services`
- `src/ndea/orchestration`
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
- `src/ndea/vector`
- `src/ndea/security`
- `src/ndea/metadata`

已经清理掉的旁路模块：
- `src/ndea/http`
- `src/ndea/portal`
- 旧问数规划与旧 SQL 生成链路

## 测试

运行全量测试：

```powershell
.\.venv\Scripts\python.exe -m pytest -q
```

当前结果：
- `70 passed`
- `1 skipped`

测试重点：
- MCP 工具
- v2 组件
- v2 集成链路
- Milvus / MySQL 基础能力
- 运行时与健康检查

## 当前状态

当前仓库已经是：
- MCP-only
- v2-only
- Milvus-only

也就是说，现在已经不再保留旧 HTTP/Portal/legacy planner 的生产旁路。项目的唯一主路径就是 `mcp_query_v2 -> QueryServiceV2 -> QueryGraphV2`。
