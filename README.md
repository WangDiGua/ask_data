# NDEA 问数 Agent

NDEA（`Nexus Data Expert Agent`）是一个面向高校复杂问数场景的智能数据问答服务。它运行在 Nexus 门户体系之上，对外同时提供 `MCP` 和 `HTTP/SSE` 两套入口，负责把自然语言问题转成安全、可控、可追踪的结构化查询流程。

当前项目的目标不是做一个泛化聊天机器人，而是做一个面向校园业务语义的“高准确率问数引擎”：

- 理解高校业务口径、别名和术语
- 结合语义资产与黄金 SQL 做检索增强
- 通过 LangGraph 编排完成规划、SQL 建议、执行、修复和响应组装
- 通过 SQLGuard、权限层、Explain 检查和审计日志保证生产安全

## 1. 技术栈

- 编排层：`LangGraph`
- 协议层：`FastMCP`
- 服务层：`FastAPI + SSE`
- 向量检索：`Qdrant`
- 数据连接：`SQLAlchemy + mysql-connector-python`
- SQL 安全：`sqlglot`
- 配置与模型：`Pydantic / pydantic-settings`

## 2. 当前能力

项目已经具备以下主链能力：

- 语义检索：检索指标合同、维度合同、Join 路径、时间语义、黄金 SQL
- 复杂问数规划：识别指标、维度、过滤、时间范围、Join 计划、澄清问题
- SQL 建议：基于 exemplar / golden SQL 的 Vanna-style SQL advisor
- 安全执行：只读校验、权限控制、Explain 成本检查、结果行数限制
- 修复回路：单次 bounded retry，避免简单错误直接失败
- 响应输出：文本、表格、图表建议
- 可观测性：健康检查、结构化审计事件、依赖状态回传

## 3. 目录结构

```text
ask_data/
├─ src/ndea/
│  ├─ context/           # 请求上下文、身份与策略上下文
│  ├─ http/              # FastAPI + SSE 服务入口
│  ├─ metadata/          # MySQL 元数据与 SQLAlchemy 连接层
│  ├─ observability/     # 健康检查、审计事件
│  ├─ orchestration/     # LangGraph 工作流编排
│  ├─ planning/          # 复杂问数规划
│  ├─ response/          # 响应组装
│  ├─ security/          # SQLGuard、权限层、安全执行
│  ├─ semantic/          # 指标合同、维度合同、Join 合同、时间语义
│  ├─ sql_advisor/       # Vanna-style SQL advisor
│  ├─ sql_generation/    # SQL 生成与修复
│  ├─ tools/             # MCP 工具入口
│  └─ vector/            # Qdrant 检索与混合重排
├─ tests/                # 单元测试、集成测试、评测夹具
├─ docs/                 # 设计与实施文档
├─ NDEA_MASTER_CONTROL.md
└─ NDEA_PROGRESS_LEDGER.md
```

## 4. 环境要求

- Python：`>= 3.14`
- MySQL：用于元数据探查和安全 SQL 执行
- Qdrant：用于语义资产与黄金 SQL 检索

## 5. 安装

在 Windows PowerShell 下：

```powershell
py -3.14 -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -U pip
python -m pip install -e .[dev]
```

## 6. 配置

项目通过根目录 `.env` 读取配置，示例见 [.env.example](.env.example)。

当前默认向量库已经切到 `Qdrant`：

```env
NDEA_QDRANT_URL=http://8.137.15.201:6333
NDEA_QDRANT_COLLECTION=semantic_assets
NDEA_QDRANT_VECTOR_NAME=embedding
```

常用配置项说明：

- `NDEA_WORKFLOW_RUNTIME`
  - 默认 `langgraph`
- `NDEA_QDRANT_URL`
  - Qdrant 服务地址
- `NDEA_QDRANT_COLLECTION`
  - 语义资产集合名，默认 `semantic_assets`
- `NDEA_ENABLE_QUERY_EXECUTION`
  - 是否启用真实 SQL 执行
- `NDEA_ENABLE_SEMANTIC_RETRIEVAL`
  - 是否启用语义检索
- `NDEA_MYSQL_CONNECTION_BACKEND`
  - 默认 `sqlalchemy`

MySQL 最低需要配置：

```env
NDEA_MYSQL_HOST=127.0.0.1
NDEA_MYSQL_PORT=3306
NDEA_MYSQL_USER=root
NDEA_MYSQL_PASSWORD=
NDEA_MYSQL_DATABASE=campus
```

## 7. 启动方式

### 7.1 健康检查

```powershell
.\.venv\Scripts\python.exe -m ndea.main --check liveness
.\.venv\Scripts\python.exe -m ndea.main --check readiness
```

说明：

- `liveness` 只检查服务自身是否可启动
- `readiness` 会检查依赖状态
- 当前版本下，如果 `Qdrant` 可达但 `semantic_assets` 集合不存在，`readiness` 会判定为不就绪

### 7.2 启动 HTTP 服务

```powershell
.\.venv\Scripts\uvicorn.exe ndea.main:http_app --host 0.0.0.0 --port 8000
```

启动后可用入口：

- `GET /health/liveness`
- `GET /health/readiness`
- `POST /api/query-workflow`
- `POST /api/query-workflow/stream`
- `POST/GET /mcp/...`

### 7.3 启动 MCP 服务

如果你希望直接以 FastMCP 方式运行：

```powershell
.\.venv\Scripts\fastmcp.exe run src\ndea\main.py:app
```

如果需要 HTTP 模式：

```powershell
.\.venv\Scripts\fastmcp.exe run src\ndea\main.py:app -t http --host 127.0.0.1 -p 8000
```

## 8. HTTP 调用示例

### 8.1 同步问数

```json
POST /api/query-workflow
{
  "query_text": "按学院统计 2024 学年在校生人数",
  "query_vector": [0.1, 0.2, 0.3],
  "database": "campus",
  "execute": true,
  "request_context": {
    "trace_id": "trace-1",
    "request_id": "request-1",
    "actor_id": "user-1",
    "policy": {
      "allowed_tables": ["student", "department"]
    }
  }
}
```

### 8.2 流式问数

```text
POST /api/query-workflow/stream
```

返回为 SSE 事件流，事件名统一为 `workflow`，内容会按节点返回：

- `planner`
- `advisor`
- `generator`
- `executor`
- `repair`
- `final`

## 9. MCP 工具

当前已注册的主要 MCP 工具包括：

- `system_status`
- `inspect_table_schema`
- `execute_guarded_query`
- `mcp_vector_locator`
- `mcp_sql_rag_engine`
- `mcp_query_planner`
- `mcp_query_workflow`

## 10. Qdrant 使用说明

当前代码已经完成 `Milvus -> Qdrant` 切换，但要让语义检索真正可用，还需要满足两个条件：

1. Qdrant 中存在目标集合，例如 `semantic_assets`
2. 集合里已经导入语义资产

资产至少应包含：

- 指标合同 `metric_contract`
- 维度合同 `dimension_contract`
- Join 路径 `join_path`
- 时间语义 `time_semantics`
- 黄金 SQL `golden_sql`

如果 Qdrant 服务可达但集合为空，系统虽然能启动，但检索相关链路无法真正发挥效果。

## 11. 测试

运行全量测试：

```powershell
.\.venv\Scripts\python.exe -m pytest -q
```

项目当前包含：

- 单元测试
- 工作流集成测试
- 复杂问数测试
- 离线评测夹具
- Live smoke 测试（环境变量控制）

## 12. 开发建议

如果你要继续增强问数准确率，优先级建议如下：

1. 建好 Qdrant 中的语义资产集合并导入真实高校业务资产
2. 接入真实 embedding 流程
3. 扩展时间趋势、同比环比、drill-down 等复杂分析能力
4. 补齐 staging 环境的真实联调与回归数据集

## 13. 当前状态说明

这个仓库已经不是原型骨架，而是一套可运行的高校问数 Agent 基线系统。  
它已经具备完整主链，但是否能在真实高校场景里达到高准确率，仍然高度依赖：

- Qdrant 中的语义资产质量
- 黄金 SQL 质量
- 权限策略配置
- MySQL / Qdrant 实际数据环境

## 14. 相关文档

- [NDEA_MASTER_CONTROL.md](NDEA_MASTER_CONTROL.md)
- [NDEA_PROGRESS_LEDGER.md](NDEA_PROGRESS_LEDGER.md)

