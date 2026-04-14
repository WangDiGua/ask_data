# NDEA AI Development Master Control

## 1. Document Purpose

This document is the stable control document for the `Nexus Data Expert Agent (NDEA)` project.

It is written for AI-driven iterative development, not for traditional manual handoff.

Its purpose is to ensure that across multiple AI sessions we can consistently answer:

- What the project is
- What the system must do
- What architecture constraints are already fixed
- What modules exist
- What interfaces and behaviors should remain stable
- What decisions should not be re-litigated every session

This document should change slowly.
Implementation progress, current state, and next actions belong in `NDEA_PROGRESS_LEDGER.md`.

## 2. Project Definition

NDEA is a standalone Python MCP service running on top of the Nexus portal ecosystem.

Its role is to act as a domain expert data-questioning agent for university/private data environments. It receives natural-language questions from the Nexus portal, performs reasoning with ReAct-style orchestration, uses vector retrieval plus live metadata to understand business meaning, generates guarded SQL, executes only safe read-only queries, and returns text, table, and visualization-ready results.

NDEA does not own the large model itself. The reasoning model is selected by the Nexus portal and passed through the request context. NDEA is responsible for orchestration, domain grounding, SQL generation constraints, safety controls, and structured result output.

## 3. Core Product Goal

Build an intelligent data-questioning agent that is:

- Accurate in university business semantics
- Safe against over-permission and risky SQL
- Grounded in real schema instead of stale docs
- Strong at recovering from SQL errors
- Easy for the Nexus portal to call through MCP
- Easy for future AI sessions to continue building without losing context

## 4. Non-Negotiable Project Constraints

These are fixed constraints and should not be casually changed in later AI sessions.

### 4.1 Architecture Constraints

- NDEA is a standalone Python service
- NDEA communicates through MCP
- Nexus portal remains the model gateway and conversation entry point
- NDEA does not embed its own permanent model provider contract as the primary control plane
- NDEA uses ReAct-style internal orchestration
- NDEA uses vector retrieval as a first-class capability, not an optional addon
- NDEA uses live metadata fetching as a first-class capability, not a static offline-only schema approach

### 4.2 Data Access Constraints

- Query execution is read-only
- Unsafe SQL must be blocked before execution
- Permission filtering cannot rely only on prompt instructions
- Table-level and row-level controls must be enforced in execution-related layers
- Explain/cost guard is mandatory before final execution for non-trivial queries

### 4.3 Product Behavior Constraints

- The system should prefer correctness over answering too quickly
- The system may ask clarifying questions when the request is ambiguous
- The system should provide structured outputs suitable for portal rendering
- The system should preserve observability through traceable execution metadata

## 5. System Role Boundary

### 5.1 What Nexus Portal Owns

- User-facing chat entry
- Model selection
- Primary authentication session
- User token/context forwarding
- Final frontend rendering
- Multi-tool orchestration at the portal level when needed

### 5.2 What NDEA Owns

- Domain reasoning workflow
- Retrieval over semantic assets
- Live schema and metadata inspection
- Query planning for NL2SQL
- SQL generation constraints
- SQL guardrails and safe execution
- Structured data result generation
- Visualization payload generation
- Error observation and self-repair loop

### 5.3 What NDEA Must Not Assume

- That prompt-only permission instructions are sufficient
- That schema docs outside the database are always current
- That model outputs are correct on the first try
- That university business terms map cleanly to physical table names

## 6. Target Capability Scope

NDEA is expected to support these question categories:

- Single metric lookup
- Aggregated reporting
- Dimension breakdown
- Ranking and Top N queries
- Trend analysis over time
- Period-over-period comparison
- Business-definition-based filtering
- Drill-down from summary to detail when permission allows
- Visualization-ready outputs for charts and tables

NDEA is expected to handle domain language such as university-specific business jargon, aliases, unofficial naming habits, and implicit metric definitions.

## 7. High-Level Architecture

The system consists of these major parts:

1. Nexus portal request entry
2. NDEA MCP server
3. ReAct orchestration engine
4. Vector retrieval engine
5. Live metadata inspector
6. SQL generation and repair module
7. Safe execution and permission enforcement module
8. Structured response assembler

Logical flow:

1. User asks a question in Nexus portal
2. Portal forwards model context and user permission context to NDEA
3. NDEA classifies intent and starts ReAct orchestration
4. Vector engine recalls glossary, metric semantics, schema hints, and golden SQL examples
5. Metadata inspector fetches live schema/comment/enum information
6. Planner builds a query plan
7. SQL generator produces candidate SQL
8. Safe executor performs guard checks, permission injection, and execution
9. If execution fails, repair loop uses observed errors to revise SQL
10. Final answer is returned as text, table, and optional visualization payload

## 8. Core Design Principles

### 8.1 Security First

The system must fail safely. If a query is ambiguous, too expensive, or outside permission, NDEA should stop, narrow scope, or ask for clarification.

### 8.2 Semantic + Physical Dual Drive

Vector retrieval solves semantic understanding.
Live metadata solves current physical truth.
Neither is sufficient alone.

### 8.3 Real Schema Over Stale Documentation

When there is conflict between offline docs and live database metadata, live metadata is the source of truth for execution-related behavior.

### 8.4 Tool Atomicity

Each MCP tool should have a clear single responsibility, stable interface, and replaceable internals.

### 8.5 Recoverable Execution

SQL generation should be treated as iterative. Failure observation and correction are part of the normal workflow, not an exceptional edge path.

### 8.6 AI Session Continuity

Decisions, module boundaries, and progress status should remain legible to future AI sessions. Development artifacts should always optimize for resumability.

## 9. Core Internal Workflow

The internal reasoning loop should follow this conceptual sequence:

1. Intent understanding
2. Semantic recall
3. Live schema verification
4. Query plan construction
5. SQL candidate generation
6. Permission and risk guarding
7. Safe execution
8. Error observation
9. SQL repair if needed
10. Result assembly

This means NDEA is not just an NL2SQL generator. It is an orchestrated reasoning agent around NL2SQL.

## 10. Core Semantic Engine

NDEA relies on a multi-asset semantic layer backed by vector search.

### 10.1 Golden SQL Assets

Natural language to SQL exemplars for difficult and representative business questions.

Purpose:

- Few-shot grounding
- Query pattern transfer
- Join-path guidance
- Metric formula reuse

### 10.2 Business Glossary Assets

Domain phrases, aliases, jargon, and business definitions.

Examples include terms like:

- academic status
- enrollment status
- delayed exam
- innovation project
- full-time faculty

These terms need semantic definitions and field-level grounding.

### 10.3 Schema Semantic Assets

Vectorized representations of table names, column comments, table comments, enum notes, and alias-rich descriptions.

Purpose:

- Link business words to physical schema
- Reduce wrong-table selection

### 10.4 Metric Semantic Assets

Business metric definitions should be treated as first-class knowledge.

Each metric should ideally define:

- metric name
- aliases
- business meaning
- grain
- aggregation logic
- required filters
- time semantics
- mapped tables and columns

### 10.5 Join Path Knowledge Assets

Join relationships should not depend only on the model guessing from names.

Join-path assets should describe:

- source table
- target table
- join keys
- join type
- semantic meaning
- cardinality caveats

## 11. Required MCP Tool Set

The initial canonical tool set for NDEA is:

### 11.1 `mcp_vector_locator`

Purpose:

- Recall business terms
- Recall candidate metrics
- Recall candidate tables and columns
- Recall join-path hints

### 11.2 `mcp_sql_rag_engine`

Purpose:

- Retrieve similar golden SQL cases
- Provide few-shot SQL context
- Help construct candidate SQL patterns

### 11.3 `mcp_db_inspector`

Purpose:

- Fetch live DDL
- Fetch comments
- Fetch enums
- Fetch live schema facts needed for planning and execution

### 11.4 `mcp_safe_executor`

Purpose:

- Validate SQL safety
- Inject permission constraints
- Run explain or equivalent guard checks
- Execute read-only SQL
- Return structured result or structured failure

## 12. Query Planning Model

Before SQL generation, the system should explicitly determine:

- What the user is asking for
- Whether the question is metric, detail, ranking, trend, comparison, or drill-down
- Required dimensions
- Required filters
- Required time scope
- Candidate tables
- Candidate joins
- Whether clarification is required before execution

The planner should exist as its own logical stage even if implementation initially lives inside a larger orchestration module.

## 13. Security and Permission Model

Security is mandatory and layered.

### 13.1 Resource Masking

Tables and resources outside the user scope should be hidden from retrieval and planning wherever possible.

### 13.2 Row-Level Security

Identity-based filtering should be injected or enforced in execution-related logic.

### 13.3 Field-Level Restrictions

Sensitive columns should support masking, omission, or denial based on policy.

### 13.4 SQL Guarding

The system should prevent:

- write operations
- destructive statements
- multi-statement abuse
- dangerous functions where policy disallows them
- excessive scans beyond threshold
- execution without required filters where policy demands them

### 13.5 Auditability

Each request should be traceable with:

- trace id
- user context id
- selected model metadata
- chosen tool path
- generated SQL summary
- execution guard result
- final outcome

## 14. Error Repair Philosophy

SQL failure is expected in real-world intelligent querying. NDEA should treat database errors as structured observations.

Common repair triggers include:

- unknown column
- unknown table
- invalid aggregate
- ambiguous column
- enum mismatch
- join path mismatch
- permission-filter conflict

Repair should attempt bounded correction. If the query remains ambiguous or unsafe after limited retries, the system should stop and either ask for clarification or return a constrained failure response.

## 15. Response Types

NDEA should support three primary output shapes:

### 15.1 Text Response

Human-readable answer with concise explanation.

### 15.2 Tabular Response

Structured rows, columns, pagination or truncation metadata, and summary information.

### 15.3 Visualization Response

Structured chart payload aligned with the Nexus portal visualization contract.
NDEA should return data and chart semantics, not frontend-specific executable code.
The canonical chart payload format is `Apache ECharts option JSON` plus raw source data and summary metadata.

NDEA itself does not include or own a frontend.
Rendering belongs to Nexus or another caller that consumes the structured response.

## 16. Integration Contract with Nexus

Nexus should pass structured context into NDEA, including at minimum:

- user identity
- role or policy context
- organization or tenant context
- allowed resource scope
- selected model information
- trace id
- conversation carryover context if available

NDEA should return structured results that Nexus can reliably render and audit.

## 17. Service-Only Delivery Boundary

NDEA is delivered as a backend service only.

This project does not include:

- a standalone frontend application
- built-in dashboard pages
- browser-side chart rendering code
- user-facing chat UI

This project does include:

- MCP service runtime
- tool registration and orchestration
- database connectivity and metadata inspection
- vector retrieval capability
- guarded SQL execution
- structured text/table/chart payload output
- integration contracts for Nexus consumption

## 18. Selected Technology Stack

The following implementation choices are already fixed for this project unless explicitly reopened:

- MCP Python framework: `FastMCP`
- Vector database: `Milvus`
- Initial relational database scope: `MySQL`
- SQL parsing and AST engine: `SQLGlot`
- SQL safety model: `SQLGlot AST analysis + custom NDEA rule engine`
- Visualization payload format: `Apache ECharts`

### 18.1 FastMCP

FastMCP is the chosen Python MCP framework for service construction, tool exposure, and protocol-facing server ergonomics.

### 18.2 Milvus

Milvus is the chosen vector backend for semantic assets, including glossary retrieval, golden SQL retrieval, schema semantic indexing, metric semantics, and join-path knowledge.

### 18.3 MySQL

The first supported operational relational database is MySQL.
Other databases may be added later, but current design and implementation should optimize for MySQL-first correctness.

### 18.4 SQLGlot + Custom Rule Engine

`SQLGlot` is the chosen SQL parser and AST layer.

It should be used for:

- SQL parsing
- dialect-aware normalization
- AST inspection
- SQL rewriting when required
- guard-stage structural validation

Security policy enforcement should not rely on SQLGlot alone.
NDEA should add a custom rule engine on top for:

- read-only enforcement
- statement-type allowlisting
- multi-statement blocking
- function and clause restrictions
- required filter enforcement
- permission-aware SQL rewriting or rejection
- explain and cost guard integration

### 18.5 Apache ECharts

Visualization responses should target `Apache ECharts` option-compatible payloads.
NDEA is responsible for returning structured chart semantics and data, while Nexus is responsible for rendering.

## 19. Recommended Python Module Map

The Python service should eventually be split into modules with boundaries similar to:

- `app/config`
- `app/protocol`
- `app/agent`
- `app/planner`
- `app/tools`
- `app/vector`
- `app/metadata`
- `app/sql`
- `app/security`
- `app/executor`
- `app/response`
- `app/observability`

This is a guidance boundary map, not a final implementation lock.

## 20. Fixed Decisions Already Made

These decisions are already taken and should be preserved unless there is a strong reason to reopen them:

- The project is a standalone Python implementation
- The project is service-only and does not ship its own frontend
- The service is built for AI-led iterative development
- The service integrates with Nexus through MCP
- FastMCP is the chosen MCP Python framework
- Milvus is the chosen vector database
- MySQL is the initial relational database target
- SQLGlot plus a custom NDEA rule engine is the chosen SQL safety/parsing approach
- Apache ECharts is the required visualization payload target
- Vector retrieval is core, not optional
- Live metadata is core, not optional
- ReAct orchestration is core
- Safe execution with permission enforcement is core
- The document system should help future AI sessions resume work quickly
- We use a stable master-control document plus a dynamic progress ledger

## 21. What Future AI Sessions Should Read First

At the start of every future development session, read in this order:

1. `NDEA_MASTER_CONTROL.md`
2. `NDEA_PROGRESS_LEDGER.md`
3. Current repository structure
4. Most recent changed files related to the active task

If there is a conflict between the master control document and the progress ledger:

- The master control document wins for stable architecture and constraints
- The progress ledger wins for current implementation status

## 22. Change Policy for This Document

This document should be updated only when one of the following changes:

- project scope
- system boundary
- fixed architecture decision
- MCP tool inventory
- security model
- output contract
- semantic asset model

Routine implementation progress should not be written here.
