from __future__ import annotations

from dataclasses import dataclass

from ndea.metadata.models import TableSchemaDetail, TableSchemaSummary
from ndea.query_v2 import QueryIR, SchemaHint


class SchemaResolverRepository:
    def list_tables(self, database: str) -> list[TableSchemaSummary]:
        raise NotImplementedError

    def describe_table(self, database: str, table_name: str) -> TableSchemaDetail:
        raise NotImplementedError


@dataclass(slots=True)
class SchemaResolver:
    repository: SchemaResolverRepository

    def resolve(self, database: str | None, ir: QueryIR, query_text: str) -> SchemaHint:
        if not database:
            return SchemaHint(confidence=0.0)

        tables = self.repository.list_tables(database)
        query_tokens = self._tokens(query_text)
        scored_tables: list[tuple[float, TableSchemaSummary]] = []
        for table in tables:
            score = 0.0
            haystack = f"{table.name} {table.comment}".lower()
            for token in query_tokens:
                if token and token in haystack:
                    score += 0.15
            if score > 0:
                scored_tables.append((score, table))

        scored_tables.sort(key=lambda item: item[0], reverse=True)
        candidate_tables = [item[1].name for item in scored_tables[:3]]
        base_table = candidate_tables[0] if candidate_tables else None
        dimensions: list[dict[str, str]] = []

        if base_table is not None:
            detail = self.repository.describe_table(database, base_table)
            lowered_query = query_text.lower()
            for column in detail.columns:
                name = column.name.lower()
                comment = column.comment.lower()
                if any(token in lowered_query or token in query_text for token in ("学院", "院系")) and any(
                    marker in f"{name} {comment}" for marker in ("yx", "college", "院", "系")
                ):
                    dimensions.append(
                        {
                            "dimension_id": column.name,
                            "name": column.comment or column.name,
                            "expression": f"{base_table}.{column.name}",
                            "output_alias": column.name.lower(),
                            "table": base_table,
                        }
                    )
                    break

        confidence = min(0.75, max((scored_tables[0][0] if scored_tables else 0.0), 0.0))
        return SchemaHint(
            base_table=base_table,
            candidate_tables=candidate_tables,
            dimensions=dimensions,
            filters=[],
            joins=[],
            confidence=round(confidence, 2),
        )

    def _tokens(self, query_text: str) -> list[str]:
        lowered = query_text.lower()
        return [
            token
            for token in (
                "student",
                "teacher",
                "faculty",
                "organization",
                "department",
                "college",
                "学生",
                "教师",
                "教职工",
                "组织",
                "单位",
                "部门",
                "学院",
                "院系",
                "出访",
                "出国",
                "来访",
            )
            if token in lowered or token in query_text
        ]
