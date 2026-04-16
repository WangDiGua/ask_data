from __future__ import annotations

from ndea.metadata.introspector import MetadataIntrospector
from ndea.metadata.models import TableSchemaDetail, TableSchemaSummary
from ndea.resolution import SchemaResolverRepository


class MySQLSchemaRepository(SchemaResolverRepository):
    def __init__(self, introspector: MetadataIntrospector) -> None:
        self._introspector = introspector

    def list_tables(self, database: str) -> list[TableSchemaSummary]:
        return self._introspector.list_tables(database)

    def describe_table(self, database: str, table_name: str) -> TableSchemaDetail:
        return self._introspector.describe_table(database, table_name)
