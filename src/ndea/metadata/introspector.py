from collections.abc import Callable
from typing import Any

from ndea.metadata.models import ColumnSchema, TableSchemaDetail, TableSchemaSummary


class MetadataIntrospector:
    def __init__(self, connection_factory: Callable[[], Any]) -> None:
        self._connection_factory = connection_factory

    def list_tables(self, database: str) -> list[TableSchemaSummary]:
        connection = self._connection_factory()
        try:
            with connection.cursor(dictionary=True) as cursor:
                cursor.execute(
                    """
                    SELECT
                        TABLE_NAME AS table_name,
                        TABLE_COMMENT AS table_comment
                    FROM information_schema.TABLES
                    WHERE TABLE_SCHEMA = %s
                    ORDER BY TABLE_NAME
                    """,
                    (database,),
                )
                rows = cursor.fetchall()
        finally:
            connection.close()

        return [
            TableSchemaSummary(
                name=str(row["table_name"]),
                comment=str(row.get("table_comment") or ""),
            )
            for row in rows
        ]

    def describe_table(self, database: str, table_name: str) -> TableSchemaDetail:
        connection = self._connection_factory()
        try:
            with connection.cursor(dictionary=True) as cursor:
                cursor.execute(
                    """
                    SELECT
                        COLUMN_NAME AS column_name,
                        DATA_TYPE AS data_type,
                        COLUMN_TYPE AS column_type,
                        IS_NULLABLE AS is_nullable,
                        COLUMN_COMMENT AS column_comment
                    FROM information_schema.COLUMNS
                    WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s
                    ORDER BY ORDINAL_POSITION
                    """,
                    (database, table_name),
                )
                rows = cursor.fetchall()
        finally:
            connection.close()

        columns = [
            ColumnSchema(
                name=str(row["column_name"]),
                data_type=str(row["data_type"]),
                column_type=str(row["column_type"]),
                is_nullable=str(row["is_nullable"]).upper() == "YES",
                comment=str(row.get("column_comment") or ""),
            )
            for row in rows
        ]
        return TableSchemaDetail(database=database, table_name=table_name, columns=columns)
