from ndea.config import Settings
from ndea.metadata import MetadataIntrospector, open_mysql_connection


def get_metadata_introspector() -> MetadataIntrospector:
    settings = Settings()
    return MetadataIntrospector(lambda: open_mysql_connection(settings))


def inspect_table_schema(database: str, table_name: str) -> dict[str, object]:
    schema = get_metadata_introspector().describe_table(database, table_name)
    if hasattr(schema, "model_dump"):
        return schema.model_dump()
    return schema
