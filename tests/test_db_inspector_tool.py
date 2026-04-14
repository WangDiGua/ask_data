from ndea.tools.db_inspector import inspect_table_schema


class FakeInspector:
    def describe_table(self, database: str, table_name: str) -> dict[str, object]:
        return {
            "database": database,
            "table_name": table_name,
            "columns": [],
        }


def test_inspect_table_schema_uses_injected_inspector(monkeypatch) -> None:
    monkeypatch.setattr(
        "ndea.tools.db_inspector.get_metadata_introspector",
        lambda: FakeInspector(),
    )
    payload = inspect_table_schema("campus", "student")
    assert payload["database"] == "campus"
    assert payload["table_name"] == "student"
