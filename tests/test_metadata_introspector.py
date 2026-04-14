from ndea.metadata.introspector import MetadataIntrospector


class FakeCursor:
    def __init__(self, rows: list[dict[str, object]]) -> None:
        self.rows = rows
        self.executed: list[tuple[str, tuple[object, ...] | None]] = []

    def execute(self, query: str, params: tuple[object, ...] | None = None) -> None:
        self.executed.append((query, params))

    def fetchall(self) -> list[dict[str, object]]:
        return self.rows

    def __enter__(self) -> "FakeCursor":
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        return False


class FakeConnection:
    def __init__(
        self,
        table_rows: list[dict[str, object]] | None = None,
        column_rows: list[dict[str, object]] | None = None,
    ) -> None:
        self.table_rows = table_rows or []
        self.column_rows = column_rows or []
        self.calls = 0

    def cursor(self, dictionary: bool = True) -> FakeCursor:
        self.calls += 1
        rows = self.table_rows if self.calls == 1 and self.table_rows else self.column_rows
        return FakeCursor(rows)

    def close(self) -> None:
        return None


def test_list_tables_normalizes_rows() -> None:
    inspector = MetadataIntrospector(
        lambda: FakeConnection(
            table_rows=[
                {"table_name": "student", "table_comment": "Students"},
            ]
        )
    )
    tables = inspector.list_tables("campus")
    assert tables[0].name == "student"
    assert tables[0].comment == "Students"


def test_describe_table_returns_columns() -> None:
    inspector = MetadataIntrospector(
        lambda: FakeConnection(
            column_rows=[
                {
                    "column_name": "role",
                    "data_type": "enum",
                    "column_type": "enum('teacher','student')",
                    "is_nullable": "NO",
                    "column_comment": "Role",
                }
            ]
        )
    )
    schema = inspector.describe_table("campus", "user_role")
    assert schema.table_name == "user_role"
    assert schema.columns[0].enum_values == ["teacher", "student"]
