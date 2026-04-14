from ndea.metadata.models import ColumnSchema, parse_mysql_enum_values


def test_parse_mysql_enum_values_returns_clean_values() -> None:
    assert parse_mysql_enum_values("enum('teacher','student')") == ["teacher", "student"]


def test_column_schema_extracts_enum_values_from_column_type() -> None:
    column = ColumnSchema(
        name="role",
        data_type="enum",
        column_type="enum('teacher','student')",
        is_nullable=False,
        comment="Role",
    )
    assert column.enum_values == ["teacher", "student"]
