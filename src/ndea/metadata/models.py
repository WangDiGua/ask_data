import re

from pydantic import BaseModel, Field, model_validator


def parse_mysql_enum_values(column_type: str) -> list[str]:
    matches = re.findall(r"'((?:[^'\\\\]|\\\\.)*)'", column_type)
    return [match.replace("\\'", "'") for match in matches]


class TableSchemaSummary(BaseModel):
    name: str
    comment: str = ""


class ColumnSchema(BaseModel):
    name: str
    data_type: str
    column_type: str
    is_nullable: bool
    comment: str = ""
    enum_values: list[str] = Field(default_factory=list)

    @model_validator(mode="after")
    def fill_enum_values(self) -> "ColumnSchema":
        if not self.enum_values and self.data_type.lower() == "enum":
            self.enum_values = parse_mysql_enum_values(self.column_type)
        return self


class TableSchemaDetail(BaseModel):
    database: str
    table_name: str
    columns: list[ColumnSchema]
