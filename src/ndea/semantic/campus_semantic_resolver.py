from __future__ import annotations

from ndea.planning.core_registry import CoreFieldDefinition, CoreTableDefinition, get_core_table, join_rule
from ndea.query_v2 import QueryIR, SemanticHint


IDENTIFIER_FIELD_BY_TABLE: dict[str, dict[str, str]] = {
    "dcstu": {"学号": "student_no"},
    "dcemp": {"工号": "staff_no", "教工号": "staff_no", "职工号": "staff_no"},
    "t_bsdt_jzgygcg": {"工号": "staff_no", "教工号": "staff_no", "职工号": "staff_no"},
    "t_bsdt_xsygcg": {"学号": "student_no"},
}


class CampusSemanticResolver:
    def resolve(self, ir: QueryIR) -> SemanticHint:
        table = self._resolve_table(ir)
        if table is None:
            return SemanticHint(
                base_table=None,
                candidate_tables=[],
                dimensions=[],
                filters=[],
                joins=[],
                ambiguities=list(ir.ambiguities),
                confidence=max(0.15, ir.confidence - 0.2),
            )

        dimensions = [self._dimension_payload(table, item) for item in ir.dimensions]
        dimensions = [item for item in dimensions if item is not None]
        joins = self._resolve_joins(table, dimensions)
        filters = self._default_filters(table)
        filters.extend(self._semantic_filters(table, ir.filters))
        filters.extend(self._identifier_filters(table, ir.identifiers))
        if ir.time_scope and ir.time_scope.get("value") and table.time_field:
            filters.append(f"{table.time_field} = '{ir.time_scope['value']}'")
        filters = list(dict.fromkeys(filters))

        return SemanticHint(
            base_table=table.table,
            measure_expression="COUNT(*)",
            entity_scope=ir.entity_scope or table.role,
            dimensions=dimensions,
            filters=filters,
            joins=joins,
            candidate_tables=list(dict.fromkeys([table.table, *[join["right_table"] for join in joins]])),
            ambiguities=list(ir.ambiguities),
            confidence=self._confidence(table, ir, dimensions),
        )

    def _resolve_table(self, ir: QueryIR) -> CoreTableDefinition | None:
        campus_terms = set(ir.campus_terms)
        if "teacher_outbound" in campus_terms:
            return get_core_table("t_bsdt_jzgygcg")
        if "student_outbound" in campus_terms:
            return get_core_table("t_bsdt_xsygcg")
        if "visiting_expert" in campus_terms:
            return get_core_table("t_gjc_lfzj")
        if ir.entity_scope == "student":
            return get_core_table("dcstu")
        if ir.entity_scope == "faculty":
            return get_core_table("dcemp")
        if ir.entity_scope == "organization":
            return get_core_table("dcorg")
        if ir.metric == "campus_population":
            return get_core_table("dcstu")
        return None

    def _dimension_payload(self, table: CoreTableDefinition, dimension: str) -> dict[str, str] | None:
        mapping: dict[str, CoreFieldDefinition | None] = {
            "college": self._field_by_id(table, "college_name"),
            "organization": self._field_by_id(table, "org_name") or self._field_by_id(table, "org_code"),
        }
        field = mapping.get(dimension)
        if field is None:
            return None
        return {
            "dimension_id": field.field_id,
            "name": field.label,
            "expression": field.expression,
            "output_alias": field.output_alias,
            "table": field.table,
        }

    def _resolve_joins(
        self,
        table: CoreTableDefinition,
        dimensions: list[dict[str, str]],
    ) -> list[dict[str, str]]:
        joins: list[dict[str, str]] = []
        for dimension in dimensions:
            dimension_table = dimension.get("table")
            if not dimension_table or dimension_table == table.table:
                continue
            join = join_rule(table.table, dimension_table)
            if join is None:
                continue
            joins.append(
                {
                    "join_id": join[0],
                    "join_sql": join[1],
                    "left_table": table.table,
                    "right_table": join[2],
                    "join_type": "INNER",
                }
            )
        return joins

    def _default_filters(self, table: CoreTableDefinition) -> list[str]:
        if table.table == "dcstu":
            return ["dcstu.SFZX = '是'"]
        if table.table == "dcemp":
            return ["dcemp.RYZTMC = '在岗'"]
        return []

    def _semantic_filters(self, table: CoreTableDefinition, filters: list[str]) -> list[str]:
        resolved: list[str] = []
        for item in filters:
            if item == "在校" and table.table == "dcstu":
                resolved.append("dcstu.SFZX = '是'")
            if item == "在岗" and table.table == "dcemp":
                resolved.append("dcemp.RYZTMC = '在岗'")
        return resolved

    def _identifier_filters(self, table: CoreTableDefinition, identifiers: list[dict[str, str]]) -> list[str]:
        resolved: list[str] = []
        field_map = IDENTIFIER_FIELD_BY_TABLE.get(table.table, {})
        for identifier in identifiers:
            field_id = field_map.get(identifier["type"])
            if not field_id:
                continue
            field = self._field_by_id(table, field_id)
            if field is None:
                continue
            value = identifier["value"].replace("'", "''")
            resolved.append(f"{field.expression} = '{value}'")
        return resolved

    def _confidence(
        self,
        table: CoreTableDefinition,
        ir: QueryIR,
        dimensions: list[dict[str, str]],
    ) -> float:
        confidence = ir.confidence + 0.15
        if table.table in {"dcstu", "dcemp", "t_bsdt_jzgygcg", "t_bsdt_xsygcg", "t_gjc_lfzj"}:
            confidence += 0.05
        if dimensions:
            confidence += 0.05
        if ir.identifiers:
            confidence += 0.05
        if ir.ambiguities:
            confidence -= 0.1
        return round(max(0.2, min(0.98, confidence)), 2)

    def _field_by_id(self, table: CoreTableDefinition, field_id: str) -> CoreFieldDefinition | None:
        for field in table.fields:
            if field.field_id == field_id:
                return field
        return None
