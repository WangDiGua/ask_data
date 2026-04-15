from __future__ import annotations

from dataclasses import dataclass
import re

from ndea.planning.models import (
    LookupIdentifierPayload,
    QueryPlanPayload,
    ResolvedDimensionPayload,
    ResolvedFilterPayload,
)


@dataclass(frozen=True)
class IdentifierBinding:
    identifier_type: str
    label: str
    aliases: tuple[str, ...]
    table: str
    column: str
    priority: int

    @property
    def expression(self) -> str:
        return f"{self.table}.{self.column}"


@dataclass(frozen=True)
class AttributeBinding:
    table: str
    column: str
    output_alias: str
    priority: int

    @property
    def expression(self) -> str:
        return f"{self.table}.{self.column}"


@dataclass(frozen=True)
class AttributeDefinition:
    attribute_id: str
    name: str
    aliases: tuple[str, ...]
    bindings: tuple[AttributeBinding, ...]

    def binding_for_table(self, table: str) -> AttributeBinding | None:
        candidates = [binding for binding in self.bindings if binding.table == table]
        if not candidates:
            return None
        return max(candidates, key=lambda binding: binding.priority)


@dataclass(frozen=True)
class RecordColumnBinding:
    name: str
    table: str
    column: str
    output_alias: str

    @property
    def expression(self) -> str:
        return f"{self.table}.{self.column}"


@dataclass(frozen=True)
class RecordDefinition:
    record_id: str
    name: str
    aliases: tuple[str, ...]
    identifier_type: str
    table: str
    identifier_column: str
    columns: tuple[RecordColumnBinding, ...]
    priority: int


@dataclass(frozen=True)
class IdentifierMatch:
    binding: IdentifierBinding
    value: str
    matched_alias: str | None
    generic: bool = False


IDENTIFIER_BINDINGS: tuple[IdentifierBinding, ...] = (
    IdentifierBinding("staff_no", "工号", ("工号", "教工号", "职工号"), "dcemp", "XGH", 100),
    IdentifierBinding("staff_no", "工号", ("工号", "教工号", "职工号"), "t_bsdt_jzgygcg", "ZGH", 90),
    IdentifierBinding("staff_no", "工号", ("工号", "教工号", "职工号"), "t_gjc_lfzj", "SBRGH", 35),
    IdentifierBinding("staff_no", "工号", ("工号", "教工号", "职工号"), "t_gjc_lfzj", "YQRGH", 30),
    IdentifierBinding("student_no", "学号", ("学号", "学工号"), "dcstu", "XGH", 100),
    IdentifierBinding("student_no", "学号", ("学号", "学工号"), "t_bsdt_xsygcg", "ZGH", 90),
)


ATTRIBUTE_DEFINITIONS: tuple[AttributeDefinition, ...] = (
    AttributeDefinition(
        "person_name",
        "姓名",
        ("姓名", "名字", "是谁", "叫什么"),
        (
            AttributeBinding("dcemp", "XM", "name", 100),
            AttributeBinding("dcstu", "XM", "name", 100),
            AttributeBinding("t_bsdt_jzgygcg", "XM", "name", 80),
            AttributeBinding("t_bsdt_xsygcg", "XM", "name", 80),
            AttributeBinding("t_gjc_lfzj", "ZJXM", "name", 40),
        ),
    ),
    AttributeDefinition(
        "staff_org_name",
        "所在单位",
        ("所在单位", "所属单位", "哪个单位", "什么单位", "单位", "部门"),
        (AttributeBinding("dcemp", "SZDWMC", "org_name", 100),),
    ),
    AttributeDefinition(
        "student_college_name",
        "学院",
        ("所在学院", "所在院系", "学院", "院系"),
        (AttributeBinding("dcstu", "YXMC", "college_name", 100),),
    ),
    AttributeDefinition(
        "major_name",
        "专业",
        ("专业", "专业名称"),
        (AttributeBinding("dcstu", "ZYMC", "major_name", 100),),
    ),
    AttributeDefinition(
        "study_level",
        "培养层次",
        ("培养层次", "层次", "学历层次"),
        (AttributeBinding("dcstu", "PYCCMC", "level_name", 100),),
    ),
    AttributeDefinition(
        "status_name",
        "状态",
        ("人员状态", "在岗状态", "在校状态", "是否在校", "状态"),
        (
            AttributeBinding("dcemp", "RYZTMC", "status_name", 100),
            AttributeBinding("dcstu", "SFZX", "status_name", 100),
        ),
    ),
    AttributeDefinition(
        "gender_name",
        "性别",
        ("性别",),
        (
            AttributeBinding("dcemp", "XB", "gender_name", 100),
            AttributeBinding("dcstu", "XBMC", "gender_name", 100),
            AttributeBinding("t_gjc_lfzj", "XB", "gender_name", 35),
        ),
    ),
    AttributeDefinition(
        "title_name",
        "职称",
        ("职称",),
        (
            AttributeBinding("t_bsdt_jzgygcg", "ZC", "title_name", 100),
            AttributeBinding("t_gjc_lfzj", "ZC", "title_name", 40),
        ),
    ),
    AttributeDefinition(
        "admin_position",
        "行政职务",
        ("行政职务", "职务"),
        (
            AttributeBinding("t_bsdt_jzgygcg", "XZZW", "position_name", 100),
            AttributeBinding("t_gjc_lfzj", "ZW", "position_name", 45),
        ),
    ),
    AttributeDefinition(
        "dispatch_org_name",
        "派出单位",
        ("派出单位",),
        (
            AttributeBinding("t_bsdt_jzgygcg", "PCDW", "dispatch_org_name", 100),
            AttributeBinding("t_bsdt_xsygcg", "PCDW", "dispatch_org_name", 100),
        ),
    ),
    AttributeDefinition(
        "political_status",
        "政治面貌",
        ("政治面貌",),
        (AttributeBinding("dcstu", "ZZMMMC", "political_status_name", 100),),
    ),
    AttributeDefinition(
        "student_category",
        "学生类别",
        ("学生类别", "类别"),
        (AttributeBinding("dcstu", "XSLBMC", "student_category_name", 100),),
    ),
)

GENERIC_INFO_ALIASES = ("信息", "详情", "基本信息", "资料", "档案")

DEFAULT_ATTRIBUTES_BY_IDENTIFIER: dict[str, tuple[str, ...]] = {
    "staff_no": ("person_name", "staff_org_name", "status_name"),
    "student_no": ("person_name", "student_college_name", "major_name", "study_level"),
}


RECORD_DEFINITIONS: tuple[RecordDefinition, ...] = (
    RecordDefinition(
        "staff_outbound_records",
        "\u51fa\u56fd\u8bb0\u5f55",
        (
            "\u51fa\u56fd\u8bb0\u5f55",
            "\u56e0\u516c\u51fa\u56fd\u8bb0\u5f55",
            "\u51fa\u56fd\u660e\u7ec6",
            "\u56e0\u516c\u51fa\u56fd\u660e\u7ec6",
            "\u51fa\u8bbf\u8bb0\u5f55",
            "\u51fa\u8bbf\u660e\u7ec6",
        ),
        "staff_no",
        "t_bsdt_jzgygcg",
        "ZGH",
        (
            RecordColumnBinding("\u59d3\u540d", "t_bsdt_jzgygcg", "XM", "name"),
            RecordColumnBinding("\u5e74\u4efd", "t_bsdt_jzgygcg", "NF", "year"),
            RecordColumnBinding("\u6d3e\u51fa\u5355\u4f4d", "t_bsdt_jzgygcg", "PCDW", "dispatch_org_name"),
            RecordColumnBinding("\u51fa\u8bbf\u4efb\u52a1\u7c7b\u578b", "t_bsdt_jzgygcg", "CFRWLX", "mission_type"),
            RecordColumnBinding("\u51fa\u8bbf\u56fd\u5bb6\u5730\u533a", "t_bsdt_jzgygcg", "CFGJHDQ", "country_region"),
            RecordColumnBinding("\u51fa\u53d1\u65e5\u671f", "t_bsdt_jzgygcg", "CFNF", "depart_date"),
            RecordColumnBinding("\u6210\u884c\u65e5\u671f", "t_bsdt_jzgygcg", "CJSJ", "start_date"),
            RecordColumnBinding("\u5165\u5883\u65e5\u671f", "t_bsdt_jzgygcg", "RJSJ", "return_date"),
            RecordColumnBinding("\u9080\u8bf7\u5355\u4f4d", "t_bsdt_jzgygcg", "YQRDWZWMC", "host_org_name"),
            RecordColumnBinding("\u6279\u4ef6\u53f7", "t_bsdt_jzgygcg", "PJH", "approval_number"),
        ),
        100,
    ),
    RecordDefinition(
        "student_outbound_records",
        "\u51fa\u56fd\u8bb0\u5f55",
        (
            "\u51fa\u56fd\u8bb0\u5f55",
            "\u56e0\u516c\u51fa\u56fd\u8bb0\u5f55",
            "\u51fa\u56fd\u660e\u7ec6",
            "\u56e0\u516c\u51fa\u56fd\u660e\u7ec6",
            "\u51fa\u8bbf\u8bb0\u5f55",
            "\u51fa\u8bbf\u660e\u7ec6",
        ),
        "student_no",
        "t_bsdt_xsygcg",
        "ZGH",
        (
            RecordColumnBinding("\u59d3\u540d", "t_bsdt_xsygcg", "XM", "name"),
            RecordColumnBinding("\u5e74\u4efd", "t_bsdt_xsygcg", "NF", "year"),
            RecordColumnBinding("\u6d3e\u51fa\u5355\u4f4d", "t_bsdt_xsygcg", "PCDW", "dispatch_org_name"),
            RecordColumnBinding("\u51fa\u8bbf\u4efb\u52a1\u7c7b\u578b", "t_bsdt_xsygcg", "CFRWLX", "mission_type"),
            RecordColumnBinding("\u51fa\u8bbf\u56fd\u5bb6\u5730\u533a", "t_bsdt_xsygcg", "CFGJHDQ", "country_region"),
            RecordColumnBinding("\u51fa\u53d1\u65e5\u671f", "t_bsdt_xsygcg", "CFNF", "depart_date"),
            RecordColumnBinding("\u6210\u884c\u65e5\u671f", "t_bsdt_xsygcg", "CJSJ", "start_date"),
            RecordColumnBinding("\u5165\u5883\u65e5\u671f", "t_bsdt_xsygcg", "RJSJ", "return_date"),
            RecordColumnBinding("\u9080\u8bf7\u5355\u4f4d", "t_bsdt_xsygcg", "YQRDWZWMC", "host_org_name"),
            RecordColumnBinding("\u6279\u4ef6\u53f7", "t_bsdt_xsygcg", "PJH", "approval_number"),
        ),
        95,
    ),
)


def build_attribute_lookup_plan(query_text: str) -> QueryPlanPayload | None:
    requested_attributes = _extract_requested_attributes(query_text)
    identifier_matches = _extract_identifier_matches(query_text, requested_attributes)
    if not identifier_matches:
        return None

    if not requested_attributes and any(alias in query_text for alias in GENERIC_INFO_ALIASES):
        requested_attributes = _default_attributes(identifier_matches[0].binding.identifier_type)

    if not requested_attributes:
        return _clarification_payload(
            query_text=query_text,
            identifier_matches=identifier_matches,
            clarification_reason="Need target attributes before planning attribute lookup",
            clarification_questions=["请说明你要查询该编号的哪些属性，例如姓名、职称、所在单位、专业。"],
        )

    resolved = _resolve_best_candidate(identifier_matches, requested_attributes)
    if resolved is None:
        return _clarification_payload(
            query_text=query_text,
            identifier_matches=identifier_matches,
            clarification_reason="Requested attributes are not available from a single authoritative table",
            clarification_questions=["该编号涉及的属性分布在不同表里，请拆开查询，或明确要查基本信息还是业务记录字段。"],
        )

    identifier_match, attribute_bindings = resolved
    candidate_tables = _ordered_candidate_tables(identifier_matches)
    lookup_identifier = LookupIdentifierPayload(
        identifier_type=identifier_match.binding.identifier_type,
        label=identifier_match.binding.label,
        table=identifier_match.binding.table,
        column=identifier_match.binding.column,
        expression=identifier_match.binding.expression,
        value=identifier_match.value,
    )
    lookup_attributes = [
        ResolvedDimensionPayload(
            dimension_id=definition.attribute_id,
            name=definition.name,
            expression=binding.expression,
            output_alias=binding.output_alias,
            table=binding.table,
        )
        for definition, binding in zip(requested_attributes, attribute_bindings, strict=True)
    ]

    return QueryPlanPayload(
        query_text=query_text,
        intent_type="attribute_lookup",
        summary=(
            f"Resolved attribute lookup for {identifier_match.binding.label}"
            f" {identifier_match.value} on table {identifier_match.binding.table}"
        ),
        clarification_required=False,
        clarification_reason=None,
        candidate_tables=candidate_tables,
        candidate_metrics=[],
        join_hints=[],
        selected_sql_asset_id=None,
        selected_sql=None,
        filters=[
            ResolvedFilterPayload(
                filter_id=f"{identifier_match.binding.identifier_type}_lookup",
                expression=f"{identifier_match.binding.expression} = '{_escape_sql_literal(identifier_match.value)}'",
                source="identifier_lookup",
            )
        ],
        chosen_strategy="identifier_attribute_lookup",
        lookup_identifier=lookup_identifier,
        lookup_attributes=lookup_attributes,
    )


def build_record_lookup_plan(query_text: str) -> QueryPlanPayload | None:
    identifier_matches = _extract_explicit_identifier_matches(query_text)
    if not identifier_matches:
        identifier_matches = _extract_generic_identifier_matches(query_text)
    if not identifier_matches:
        return None

    selected_definition, selected_identifier = _resolve_record_lookup_candidate(
        query_text,
        identifier_matches,
    )
    if selected_definition is None or selected_identifier is None:
        return None

    lookup_identifier = LookupIdentifierPayload(
        identifier_type=selected_identifier.binding.identifier_type,
        label=selected_identifier.binding.label,
        table=selected_definition.table,
        column=selected_definition.identifier_column,
        expression=f"{selected_definition.table}.{selected_definition.identifier_column}",
        value=selected_identifier.value,
    )
    lookup_attributes = [
        ResolvedDimensionPayload(
            dimension_id=column.output_alias,
            name=column.name,
            expression=column.expression,
            output_alias=column.output_alias,
            table=column.table,
        )
        for column in selected_definition.columns
    ]

    return QueryPlanPayload(
        query_text=query_text,
        intent_type="record_lookup",
        summary=(
            f"Resolved {selected_definition.name} lookup for "
            f"{selected_identifier.binding.label} {selected_identifier.value}"
        ),
        clarification_required=False,
        clarification_reason=None,
        candidate_tables=[selected_definition.table],
        candidate_metrics=[],
        join_hints=[],
        selected_sql_asset_id=None,
        selected_sql=None,
        filters=[
            ResolvedFilterPayload(
                filter_id=f"{selected_identifier.binding.identifier_type}_record_lookup",
                expression=(
                    f"{selected_definition.table}.{selected_definition.identifier_column} = "
                    f"'{_escape_sql_literal(selected_identifier.value)}'"
                ),
                source="identifier_record_lookup",
            )
        ],
        chosen_strategy="identifier_record_lookup",
        lookup_identifier=lookup_identifier,
        lookup_attributes=lookup_attributes,
        lookup_record_label=selected_definition.name,
    )


def _clarification_payload(
    query_text: str,
    identifier_matches: list[IdentifierMatch],
    clarification_reason: str,
    clarification_questions: list[str],
) -> QueryPlanPayload:
    top_identifier = identifier_matches[0]
    return QueryPlanPayload(
        query_text=query_text,
        intent_type="attribute_lookup",
        summary=clarification_reason,
        clarification_required=True,
        clarification_reason=clarification_reason,
        clarification_questions=clarification_questions,
        candidate_tables=_ordered_candidate_tables(identifier_matches),
        candidate_metrics=[],
        join_hints=[],
        selected_sql_asset_id=None,
        selected_sql=None,
        lookup_identifier=LookupIdentifierPayload(
            identifier_type=top_identifier.binding.identifier_type,
            label=top_identifier.binding.label,
            table=top_identifier.binding.table,
            column=top_identifier.binding.column,
            expression=top_identifier.binding.expression,
            value=top_identifier.value,
        ),
    )


def _default_attributes(identifier_type: str) -> list[AttributeDefinition]:
    attribute_ids = DEFAULT_ATTRIBUTES_BY_IDENTIFIER.get(identifier_type, ())
    return [definition for definition in ATTRIBUTE_DEFINITIONS if definition.attribute_id in attribute_ids]


def _extract_requested_attributes(query_text: str) -> list[AttributeDefinition]:
    resolved: list[AttributeDefinition] = []
    for definition in ATTRIBUTE_DEFINITIONS:
        if any(alias in query_text for alias in definition.aliases):
            resolved.append(definition)
    return resolved


def _extract_identifier_matches(
    query_text: str,
    requested_attributes: list[AttributeDefinition],
) -> list[IdentifierMatch]:
    matches = _extract_explicit_identifier_matches(query_text)
    if matches:
        return matches
    if not requested_attributes:
        return []
    return _extract_generic_identifier_matches(query_text)


def _extract_explicit_identifier_matches(query_text: str) -> list[IdentifierMatch]:
    matches: list[IdentifierMatch] = []
    seen: set[tuple[str, str, str]] = set()
    for binding in IDENTIFIER_BINDINGS:
        for alias in binding.aliases:
            patterns = (
                rf"{re.escape(alias)}\s*(?:是|为|=|:|：)?\s*([A-Za-z0-9_-]+)",
                rf"{re.escape(alias)}([A-Za-z0-9_-]+)",
            )
            for pattern in patterns:
                match = re.search(pattern, query_text, flags=re.IGNORECASE)
                if match is None:
                    continue
                value = match.group(1)
                key = (binding.table, binding.column, value)
                if key in seen:
                    continue
                seen.add(key)
                matches.append(IdentifierMatch(binding=binding, value=value, matched_alias=alias))
    return sorted(matches, key=lambda item: item.binding.priority, reverse=True)


def _extract_generic_identifier_matches(query_text: str) -> list[IdentifierMatch]:
    patterns = (
        r"(?<![A-Za-z0-9_-])([A-Za-z0-9_-]{4,})(?=的)",
        r"(?<![A-Za-z0-9_-])([A-Za-z0-9_-]{4,})(?=是(?:什么|哪个|啥))",
    )
    value: str | None = None
    for pattern in patterns:
        match = re.search(pattern, query_text, flags=re.IGNORECASE)
        if match is not None:
            value = match.group(1)
            break
    if value is None:
        return []

    matches = [
        IdentifierMatch(
            binding=binding,
            value=value,
            matched_alias=None,
            generic=True,
        )
        for binding in IDENTIFIER_BINDINGS
    ]
    return sorted(matches, key=lambda item: item.binding.priority, reverse=True)


def _resolve_best_candidate(
    identifier_matches: list[IdentifierMatch],
    requested_attributes: list[AttributeDefinition],
) -> tuple[IdentifierMatch, list[AttributeBinding]] | None:
    best_match: tuple[int, int, IdentifierMatch, list[AttributeBinding]] | None = None
    for identifier_match in identifier_matches:
        attribute_bindings: list[AttributeBinding] = []
        score = identifier_match.binding.priority - (15 if identifier_match.generic else 0)
        complete = True
        for definition in requested_attributes:
            binding = definition.binding_for_table(identifier_match.binding.table)
            if binding is None:
                complete = False
                break
            attribute_bindings.append(binding)
            score += binding.priority
        if not complete:
            continue
        candidate = (len(attribute_bindings), score, identifier_match, attribute_bindings)
        if best_match is None or candidate[:2] > best_match[:2]:
            best_match = candidate
    if best_match is None:
        return None
    return best_match[2], best_match[3]


def _resolve_record_lookup_candidate(
    query_text: str,
    identifier_matches: list[IdentifierMatch],
) -> tuple[RecordDefinition | None, IdentifierMatch | None]:
    best_definition: RecordDefinition | None = None
    best_identifier: IdentifierMatch | None = None
    best_score: int | None = None
    for definition in RECORD_DEFINITIONS:
        if not any(alias in query_text for alias in definition.aliases):
            continue
        for identifier_match in identifier_matches:
            if identifier_match.binding.identifier_type != definition.identifier_type:
                continue
            if identifier_match.binding.table != definition.table:
                continue
            score = definition.priority + identifier_match.binding.priority
            if identifier_match.generic:
                score -= 10
            if best_score is None or score > best_score:
                best_definition = definition
                best_identifier = identifier_match
                best_score = score
    return best_definition, best_identifier


def _ordered_candidate_tables(identifier_matches: list[IdentifierMatch]) -> list[str]:
    ordered: list[str] = []
    for match in identifier_matches:
        if match.binding.table not in ordered:
            ordered.append(match.binding.table)
    return ordered


def _escape_sql_literal(value: str) -> str:
    return value.replace("\\", "\\\\").replace("'", "''")
