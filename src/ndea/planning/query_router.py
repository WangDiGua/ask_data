from __future__ import annotations

import re
from typing import Any

from ndea.planning.core_registry import (
    CoreFieldDefinition,
    CoreTableDefinition,
    field_by_id,
    get_core_table,
    iter_fields,
    join_rule,
    tables_for_query,
)
from ndea.planning.models import (
    JoinPlanStepPayload,
    QueryPlanPayload,
    ResolvedDimensionPayload,
    ResolvedFilterPayload,
    ResolvedMetricPayload,
    ResolvedTimeScopePayload,
)

GROUP_CUES = ("按", "按照", "分组", "维度", "分布", "构成", "排名")
ROSTER_CUES = ("名单", "列出", "有哪些", "哪些", "谁", "人员", "清单")
DETAIL_CUES = ("明细", "详情", "记录", "资料", "信息")
TREND_CUES = ("趋势", "变化", "近", "历年", "每年", "按年", "按月")
COUNT_CUES = ("多少", "人数", "数量", "总数", "统计")
ATTRIBUTE_CUES = ("姓名", "职称", "所在单位", "学院", "专业", "状态", "政治面貌", "性别", "信息")
IDENTIFIER_LOOKUP_CUES = ATTRIBUTE_CUES + ("出国", "出访", "来访专家") + ROSTER_CUES + DETAIL_CUES
TOP_N_PATTERN = re.compile(r"(?:前|top\s*)(\d+)", flags=re.IGNORECASE)
YEAR_PATTERN = re.compile(r"(20\d{2})年?")
IDENTIFIER_PATTERN = re.compile(
    r"(工号|教工号|职工号|学号|学工号)\s*(?:是|为|=|:|：)?\s*([A-Za-z0-9_-]+)"
)
BARE_IDENTIFIER_PATTERN = re.compile(r"(?<![A-Za-z0-9_-])([A-Za-z]{0,3}\d{4,}|[1-9]\d{4,})(?![A-Za-z0-9_-])")
REFERENCE_PATTERN = re.compile(r"^(他|她|ta|TA|这个老师|这个学生|这个人|此人)(的)?")
LEADING_QUERY_VERBS = re.compile(
    r"^(我要|我想|我想看|我想查|我想要|帮我查一下|帮我查|查一下|查查|看一下|看下|请查一下|请查|请帮我查一下|请帮我查)\s*"
)


def rewrite_query_text(
    query_text: str,
    request_context: dict[str, object] | None = None,
) -> str:
    rewritten = query_text.strip()
    if not rewritten:
        return rewritten

    if IDENTIFIER_PATTERN.search(rewritten):
        return rewritten

    if request_context:
        recent_messages = _recent_user_messages(request_context)
        previous_identifier = _previous_identifier(recent_messages[:-1] if recent_messages else [])
        if previous_identifier is not None:
            normalized = LEADING_QUERY_VERBS.sub("", rewritten).strip()
            if REFERENCE_PATTERN.match(normalized):
                suffix = REFERENCE_PATTERN.sub("", normalized).lstrip("的").strip()
                suffix = (
                    suffix.replace("出国的记录", "出国记录")
                    .replace("出访的记录", "出访记录")
                    .replace("出国的明细", "出国明细")
                    .replace("出访的明细", "出访明细")
                )
                if not suffix:
                    rewritten = f"{previous_identifier[0]}{previous_identifier[1]}"
                else:
                    rewritten = f"{previous_identifier[0]}{previous_identifier[1]}的{suffix}"
            elif any(
                token in normalized
                for token in ("出国记录", "出访记录", "出国明细", "出访明细", "职称", "姓名", "所在单位", "学院", "专业", "状态", "名单", "详情", "信息")
            ):
                rewritten = f"{previous_identifier[0]}{previous_identifier[1]}的{normalized.lstrip('的')}"

    return rewritten


def build_registry_metric_plan(
    query_text: str,
    request_context: dict[str, object] | None = None,
) -> QueryPlanPayload | None:
    rewritten = rewrite_query_text(query_text, request_context)
    table = _infer_metric_table(rewritten)
    if table is None:
        return None

    group_dimensions = _match_fields(rewritten, table, mode="aggregate")
    filters = list(_default_filters(table))
    filters.extend(_value_filters(rewritten, table, group_dimensions))
    filters = _dedupe_filters(filters)
    time_scope, time_dimensions = _time_resolution(rewritten, table)
    if time_dimensions:
        for field in time_dimensions:
            if field.field_id not in {item.field_id for item in group_dimensions}:
                group_dimensions.append(field)
    join_plan = _join_plan_for_fields(table, group_dimensions)
    candidate_tables = [table.table]
    candidate_tables.extend(
        step.right_table for step in join_plan if step.right_table and step.right_table not in candidate_tables
    )

    intent_type = "metric"
    lowered = rewritten.lower()
    if any(token in rewritten for token in TREND_CUES):
        intent_type = "trend"
    elif TOP_N_PATTERN.search(lowered) or "最多" in rewritten or "最高" in rewritten:
        intent_type = "ranking"
    elif len(group_dimensions) >= 2:
        intent_type = "comparison"

    needs_grouping = bool(group_dimensions) or any(token in rewritten for token in GROUP_CUES)
    if not needs_grouping and not any(token in rewritten for token in COUNT_CUES):
        return None

    result_limit = _result_limit(rewritten)
    return QueryPlanPayload(
        query_text=query_text,
        rewritten_query_text=rewritten,
        intent_type=intent_type,
        answer_mode="aggregate",
        summary=f"Resolved {table.label} aggregate query from core-table registry",
        clarification_required=False,
        clarification_reason=None,
        candidate_tables=candidate_tables,
        candidate_metrics=[table.label],
        join_hints=[step.join_id for step in join_plan],
        selected_sql_asset_id=None,
        selected_sql=None,
        metric_id=f"registry:{table.table}:count",
        dimensions=[_to_dimension_payload(field) for field in group_dimensions],
        filters=filters,
        time_scope=time_scope,
        time_grain=time_scope.scope_type if time_scope is not None else None,
        join_plan=join_plan,
        chosen_strategy="core_table_metric_registry",
        confidence=0.84 if group_dimensions else 0.78,
        resolved_metric=ResolvedMetricPayload(
            metric_id=f"registry:{table.table}:count",
            name=f"{table.label}数量",
            base_table=table.table,
            measure_expression="COUNT(*)",
            default_filters=[item.expression for item in _default_filters(table)],
            entity_scope=table.role,
        ),
        result_limit=result_limit if intent_type == "ranking" else None,
        sort_expressions=_sort_expressions(intent_type, group_dimensions),
        resolved_entities=[{"type": "table", "value": table.table, "label": table.label}],
    )


def build_identifier_clarification_plan(
    query_text: str,
    request_context: dict[str, object] | None = None,
) -> QueryPlanPayload | None:
    rewritten = rewrite_query_text(query_text, request_context)
    if IDENTIFIER_PATTERN.search(rewritten):
        return None
    if not any(token in rewritten for token in IDENTIFIER_LOOKUP_CUES):
        return None

    identifier_value = _bare_identifier_value(rewritten)
    if identifier_value is None:
        return None

    return QueryPlanPayload(
        query_text=query_text,
        rewritten_query_text=rewritten,
        intent_type="clarification",
        answer_mode="clarification",
        summary="Need identifier type before resolving numbered lookup",
        clarification_required=True,
        clarification_reason="Identifier value is present without a labeled identifier type",
        clarification_questions=[
            f"编号 {identifier_value} 可能是工号或学号，请先说明这是工号还是学号，再说明要查属性、记录还是名单。"
        ],
        chosen_strategy="identifier_clarification",
        confidence=0.93,
        resolved_entities=[{"type": "identifier_value", "value": identifier_value, "label": "编号"}],
    )


def build_roster_or_detail_plan(
    query_text: str,
    request_context: dict[str, object] | None = None,
) -> QueryPlanPayload | None:
    rewritten = rewrite_query_text(query_text, request_context)
    if not any(token in rewritten for token in ROSTER_CUES + DETAIL_CUES):
        return None
    if (
        any(token in rewritten for token in COUNT_CUES + GROUP_CUES + TREND_CUES)
        or TOP_N_PATTERN.search(rewritten.lower()) is not None
        or "最多" in rewritten
        or "最少" in rewritten
    ) and not any(token in rewritten for token in ("名单", "列出", "清单", "记录", "明细", "详情")):
        return None

    table = _infer_roster_table(rewritten)
    if table is None:
        return None

    requested_fields = _match_fields(rewritten, table, mode="roster")
    if not requested_fields:
        requested_fields = [
            field
            for field_id in table.default_projection
            if (field := field_by_id(table, field_id)) is not None
        ]

    filters = list(_default_filters(table))
    filters.extend(_value_filters(rewritten, table, requested_fields))
    filters = _dedupe_filters(filters)
    time_scope, _ = _time_resolution(rewritten, table)
    if time_scope is not None and time_scope.value is not None and table.time_field is not None:
        filters.append(
            ResolvedFilterPayload(
                filter_id="explicit_year",
                expression=f"{table.time_field} = '{time_scope.value}'",
                source="query_year",
            )
        )
        time_scope = None

    join_plan = _join_plan_for_fields(table, requested_fields)
    candidate_tables = [table.table]
    candidate_tables.extend(
        step.right_table for step in join_plan if step.right_table and step.right_table not in candidate_tables
    )
    answer_mode = "roster" if any(token in rewritten for token in ROSTER_CUES) else "detail"
    limit = _result_limit(rewritten) or 50

    return QueryPlanPayload(
        query_text=query_text,
        rewritten_query_text=rewritten,
        intent_type=answer_mode,
        answer_mode=answer_mode,
        summary=f"Resolved {table.label}{'名单' if answer_mode == 'roster' else '明细'} query from core-table registry",
        clarification_required=False,
        clarification_reason=None,
        candidate_tables=candidate_tables,
        candidate_metrics=[],
        join_hints=[step.join_id for step in join_plan],
        selected_sql_asset_id=None,
        selected_sql=None,
        filters=filters,
        time_scope=time_scope,
        lookup_attributes=[_to_dimension_payload(field) for field in requested_fields],
        lookup_record_label=f"{table.label}{'名单' if answer_mode == 'roster' else '明细'}",
        chosen_strategy="core_table_projection_registry",
        confidence=0.81 if filters else 0.68,
        join_plan=join_plan,
        result_limit=limit,
        sort_expressions=list(table.default_sort),
        resolved_entities=[{"type": "table", "value": table.table, "label": table.label}],
    )


def _recent_user_messages(request_context: dict[str, object]) -> list[str]:
    recent = request_context.get("recent_user_messages")
    if not isinstance(recent, list):
        return []
    return [str(item).strip() for item in recent if str(item).strip()]


def _previous_identifier(messages: list[str]) -> tuple[str, str] | None:
    for content in reversed(messages):
        match = IDENTIFIER_PATTERN.search(content)
        if match:
            return match.group(1), match.group(2)
    return None


def _bare_identifier_value(query_text: str) -> str | None:
    for match in BARE_IDENTIFIER_PATTERN.finditer(query_text):
        start, end = match.span(1)
        before = query_text[start - 1] if start > 0 else ""
        after = query_text[end] if end < len(query_text) else ""
        if before == "年" or after == "年":
            continue
        return match.group(1)
    return None


def _infer_metric_table(query_text: str) -> CoreTableDefinition | None:
    matched_tables = tables_for_query(query_text)
    if matched_tables:
        return matched_tables[0]

    if any(token in query_text for token in ("政治面貌", "学生类别", "培养层次", "专业")):
        return get_core_table("dcstu")
    if any(token in query_text for token in ("在岗", "教职工", "老师", "教师")):
        return get_core_table("dcemp")
    if any(token in query_text for token in ("来访专家", "外专", "讲座讲学", "来访目的")):
        return get_core_table("t_gjc_lfzj")
    if "出国" in query_text or "出访" in query_text:
        if "学生" in query_text:
            return get_core_table("t_bsdt_xsygcg")
        return get_core_table("t_bsdt_jzgygcg")
    if any(token in query_text for token in ("组织机构", "机构", "部门", "单位")) and any(
        cue in query_text for cue in COUNT_CUES + GROUP_CUES
    ):
        return get_core_table("dcorg")
    return None


def _infer_roster_table(query_text: str) -> CoreTableDefinition | None:
    matched_tables = tables_for_query(query_text)
    if matched_tables:
        return matched_tables[0]
    if "党员" in query_text or "团员" in query_text or "学生" in query_text:
        return get_core_table("dcstu")
    if "来访专家" in query_text or "外专" in query_text or "专家" in query_text:
        return get_core_table("t_gjc_lfzj")
    if "出国" in query_text or "出访" in query_text:
        if "学生" in query_text:
            return get_core_table("t_bsdt_xsygcg")
        return get_core_table("t_bsdt_jzgygcg")
    if "教师" in query_text or "老师" in query_text or "教职工" in query_text:
        return get_core_table("dcemp")
    return None


def _match_fields(
    query_text: str,
    table: CoreTableDefinition,
    mode: str,
) -> list[CoreFieldDefinition]:
    lowered = query_text.lower()
    matched: list[CoreFieldDefinition] = []
    for field in iter_fields(table):
        if mode not in field.modes and "aggregate" not in field.modes:
            continue
        if any(alias.lower() in lowered for alias in field.aliases):
            matched.append(field)
    return matched


def _default_filters(table: CoreTableDefinition) -> list[ResolvedFilterPayload]:
    return [
        ResolvedFilterPayload(
            filter_id=f"default_{index + 1}",
            expression=expression,
            source="core_table_default",
        )
        for index, expression in enumerate(table.default_filters)
    ]


def _value_filters(
    query_text: str,
    table: CoreTableDefinition,
    selected_fields: list[CoreFieldDefinition],
) -> list[ResolvedFilterPayload]:
    filters: list[ResolvedFilterPayload] = []
    lowered = query_text.lower()
    candidate_fields = {field.field_id: field for field in iter_fields(table)}
    for field in selected_fields:
        candidate_fields.setdefault(field.field_id, field)

    seen: set[str] = set()
    for field in candidate_fields.values():
        if "aggregate" not in field.modes and "roster" not in field.modes and "detail" not in field.modes:
            continue
        for sample in field.sample_values:
            if sample and sample.lower() in lowered:
                expression = f"{field.expression} = '{_escape_sql_literal(sample)}'"
                if expression not in seen:
                    filters.append(
                        ResolvedFilterPayload(
                            filter_id=f"value_{field.field_id}",
                            expression=expression,
                            source="core_table_value_match",
                        )
                    )
                    seen.add(expression)
        for canonical, aliases in field.value_aliases.items():
            if any(alias.lower() in lowered for alias in aliases):
                expression = f"{field.expression} = '{_escape_sql_literal(canonical)}'"
                if expression not in seen:
                    filters.append(
                        ResolvedFilterPayload(
                            filter_id=f"value_alias_{field.field_id}",
                            expression=expression,
                            source="core_table_value_alias",
                        )
                    )
                    seen.add(expression)
    return filters


def _dedupe_filters(filters: list[ResolvedFilterPayload]) -> list[ResolvedFilterPayload]:
    deduped: list[ResolvedFilterPayload] = []
    seen: set[str] = set()
    for item in filters:
        if item.expression in seen:
            continue
        seen.add(item.expression)
        deduped.append(item)
    return deduped


def _time_resolution(
    query_text: str,
    table: CoreTableDefinition,
) -> tuple[ResolvedTimeScopePayload | None, list[CoreFieldDefinition]]:
    year_match = YEAR_PATTERN.search(query_text)
    year_fields = [field for field in iter_fields(table) if field.field_id == "year"]
    if year_match and table.time_field is not None:
        payload = ResolvedTimeScopePayload(
            scope_type="year",
            field=table.time_field,
            value=year_match.group(1),
            label=f"{year_match.group(1)}年",
        )
        return payload, []
    if year_fields and any(token in query_text for token in TREND_CUES + ("按年", "每年", "历年")):
        return None, year_fields
    return None, []


def _join_plan_for_fields(
    table: CoreTableDefinition,
    fields: list[CoreFieldDefinition],
) -> list[JoinPlanStepPayload]:
    steps: list[JoinPlanStepPayload] = []
    seen: set[str] = set()
    for field in fields:
        if field.table == table.table:
            continue
        join = join_rule(table.table, field.table)
        if join is None or join[0] in seen:
            continue
        seen.add(join[0])
        steps.append(
            JoinPlanStepPayload(
                join_id=join[0],
                join_sql=join[1],
                left_table=table.table,
                right_table=join[2],
                join_type="INNER",
            )
        )
    return steps


def _sort_expressions(intent_type: str, dimensions: list[CoreFieldDefinition]) -> list[str]:
    if intent_type == "trend":
        if dimensions:
            return [f"{dimensions[0].expression} ASC"]
        return []
    if dimensions:
        return ["total DESC"]
    return []


def _result_limit(query_text: str) -> int | None:
    match = TOP_N_PATTERN.search(query_text.lower())
    if match is None:
        return None
    try:
        value = int(match.group(1))
    except ValueError:
        return None
    return max(1, min(100, value))


def _to_dimension_payload(field: CoreFieldDefinition) -> ResolvedDimensionPayload:
    return ResolvedDimensionPayload(
        dimension_id=field.field_id,
        name=field.label,
        expression=field.expression,
        output_alias=field.output_alias,
        table=field.table,
    )


def _escape_sql_literal(value: str) -> str:
    return value.replace("\\", "\\\\").replace("'", "''")
