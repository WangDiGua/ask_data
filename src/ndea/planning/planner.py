from collections.abc import Iterable
import re
from typing import Any

from ndea.planning.attribute_lookup import (
    build_attribute_lookup_plan,
    build_record_lookup_plan,
)
from ndea.planning.query_router import (
    build_identifier_clarification_plan,
    build_registry_metric_plan,
    build_roster_or_detail_plan,
    rewrite_query_text,
)
from ndea.planning.models import (
    JoinPlanStepPayload,
    QueryPlanPayload,
    RankedSQLCandidatePayload,
    ResolvedDimensionPayload,
    ResolvedFilterPayload,
    ResolvedMetricPayload,
    ResolvedTimeScopePayload,
)
from ndea.semantic import DimensionContract, JoinPathContract, MetricContract, TimeSemantics


class QueryPlannerService:
    def __init__(
        self,
        vector_locator: Any,
        sql_rag: Any,
        sql_selection_threshold: float = 0.85,
    ) -> None:
        self._vector_locator = vector_locator
        self._sql_rag = sql_rag
        self._sql_selection_threshold = sql_selection_threshold

    def plan(
        self,
        query_text: str,
        query_vector: list[float],
        request_context: dict[str, object] | None = None,
    ) -> QueryPlanPayload:
        planning_context = self._read_mapping(request_context or {}, "planning_context")
        normalized_query_text = rewrite_query_text(query_text, request_context)
        identifier_clarification_plan = build_identifier_clarification_plan(
            query_text=query_text,
            request_context=request_context,
        )
        if identifier_clarification_plan is not None:
            return identifier_clarification_plan
        record_lookup_plan = build_record_lookup_plan(normalized_query_text)
        if record_lookup_plan is not None:
            record_lookup_plan.query_text = query_text
            record_lookup_plan.rewritten_query_text = normalized_query_text
            record_lookup_plan.answer_mode = "record_lookup"
            return record_lookup_plan
        attribute_lookup_plan = build_attribute_lookup_plan(normalized_query_text)
        if attribute_lookup_plan is not None:
            attribute_lookup_plan.query_text = query_text
            attribute_lookup_plan.rewritten_query_text = normalized_query_text
            attribute_lookup_plan.answer_mode = (
                "clarification"
                if attribute_lookup_plan.clarification_required
                else "attribute_lookup"
            )
            return attribute_lookup_plan
        roster_or_detail_plan = build_roster_or_detail_plan(
            query_text=query_text,
            request_context=request_context,
        )
        if roster_or_detail_plan is not None:
            return roster_or_detail_plan
        registry_metric_plan = build_registry_metric_plan(
            query_text=query_text,
            request_context=request_context,
        )
        if registry_metric_plan is not None:
            return registry_metric_plan
        degraded = False
        degradation_reasons: list[str] = []
        try:
            vector_payload = self._vector_locator.locate(
                query_text=normalized_query_text,
                query_vector=query_vector,
                asset_types=[
                    "metric",
                    "schema",
                    "join_path",
                    "metric_contract",
                    "dimension_contract",
                    "time_semantics",
                ],
                limit=50,
            )
        except Exception as exc:
            degraded = True
            degradation_reasons.append(str(exc) or "vector retrieval unavailable")
            vector_payload = {"matches": []}

        try:
            sql_payload = self._sql_rag.retrieve(
                query_text=normalized_query_text,
                query_vector=query_vector,
                limit=20,
            )
        except Exception as exc:
            degraded = True
            degradation_reasons.append(str(exc) or "sql rag unavailable")
            sql_payload = {"candidates": []}

        matches = list(self._read_items(vector_payload, "matches"))
        candidates = list(self._read_items(sql_payload, "candidates"))
        metric_contracts = self._read_contracts(
            vector_payload,
            "metric_contracts",
            "metric_contract",
            MetricContract,
        )
        dimension_contracts = self._read_contracts(
            vector_payload,
            "dimension_contracts",
            "dimension_contract",
            DimensionContract,
        )
        join_path_contracts = self._read_contracts(
            vector_payload,
            "join_path_contracts",
            "join_path",
            JoinPathContract,
        )
        time_semantics_catalog = self._read_contracts(
            vector_payload,
            "time_semantics_catalog",
            "time_semantics",
            TimeSemantics,
        )
        candidate_tables = self._collect_candidate_tables(matches, candidates)
        candidate_metrics = self._collect_candidate_metrics(matches)
        join_hints = self._collect_join_hints(matches)
        intent_type = self._classify_intent(normalized_query_text)

        metric_contract, confidence = self._resolve_metric_contract(normalized_query_text, metric_contracts)
        dimensions: list[ResolvedDimensionPayload] = []
        filters: list[ResolvedFilterPayload] = []
        time_scope: ResolvedTimeScopePayload | None = None
        entity_scope = self._read_text(planning_context, "entity_scope") or None
        join_plan: list[JoinPlanStepPayload] = []
        clarification_questions: list[str] = []
        clarification_reason: str | None = None
        chosen_strategy: str | None = None
        resolved_metric: ResolvedMetricPayload | None = None

        top_candidate = candidates[0] if candidates else None
        selected_sql_asset_id: str | None = None
        selected_sql: str | None = None
        selected_candidate_reason: str | None = None

        clarification_required = False
        ranked_sql_candidates: list[RankedSQLCandidatePayload] = []
        if metric_contract is not None:
            chosen_strategy = "metric_contract"
            if entity_scope is None:
                entity_scope = metric_contract.entity_scope
                if entity_scope is None and not metric_contract.requires_entity_scope:
                    entity_scope = metric_contract.base_table
            resolved_metric = ResolvedMetricPayload(
                metric_id=metric_contract.metric_id,
                name=metric_contract.name,
                base_table=metric_contract.base_table,
                measure_expression=metric_contract.measure_expression,
                default_filters=list(metric_contract.default_filters),
                entity_scope=entity_scope,
            )
            candidate_metrics = self._unique_strings([metric_contract.name] + candidate_metrics)
            dimensions = self._resolve_dimensions(normalized_query_text, dimension_contracts, metric_contract, planning_context)
            filters = [
                ResolvedFilterPayload(
                    filter_id=f"default_filter_{index + 1}",
                    expression=expression,
                    source="metric_default",
                )
                for index, expression in enumerate(metric_contract.default_filters)
            ]
            dimension_value_filters, filter_tables = self._resolve_dimension_value_filters(
                query_text=normalized_query_text,
                contracts=dimension_contracts,
                metric_contract=metric_contract,
                planning_context=planning_context,
            )
            filters.extend(dimension_value_filters)
            time_scope = self._resolve_time_scope(
                query_text=normalized_query_text,
                planning_context=planning_context,
                metric_contract=metric_contract,
                time_semantics_catalog=time_semantics_catalog,
            )
            join_plan = self._resolve_join_plan(
                metric_contract=metric_contract,
                dimensions=dimensions,
                join_path_contracts=join_path_contracts,
                extra_tables=filter_tables,
            )
            candidate_tables = self._collect_contract_tables(
                metric_contract=metric_contract,
                dimensions=dimensions,
                join_plan=join_plan,
                existing=candidate_tables,
                extra_tables=filter_tables,
            )
            has_specific_filters = any(
                filter_payload.source not in {None, "", "metric_default"}
                for filter_payload in filters
            ) or (
                time_scope is not None
                and (
                    time_scope.value is not None
                    or (time_scope.start is not None and time_scope.end is not None)
                )
            )
            ranked_sql_candidates = self._rank_sql_candidates(
                candidates=candidates,
                metric_contract=metric_contract,
                dimensions=dimensions,
                candidate_tables=candidate_tables,
                time_scope=time_scope,
                entity_scope=entity_scope,
                has_specific_filters=has_specific_filters,
            )
            top_ranked_candidate = ranked_sql_candidates[0] if ranked_sql_candidates else None
            if (
                not has_specific_filters
                and top_ranked_candidate is not None
                and top_ranked_candidate.compatibility_score >= self._sql_selection_threshold
            ):
                selected_sql_asset_id = top_ranked_candidate.asset_id
                selected_sql = top_ranked_candidate.sql
                selected_candidate_reason = top_ranked_candidate.selection_reason
            clarification_questions, clarification_reason = self._clarification_for_contract(
                metric_contract=metric_contract,
                entity_scope=entity_scope,
                time_scope=time_scope,
            )
            clarification_required = bool(clarification_questions)
        elif not candidate_tables and not candidate_metrics and top_candidate is None:
            clarification_required = True
            clarification_reason = "Need more semantic grounding before planning SQL"
            clarification_questions = ["请补充你要统计的对象、时间范围或业务口径。"]
        else:
            ranked_sql_candidates = self._rank_unstructured_sql_candidates(candidates)
            top_ranked_candidate = ranked_sql_candidates[0] if ranked_sql_candidates else None
            if (
                top_ranked_candidate is not None
                and top_ranked_candidate.compatibility_score >= self._sql_selection_threshold
            ):
                selected_sql_asset_id = top_ranked_candidate.asset_id
                selected_sql = top_ranked_candidate.sql
                selected_candidate_reason = top_ranked_candidate.selection_reason

        summary = clarification_reason or (
            f"Identified {intent_type} query with {len(candidate_tables)} candidate tables "
            f"and {len(candidates)} SQL candidates"
        )
        if degraded:
            prefix = "Planner degraded to deterministic fallback"
            if degradation_reasons:
                prefix = f"{prefix}: {'; '.join(degradation_reasons)}"
            summary = f"{prefix}. {summary}"

        return QueryPlanPayload(
            query_text=query_text,
            rewritten_query_text=normalized_query_text,
            intent_type=intent_type,
            answer_mode="clarification" if clarification_required else "aggregate",
            summary=summary,
            degraded=degraded,
            error_code=(
                "planner_degraded"
                if degraded
                else "clarification_required" if clarification_required and clarification_reason else None
            ),
            clarification_required=clarification_required,
            clarification_reason=clarification_reason,
            clarification_questions=clarification_questions,
            candidate_tables=candidate_tables,
            candidate_metrics=candidate_metrics,
            join_hints=join_hints,
            selected_sql_asset_id=selected_sql_asset_id,
            selected_sql=selected_sql,
            selected_candidate_reason=selected_candidate_reason,
            metric_id=metric_contract.metric_id if metric_contract is not None else None,
            dimensions=dimensions,
            filters=filters,
            time_scope=time_scope,
            time_grain=time_scope.scope_type if time_scope is not None else None,
            entity_scope=entity_scope,
            join_plan=join_plan,
            chosen_strategy=chosen_strategy,
            confidence=confidence,
            resolved_metric=resolved_metric,
            ranked_sql_candidates=ranked_sql_candidates,
        )

    def _classify_intent(self, query_text: str) -> str:
        text = query_text.lower()
        if any(keyword in text for keyword in ("compare", "comparison", "versus", " vs ", "同比", "环比")):
            return "comparison"
        if any(keyword in text for keyword in ("top ", "rank", "highest", "lowest", "most ", "least ")):
            return "ranking"
        if any(
            keyword in text
            for keyword in ("trend", "over time", "by month", "by year", "monthly", "yearly", "daily", "weekly", "趋势")
        ):
            return "trend"
        if any(keyword in text for keyword in ("list", "detail", "details", "show", "which", "who", "明细", "名单")):
            return "detail"
        return "metric"

    def _collect_candidate_tables(
        self,
        matches: list[Any],
        candidates: list[Any],
    ) -> list[str]:
        tables: list[str] = []
        for match in matches:
            if self._read_text(match, "asset_type") != "schema":
                continue
            metadata = self._read_mapping(match, "metadata")
            table_name = metadata.get("table_name")
            if isinstance(table_name, str) and table_name:
                tables.append(table_name)
                continue
            title = self._read_text(match, "title")
            normalized = title.removesuffix(" table").strip()
            if normalized:
                tables.append(normalized)

        for candidate in candidates:
            candidate_tables = self._read_items(candidate, "tables")
            for table in candidate_tables:
                if isinstance(table, str) and table:
                    tables.append(table)

        return self._unique_strings(tables)

    def _collect_candidate_metrics(self, matches: list[Any]) -> list[str]:
        metrics = [
            self._read_text(match, "title")
            for match in matches
            if self._read_text(match, "asset_type") == "metric" and self._read_text(match, "title")
        ]
        return self._unique_strings(metrics)

    def _collect_join_hints(self, matches: list[Any]) -> list[str]:
        hints = [
            self._read_text(match, "title")
            for match in matches
            if self._read_text(match, "asset_type") == "join_path" and self._read_text(match, "title")
        ]
        return self._unique_strings(hints)

    def _read_items(self, payload: Any, field: str) -> list[Any]:
        if isinstance(payload, dict):
            value = payload.get(field)
        else:
            value = getattr(payload, field, None)
        if isinstance(value, list):
            return value
        return []

    def _read_mapping(self, payload: Any, field: str) -> dict[str, Any]:
        if isinstance(payload, dict):
            value = payload.get(field)
        else:
            value = getattr(payload, field, None)
        if isinstance(value, dict):
            return value
        return {}

    def _read_text(self, payload: Any, field: str) -> str:
        if isinstance(payload, dict):
            value = payload.get(field)
        else:
            value = getattr(payload, field, None)
        if isinstance(value, str):
            return value
        return ""

    def _read_score(self, payload: Any) -> float:
        if isinstance(payload, dict):
            value = payload.get("hybrid_score", payload.get("score", 0.0))
        else:
            value = getattr(payload, "hybrid_score", getattr(payload, "score", 0.0))
        try:
            return float(value)
        except (TypeError, ValueError):
            return 0.0

    def _unique_strings(self, values: Iterable[str]) -> list[str]:
        seen: set[str] = set()
        ordered: list[str] = []
        for value in values:
            if not value or value in seen:
                continue
            seen.add(value)
            ordered.append(value)
        return ordered

    def _read_contracts(
        self,
        payload: Any,
        field: str,
        fallback_asset_type: str,
        model_type,
    ) -> list[Any]:
        contracts: list[Any] = []
        direct_items = self._read_items(payload, field)
        for item in direct_items:
            if isinstance(item, dict):
                contracts.append(model_type.model_validate(item))
            elif isinstance(item, model_type):
                contracts.append(item)

        if contracts:
            return contracts

        singular_field = field.removesuffix("s")
        for match in self._read_items(payload, "matches"):
            if self._read_text(match, "asset_type") != fallback_asset_type:
                continue
            metadata = self._read_mapping(match, "metadata")
            contract_payload = metadata.get(singular_field)
            if isinstance(contract_payload, dict):
                contracts.append(model_type.model_validate(contract_payload))
        return contracts

    def _resolve_metric_contract(
        self,
        query_text: str,
        contracts: list[MetricContract],
    ) -> tuple[MetricContract | None, float | None]:
        lowered = query_text.lower()
        best_contract: MetricContract | None = None
        best_score = 0.0
        for contract in contracts:
            aliases = [contract.name, *contract.aliases]
            score = float(contract.score or 0.0)
            matched = False
            for alias in aliases:
                normalized_alias = alias.strip().lower()
                if normalized_alias and normalized_alias in lowered:
                    score = max(score, 0.9 + len(normalized_alias) / 1000.0)
                    matched = True
            if matched or not aliases:
                if best_contract is None or score > best_score:
                    best_contract = contract
                    best_score = score
        if best_contract is None and contracts:
            best_contract = contracts[0]
            best_score = float(contracts[0].score or 0.0)
        return best_contract, round(best_score, 2) if best_contract is not None else None

    def _resolve_dimensions(
        self,
        query_text: str,
        contracts: list[DimensionContract],
        metric_contract: MetricContract,
        planning_context: dict[str, Any],
    ) -> list[ResolvedDimensionPayload]:
        requested = self._read_items(planning_context, "dimensions")
        requested_ids = {str(item) for item in requested if isinstance(item, str)}
        lowered = query_text.lower()
        has_group_cue = self._has_group_cue(query_text)
        resolved: list[ResolvedDimensionPayload] = []
        for contract in contracts:
            if metric_contract.available_dimensions and contract.dimension_id not in metric_contract.available_dimensions:
                continue
            aliases = [contract.name, *contract.aliases]
            if contract.dimension_id in requested_ids or (
                has_group_cue and any(alias.lower() in lowered for alias in aliases if alias)
            ):
                resolved.append(
                    ResolvedDimensionPayload(
                        dimension_id=contract.dimension_id,
                        name=contract.name,
                        expression=contract.expression,
                        output_alias=contract.output_alias,
                        table=contract.table,
                    )
                )
        return resolved

    def _resolve_dimension_value_filters(
        self,
        query_text: str,
        contracts: list[DimensionContract],
        metric_contract: MetricContract,
        planning_context: dict[str, Any],
    ) -> tuple[list[ResolvedFilterPayload], list[str]]:
        query_tokens = self._normalize_text(query_text)
        filters: list[ResolvedFilterPayload] = []
        filter_tables: list[str] = []
        requested_filters = self._read_items(planning_context, "filters")

        for index, raw_filter in enumerate(requested_filters, start=1):
            if isinstance(raw_filter, str) and raw_filter.strip():
                filters.append(
                    ResolvedFilterPayload(
                        filter_id=f"context_filter_{index}",
                        expression=raw_filter.strip(),
                        source="planning_context",
                    )
                )

        seen_dimensions: set[str] = set()
        for contract in contracts:
            if metric_contract.available_dimensions and contract.dimension_id not in metric_contract.available_dimensions:
                continue
            if contract.dimension_id in seen_dimensions:
                continue
            matched_value: str | None = None
            for sample_value in sorted(contract.sample_values, key=len, reverse=True):
                normalized_value = self._normalize_text(sample_value)
                if len(normalized_value) < 2:
                    continue
                if normalized_value in query_tokens:
                    matched_value = sample_value
                    break
            if matched_value is None:
                continue
            filters.append(
                ResolvedFilterPayload(
                    filter_id=f"dimension_value_{contract.dimension_id}",
                    expression=f"{contract.expression} = '{self._escape_sql_literal(matched_value)}'",
                    source="dimension_value_match",
                )
            )
            seen_dimensions.add(contract.dimension_id)
            if contract.table and contract.table != metric_contract.base_table:
                filter_tables.append(contract.table)

        return filters, self._unique_strings(filter_tables)

    def _resolve_time_scope(
        self,
        query_text: str,
        planning_context: dict[str, Any],
        metric_contract: MetricContract,
        time_semantics_catalog: list[TimeSemantics],
    ) -> ResolvedTimeScopePayload | None:
        context_time_scope = planning_context.get("time_scope")
        if isinstance(context_time_scope, dict):
            return ResolvedTimeScopePayload.model_validate(context_time_scope)

        if not metric_contract.time_field:
            return None

        academic_year_match = re.search(r"(20\d{2})学年", query_text)
        if academic_year_match:
            return ResolvedTimeScopePayload(
                scope_type="academic_year",
                field=metric_contract.time_field,
                value=academic_year_match.group(1),
                label=f"{academic_year_match.group(1)}学年",
            )

        year_match = re.search(r"(20\d{2})年(?:度)?", query_text)
        if year_match:
            return ResolvedTimeScopePayload(
                scope_type="year",
                field=metric_contract.time_field,
                value=year_match.group(1),
                label=f"{year_match.group(1)}年",
            )

        for semantics in time_semantics_catalog:
            for alias in [semantics.name, *semantics.aliases]:
                if alias and alias in query_text:
                    return ResolvedTimeScopePayload(
                        scope_type=semantics.default_grain or "time",
                        field=metric_contract.time_field,
                        label=alias,
                    )
        return None

    def _resolve_join_plan(
        self,
        metric_contract: MetricContract,
        dimensions: list[ResolvedDimensionPayload],
        join_path_contracts: list[JoinPathContract],
        extra_tables: list[str] | None = None,
    ) -> list[JoinPlanStepPayload]:
        needed_tables = {
            dimension.table
            for dimension in dimensions
            if dimension.table and dimension.table != metric_contract.base_table
        }
        needed_tables.update(
            table
            for table in (extra_tables or [])
            if table and table != metric_contract.base_table
        )
        resolved: list[JoinPlanStepPayload] = []
        for contract in join_path_contracts:
            if contract.disabled or contract.join_id not in metric_contract.join_path_ids:
                continue
            if contract.right_table in needed_tables or contract.left_table in needed_tables:
                resolved.append(
                    JoinPlanStepPayload(
                        join_id=contract.join_id,
                        join_sql=contract.join_sql,
                        left_table=contract.left_table,
                        right_table=contract.right_table,
                        join_type=contract.join_type,
                    )
                )
        return resolved

    def _collect_contract_tables(
        self,
        metric_contract: MetricContract,
        dimensions: list[ResolvedDimensionPayload],
        join_plan: list[JoinPlanStepPayload],
        existing: list[str],
        extra_tables: list[str] | None = None,
    ) -> list[str]:
        tables = [metric_contract.base_table]
        tables.extend(
            dimension.table
            for dimension in dimensions
            if dimension.table and dimension.table != metric_contract.base_table
        )
        tables.extend(
            step.right_table
            for step in join_plan
            if step.right_table and step.right_table != metric_contract.base_table
        )
        tables.extend(
            table
            for table in (extra_tables or [])
            if table and table != metric_contract.base_table
        )
        tables.extend(existing)
        return self._unique_strings([table for table in tables if table])

    def _clarification_for_contract(
        self,
        metric_contract: MetricContract,
        entity_scope: str | None,
        time_scope: ResolvedTimeScopePayload | None,
    ) -> tuple[list[str], str | None]:
        questions: list[str] = []
        reason: str | None = None
        if metric_contract.requires_entity_scope and not entity_scope:
            options = metric_contract.entity_scope_options or ["student", "faculty", "all_people"]
            localized = [self._localize_scope(option) for option in options]
            if len(localized) >= 3:
                prompt = f"你想查询{localized[0]}、{localized[1]}，还是{localized[2]}？"
            elif len(localized) == 2:
                prompt = f"你想查询{localized[0]}还是{localized[1]}？"
            else:
                prompt = f"你想查询{localized[0]}吗？"
            questions.append(prompt)
            reason = "Need entity scope before planning SQL"
        if metric_contract.requires_time_scope and time_scope is None:
            questions.append("请明确你要统计的时间范围。")
            reason = reason or "Need time scope before planning SQL"
        return questions, reason

    def _localize_scope(self, scope: str) -> str:
        mapping = {
            "student": "学生",
            "faculty": "教职工",
            "all_people": "全体在册人员",
            "organization": "组织机构",
            "expert": "来访专家",
        }
        return mapping.get(scope, scope)

    def _rank_sql_candidates(
        self,
        candidates: list[Any],
        metric_contract: MetricContract,
        dimensions: list[ResolvedDimensionPayload],
        candidate_tables: list[str],
        time_scope: ResolvedTimeScopePayload | None,
        entity_scope: str | None,
        has_specific_filters: bool,
    ) -> list[RankedSQLCandidatePayload]:
        ranked: list[RankedSQLCandidatePayload] = []
        requested_dimensions = {dimension.dimension_id for dimension in dimensions}
        requested_tables = {table for table in candidate_tables if table}
        requested_time_grain = time_scope.scope_type if time_scope is not None else None
        for candidate in candidates:
            metadata = self._read_mapping(candidate, "metadata")
            candidate_metric = self._read_text(metadata, "metric_id")
            candidate_dimensions = {
                str(item)
                for item in self._read_items(metadata, "dimensions")
                if isinstance(item, str)
            }
            candidate_time_grains = {
                str(item)
                for item in self._read_items(metadata, "time_grains")
                if isinstance(item, str)
            }
            candidate_entity_scope = self._read_text(metadata, "entity_scope") or None
            candidate_specific_filters = {
                str(item)
                for item in self._read_items(metadata, "specific_filters")
                if isinstance(item, str)
            }
            tables = {
                str(item)
                for item in self._read_items(candidate, "tables")
                if isinstance(item, str)
            }
            base_score = self._read_score(candidate)
            compatibility_score = base_score
            selection_reason = "hybrid_score"
            if candidate_metric == metric_contract.metric_id:
                compatibility_score += 0.2
                selection_reason = "compatible_metric"
            if requested_dimensions and requested_dimensions <= candidate_dimensions:
                compatibility_score += 0.2
                selection_reason = "compatible_metric_dimension"
            elif not requested_dimensions:
                if candidate_dimensions:
                    compatibility_score -= 0.35
                else:
                    compatibility_score += 0.15
            if requested_tables and requested_tables <= tables:
                compatibility_score += 0.15
            if requested_time_grain and requested_time_grain in candidate_time_grains:
                compatibility_score += 0.1
                if selection_reason == "compatible_metric_dimension":
                    selection_reason = "compatible_metric_dimension_time"
            if entity_scope and candidate_entity_scope and entity_scope == candidate_entity_scope:
                compatibility_score += 0.05
            if candidate_specific_filters and not has_specific_filters:
                compatibility_score -= 0.3
            ranked.append(
                RankedSQLCandidatePayload(
                    asset_id=self._read_text(candidate, "asset_id"),
                    sql=self._read_text(candidate, "sql"),
                    compatibility_score=round(min(1.5, compatibility_score), 2),
                    selection_reason=selection_reason,
                    score=self._read_numeric(candidate, "score"),
                    hybrid_score=self._read_numeric(candidate, "hybrid_score"),
                )
            )
        return sorted(ranked, key=lambda item: item.compatibility_score, reverse=True)

    def _rank_unstructured_sql_candidates(
        self,
        candidates: list[Any],
    ) -> list[RankedSQLCandidatePayload]:
        ranked: list[RankedSQLCandidatePayload] = []
        for candidate in candidates:
            base_score = self._read_score(candidate)
            ranked.append(
                RankedSQLCandidatePayload(
                    asset_id=self._read_text(candidate, "asset_id"),
                    sql=self._read_text(candidate, "sql"),
                    compatibility_score=round(base_score, 2),
                    selection_reason="hybrid_score",
                    score=self._read_numeric(candidate, "score"),
                    hybrid_score=self._read_numeric(candidate, "hybrid_score"),
                )
            )
        return sorted(ranked, key=lambda item: item.compatibility_score, reverse=True)

    def _read_numeric(self, payload: Any, field: str) -> float | None:
        if isinstance(payload, dict):
            value = payload.get(field)
        else:
            value = getattr(payload, field, None)
        if value in (None, ""):
            return None
        try:
            return float(value)
        except (TypeError, ValueError):
            return None

    def _normalize_text(self, value: str) -> str:
        return re.sub(r"[\s,，。、“”\"'‘’（）()\-_/]+", "", value.strip().lower())

    def _escape_sql_literal(self, value: str) -> str:
        return value.replace("\\", "\\\\").replace("'", "''")

    def _has_group_cue(self, query_text: str) -> bool:
        return any(keyword in query_text for keyword in ("按", "分", "各", "每", "维度", "排名", "TOP", "top"))
