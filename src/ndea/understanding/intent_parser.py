from __future__ import annotations

import re

from ndea.query_v2 import QueryIR


YEAR_PATTERN = re.compile(r"(20\d{2})")
TOP_PATTERN = re.compile(r"(?:top|前)\s*(\d+)", flags=re.IGNORECASE)
IDENTIFIER_PATTERN = re.compile(r"(工号|教工号|职工号|学号)\s*(?:是|为|=|:)?\s*([A-Za-z0-9_-]+)")


class IntentParser:
    def parse(self, query_text: str) -> QueryIR:
        text = query_text.strip()
        lowered = text.lower()
        intent_type = "metric"
        answer_mode = "aggregate"
        metric = None
        dimensions: list[str] = []
        filters: list[str] = []
        ambiguities: list[str] = []
        campus_terms: list[str] = []
        entity_scope = None
        sort: list[str] = []
        limit = None
        identifiers = self._extract_identifiers(text)

        if any(token in text for token in ("名单", "列出", "哪些", "记录", "明细", "详情")):
            intent_type = "detail"
            answer_mode = "detail"
        if any(token in text for token in ("趋势", "近三年", "近五年", "按年", "每年", "按月")):
            intent_type = "trend"
        if any(token in text for token in ("排名", "top", "前十", "前10", "最多", "最高")):
            intent_type = "ranking"
            sort = ["total DESC"]

        top_match = TOP_PATTERN.search(lowered)
        if top_match is not None:
            limit = max(1, min(100, int(top_match.group(1))))

        if any(token in text for token in ("学生", "在校生", "学工")):
            entity_scope = "student"
            campus_terms.append("student")
        elif any(token in text for token in ("教师", "教职工", "老师", "在岗")):
            entity_scope = "faculty"
            campus_terms.append("faculty")
        elif any(token in text for token in ("机构", "部门", "单位", "组织")):
            entity_scope = "organization"
            campus_terms.append("organization")

        if any(identifier["type"] in {"工号", "教工号", "职工号"} for identifier in identifiers):
            entity_scope = entity_scope or "faculty"
            if "faculty" not in campus_terms:
                campus_terms.append("faculty")
        if any(identifier["type"] == "学号" for identifier in identifiers):
            entity_scope = entity_scope or "student"
            if "student" not in campus_terms:
                campus_terms.append("student")

        if any(token in text for token in ("人数", "多少人", "总数", "数量", "统计")):
            metric = "count"
        if any(token in text for token in ("学院", "院系")):
            dimensions.append("college")
            campus_terms.append("college")
        if any(token in text for token in ("部门", "单位")):
            dimensions.append("organization")
            campus_terms.append("organization_dimension")
        if "学年" in text:
            campus_terms.append("academic_year")
        if "学期" in text:
            campus_terms.append("semester")
        if "在校" in text:
            filters.append("在校")
            campus_terms.append("active_student")
        if "在岗" in text:
            filters.append("在岗")
            campus_terms.append("active_faculty")

        if any(token in text for token in ("出访", "出国", "因公出国")):
            intent_type = "detail"
            answer_mode = "detail"
            metric = None
            if entity_scope == "student":
                campus_terms.append("student_outbound")
            else:
                entity_scope = entity_scope or "faculty"
                if "faculty" not in campus_terms:
                    campus_terms.append("faculty")
                campus_terms.append("teacher_outbound")

        if any(token in text for token in ("来访专家", "来访记录", "邀请来访")):
            intent_type = "detail"
            answer_mode = "detail"
            metric = None
            campus_terms.append("visiting_expert")

        if any(token in text for token in ("我们学校有多少人", "学校有多少人", "全校多少人")):
            metric = "campus_population"
            ambiguities.append("entity_scope_required")
            if entity_scope is None:
                answer_mode = "clarification"

        time_scope = None
        year_match = YEAR_PATTERN.search(text)
        if year_match:
            year_value = year_match.group(1)
            scope_type = "academic_year" if "学年" in text else "year"
            time_scope = {
                "scope_type": scope_type,
                "field": None,
                "value": year_value,
                "label": f"{year_value}{'学年' if scope_type == 'academic_year' else '年'}",
            }

        confidence = 0.45
        if metric is not None:
            confidence += 0.15
        if entity_scope is not None:
            confidence += 0.15
        if identifiers:
            confidence += 0.15
        if dimensions:
            confidence += 0.1
        if time_scope is not None:
            confidence += 0.1
        if any(term in campus_terms for term in ("teacher_outbound", "student_outbound", "visiting_expert")):
            confidence += 0.1
        if ambiguities:
            confidence -= 0.15

        return QueryIR(
            intent_type=intent_type,
            entity_scope=entity_scope,
            metric=metric,
            dimensions=dimensions,
            filters=filters,
            identifiers=identifiers,
            time_scope=time_scope,
            sort=sort,
            limit=limit,
            answer_mode=answer_mode,
            ambiguities=ambiguities,
            campus_terms=list(dict.fromkeys(campus_terms)),
            confidence=round(max(0.1, min(0.95, confidence)), 2),
        )

    def _extract_identifiers(self, text: str) -> list[dict[str, str]]:
        identifiers: list[dict[str, str]] = []
        for match in IDENTIFIER_PATTERN.finditer(text):
            identifiers.append({"type": match.group(1), "value": match.group(2)})
        return identifiers
