from __future__ import annotations

import re
from typing import Any

from ndea.query_v2 import InteractionResult


REFERENCE_PATTERN = re.compile(r"^(这个老师|这个学生|这个人|刚才那个|按刚才那个|这个)(的)?")
IDENTIFIER_PATTERN = re.compile(r"(工号|教工号|职工号|学号)\s*(?:是|为|=|:)?\s*([A-Za-z0-9_-]+)")
LEADING_FILLERS = re.compile(r"^(我想|我要|帮我|请|麻烦|看一下|查一下|查一查)\s*")


class InteractionService:
    def process(
        self,
        query_text: str,
        request_context: dict[str, Any] | None = None,
    ) -> InteractionResult:
        raw = query_text.strip()
        normalized = self._normalize(raw)
        recent_messages = self._recent_user_messages(request_context or {})
        rewritten = raw
        references_resolved = False
        notes: list[str] = []

        if raw and not IDENTIFIER_PATTERN.search(raw) and recent_messages:
            previous_identifier = self._previous_identifier(recent_messages[:-1] if recent_messages else [])
            if previous_identifier is not None:
                candidate = LEADING_FILLERS.sub("", raw).strip()
                if REFERENCE_PATTERN.match(candidate):
                    suffix = REFERENCE_PATTERN.sub("", candidate).strip()
                    suffix = suffix.lstrip("的").strip()
                    suffix = self._normalize_suffix(suffix)
                    rewritten = f"{previous_identifier[0]}{previous_identifier[1]} {suffix}".strip()
                    references_resolved = True
                    notes.append("Resolved reference using recent identifier context")

        return InteractionResult(
            query_text=raw,
            normalized_query_text=normalized,
            rewritten_query_text=rewritten,
            recent_user_messages=recent_messages,
            context_summary=self._context_summary(recent_messages),
            references_resolved=references_resolved,
            notes=notes,
        )

    def _normalize(self, query_text: str) -> str:
        return " ".join(query_text.split()).strip()

    def _normalize_suffix(self, suffix: str) -> str:
        normalized = " ".join(suffix.split()).strip()
        normalized = normalized.replace("出访的记录", "出访记录")
        normalized = normalized.replace("出国的记录", "出国记录")
        normalized = normalized.replace("来访的记录", "来访记录")
        return normalized

    def _recent_user_messages(self, request_context: dict[str, Any]) -> list[str]:
        recent = request_context.get("recent_user_messages")
        if not isinstance(recent, list):
            return []
        return [str(item).strip() for item in recent if str(item).strip()]

    def _previous_identifier(self, messages: list[str]) -> tuple[str, str] | None:
        for message in reversed(messages):
            match = IDENTIFIER_PATTERN.search(message)
            if match:
                return match.group(1), match.group(2)
        return None

    def _context_summary(self, messages: list[str]) -> str | None:
        if not messages:
            return None
        return " | ".join(messages[-3:])
