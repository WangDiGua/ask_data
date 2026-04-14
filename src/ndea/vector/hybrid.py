from __future__ import annotations

import re
from typing import Any

from ndea.config import Settings


class HybridSearchScorer:
    def __init__(self, settings: Settings) -> None:
        self._vector_weight = settings.qdrant_hybrid_vector_weight
        self._keyword_weight = settings.qdrant_hybrid_keyword_weight
        self._exact_match_boost = settings.qdrant_hybrid_exact_match_boost

    def score(
        self,
        query_text: str,
        fields: list[str],
        vector_score: float,
    ) -> tuple[float, float]:
        keyword_score = self.keyword_score(query_text, fields)
        normalized_query = self._normalize_text(query_text)
        exact_boost = 0.0
        if normalized_query and any(
            normalized_query in self._normalize_text(field) for field in fields if field
        ):
            exact_boost = self._exact_match_boost
        hybrid_score = (
            (self._vector_weight * max(0.0, min(1.0, vector_score)))
            + (self._keyword_weight * keyword_score)
            + exact_boost
        )
        return min(1.0, hybrid_score), keyword_score

    def keyword_score(self, query_text: str, fields: list[str]) -> float:
        query_tokens = self._tokenize(query_text)
        if not query_tokens:
            return 0.0

        doc_tokens: set[str] = set()
        for field in fields:
            doc_tokens.update(self._tokenize(field))
        if not doc_tokens:
            return 0.0

        overlap = len(query_tokens & doc_tokens) / len(query_tokens)
        return min(1.0, overlap)

    def collect_strings(self, payload: Any) -> list[str]:
        values: list[str] = []
        self._collect(payload, values)
        return [value for value in values if value]

    def _collect(self, payload: Any, values: list[str]) -> None:
        if isinstance(payload, str):
            values.append(payload)
            return
        if isinstance(payload, dict):
            for value in payload.values():
                self._collect(value, values)
            return
        if isinstance(payload, list):
            for item in payload:
                self._collect(item, values)

    def _tokenize(self, text: str) -> set[str]:
        normalized = self._normalize_text(text)
        if not normalized:
            return set()

        tokens: set[str] = set()
        for chunk in re.findall(r"[\u4e00-\u9fff]+|[a-z0-9_]+", normalized):
            if not chunk:
                continue
            tokens.add(chunk)
            if re.fullmatch(r"[\u4e00-\u9fff]+", chunk):
                for width in (2, 3):
                    if len(chunk) < width:
                        continue
                    for index in range(len(chunk) - width + 1):
                        tokens.add(chunk[index : index + width])
            elif "_" in chunk:
                tokens.update(part for part in chunk.split("_") if part)
        return tokens

    def _normalize_text(self, text: str) -> str:
        return " ".join(re.findall(r"[\u4e00-\u9fff]+|[a-z0-9_]+", str(text).lower()))

