from __future__ import annotations

from typing import Any

from ndea.query_v2 import PromotionCandidate


class MilvusLearningSync:
    def __init__(self, client: Any | None = None) -> None:
        self._client = client

    def sync(self, promotions: list[PromotionCandidate]) -> list[dict[str, object]]:
        return [
            {
                "promotion_type": promotion.promotion_type,
                "session_id": promotion.session_id,
                "confidence": promotion.confidence,
            }
            for promotion in promotions
        ]
