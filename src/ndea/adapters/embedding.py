from __future__ import annotations

import hashlib
import json
import urllib.request

from ndea.config import Settings


class EmbeddingService:
    def __init__(self, settings: Settings) -> None:
        self._settings = settings

    def embed_query(self, query_text: str) -> list[float]:
        if self._settings.embedding_provider.lower() == "http":
            return self._http_embed(query_text)
        return self._hash_embed(query_text)

    def _http_embed(self, query_text: str) -> list[float]:
        payload = json.dumps({"model": self._settings.embedding_model, "input": [query_text]}, ensure_ascii=False).encode("utf-8")
        request = urllib.request.Request(
            url=f"{self._settings.embedding_base_url.rstrip('/')}/api/embed",
            data=payload,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urllib.request.urlopen(request, timeout=30) as response:
            body = json.loads(response.read().decode("utf-8"))
        embeddings = body.get("embeddings")
        if not isinstance(embeddings, list) or not embeddings or not isinstance(embeddings[0], list):
            return self._hash_embed(query_text)
        return [float(value) for value in embeddings[0]]

    def _hash_embed(self, query_text: str) -> list[float]:
        digest = hashlib.sha256(query_text.encode("utf-8")).digest()
        return [round(byte / 255.0, 6) for byte in digest[:16]]
