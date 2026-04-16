from __future__ import annotations

from importlib import import_module
from typing import Any

from ndea.config import Settings
from ndea.query_v2 import PlanCandidate


class LlamaIndexNL2SQLEngine:
    model_name = "llamaindex"

    def __init__(
        self,
        enabled: bool = True,
        query_engine: Any | None = None,
    ) -> None:
        self._enabled = enabled
        self._query_engine = query_engine

    def generate(self, query_text: str, plan: PlanCandidate) -> str | None:
        if not self._enabled or plan.base_table is None:
            return None
        if self._query_engine is not None:
            return self._coerce_sql(self._query_engine, query_text, plan)
        try:
            import llama_index  # noqa: F401
        except ImportError:
            return None
        return None

    def _coerce_sql(self, query_engine: Any, query_text: str, plan: PlanCandidate) -> str | None:
        if hasattr(query_engine, "generate_sql"):
            sql = query_engine.generate_sql(query_text=query_text, plan=plan)
            return sql if isinstance(sql, str) and sql.strip() else None
        if callable(query_engine):
            sql = query_engine(query_text=query_text, plan=plan)
            return sql if isinstance(sql, str) and sql.strip() else None
        return None


def build_llamaindex_query_engine(settings: Settings) -> Any | None:
    factory_path = settings.llamaindex_engine_factory.strip()
    if not factory_path:
        return None
    module_name, _, attr_name = factory_path.partition(":")
    if not module_name or not attr_name:
        raise ValueError("NDEA_LLAMAINDEX_ENGINE_FACTORY must use module_path:callable_name")
    module = import_module(module_name)
    factory = getattr(module, attr_name)
    if not callable(factory):
        raise TypeError("Configured LlamaIndex engine factory must be callable")
    return factory(settings)
