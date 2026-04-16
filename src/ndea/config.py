from pydantic import model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_prefix="NDEA_", extra="ignore")

    app_name: str = "NDEA"
    env: str = "development"
    log_level: str = "INFO"
    workflow_runtime: str = "langgraph"
    enable_legacy_tools: bool = False
    embedding_provider: str = "http"
    milvus_uri: str = "http://8.137.15.201:6333"
    milvus_token: str = ""
    milvus_database: str = "default"
    milvus_collection: str = "semantic_assets"
    milvus_vector_name: str = "embedding"
    milvus_search_limit: int = 5
    milvus_hybrid_enabled: bool = True
    milvus_hybrid_overfetch_limit: int = 20
    milvus_hybrid_vector_weight: float = 0.65
    milvus_hybrid_keyword_weight: float = 0.35
    milvus_hybrid_exact_match_boost: float = 0.15
    embedding_base_url: str = "http://192.168.2.43:11434"
    embedding_model: str = "bge-m3"
    embedding_vector_name: str = "embedding"
    langfuse_public_key: str = ""
    langfuse_secret_key: str = ""
    langfuse_host: str = "https://cloud.langfuse.com"
    learning_mysql_database: str = "ndea_learning"
    nl2sql_engine: str = "llamaindex"
    llamaindex_engine_factory: str = ""
    milvus_collection_sql_cases: str = "sql_cases"
    milvus_collection_query_memory: str = "query_memory"
    mysql_host: str = "127.0.0.1"
    mysql_port: int = 3306
    mysql_connection_backend: str = "sqlalchemy"
    mysql_connect_timeout: int = 5
    mysql_read_timeout: int = 30
    mysql_query_row_limit: int = 200
    mysql_explain_row_limit: int = 100000
    sqlalchemy_pool_size: int = 5
    sqlalchemy_max_overflow: int = 10
    sqlalchemy_pool_recycle: int = 1800
    sqlalchemy_pool_pre_ping: bool = True
    enable_query_execution: bool = True
    enable_semantic_retrieval: bool = True
    permission_allowed_tables: str = ""
    permission_blocked_columns: str = ""
    permission_masked_columns: str = ""
    permission_row_filters: str = ""
    mysql_user: str = "root"
    mysql_password: str = ""
    mysql_database: str = ""

    @model_validator(mode="before")
    @classmethod
    def _map_legacy_qdrant_settings(cls, data):
        if not isinstance(data, dict):
            return data

        mappings = {
            "qdrant_url": "milvus_uri",
            "qdrant_api_key": "milvus_token",
            "qdrant_collection": "milvus_collection",
            "qdrant_vector_name": "milvus_vector_name",
            "qdrant_search_limit": "milvus_search_limit",
            "qdrant_hybrid_enabled": "milvus_hybrid_enabled",
            "qdrant_hybrid_overfetch_limit": "milvus_hybrid_overfetch_limit",
            "qdrant_hybrid_vector_weight": "milvus_hybrid_vector_weight",
            "qdrant_hybrid_keyword_weight": "milvus_hybrid_keyword_weight",
            "qdrant_hybrid_exact_match_boost": "milvus_hybrid_exact_match_boost",
        }
        resolved = dict(data)
        for legacy_key, milvus_key in mappings.items():
            if milvus_key not in resolved and legacy_key in resolved:
                resolved[milvus_key] = resolved[legacy_key]
        return resolved

    @property
    def qdrant_url(self) -> str:
        return self.milvus_uri

    @property
    def qdrant_api_key(self) -> str:
        return self.milvus_token

    @property
    def qdrant_collection(self) -> str:
        return self.milvus_collection

    @property
    def qdrant_vector_name(self) -> str:
        return self.milvus_vector_name

    @property
    def qdrant_search_limit(self) -> int:
        return self.milvus_search_limit

    @property
    def qdrant_hybrid_enabled(self) -> bool:
        return self.milvus_hybrid_enabled

    @property
    def qdrant_hybrid_overfetch_limit(self) -> int:
        return self.milvus_hybrid_overfetch_limit

    @property
    def qdrant_hybrid_vector_weight(self) -> float:
        return self.milvus_hybrid_vector_weight

    @property
    def qdrant_hybrid_keyword_weight(self) -> float:
        return self.milvus_hybrid_keyword_weight

    @property
    def qdrant_hybrid_exact_match_boost(self) -> float:
        return self.milvus_hybrid_exact_match_boost

