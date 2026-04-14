from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_prefix="NDEA_", extra="ignore")

    app_name: str = "NDEA"
    env: str = "development"
    log_level: str = "INFO"
    workflow_runtime: str = "langgraph"
    qdrant_url: str = "http://8.137.15.201:6333"
    qdrant_api_key: str = ""
    qdrant_collection: str = "semantic_assets"
    qdrant_vector_name: str = "embedding"
    qdrant_search_limit: int = 5
    qdrant_hybrid_enabled: bool = True
    qdrant_hybrid_overfetch_limit: int = 20
    qdrant_hybrid_vector_weight: float = 0.65
    qdrant_hybrid_keyword_weight: float = 0.35
    qdrant_hybrid_exact_match_boost: float = 0.15
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

