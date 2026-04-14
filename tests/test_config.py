from ndea.config import Settings


def test_settings_have_expected_defaults() -> None:
    settings = Settings()
    assert settings.app_name == "NDEA"
    assert settings.mysql_port == 3306
    assert settings.qdrant_url == "http://8.137.15.201:6333"
    assert settings.qdrant_collection == "semantic_assets"
    assert settings.qdrant_vector_name == "embedding"
    assert settings.qdrant_search_limit == 5
    assert settings.qdrant_hybrid_enabled is True
    assert settings.qdrant_hybrid_overfetch_limit == 20
    assert settings.qdrant_hybrid_vector_weight == 0.65
    assert settings.qdrant_hybrid_keyword_weight == 0.35
    assert settings.qdrant_hybrid_exact_match_boost == 0.15
    assert settings.permission_allowed_tables == ""
    assert settings.permission_blocked_columns == ""
    assert settings.permission_masked_columns == ""
    assert settings.permission_row_filters == ""
