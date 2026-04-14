from ndea.config import Settings


def test_mysql_timeout_defaults_exist() -> None:
    settings = Settings()
    assert settings.mysql_connect_timeout == 5
    assert settings.mysql_read_timeout == 30
    assert settings.mysql_query_row_limit == 200
    assert settings.mysql_explain_row_limit == 100000
