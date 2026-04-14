from ndea.config import Settings
from ndea.metadata.sqlalchemy_client import (
    SQLAlchemyMySQLConnectionFactory,
    build_sqlalchemy_mysql_url,
)


class FakeEngine:
    def __init__(self) -> None:
        self.raw_connection_calls = 0

    def raw_connection(self):
        self.raw_connection_calls += 1
        return f"raw-connection-{self.raw_connection_calls}"


def test_build_sqlalchemy_mysql_url_uses_mysqlconnector_driver() -> None:
    settings = Settings(
        mysql_host="db.local",
        mysql_port=3307,
        mysql_user="reporter",
        mysql_password="secret",
        mysql_database="campus",
    )

    url = build_sqlalchemy_mysql_url(settings)

    assert url == "mysql+mysqlconnector://reporter:secret@db.local:3307/campus"


def test_sqlalchemy_connection_factory_caches_engine_per_database() -> None:
    created: list[tuple[str, dict[str, object], FakeEngine]] = []

    def fake_engine_factory(url: str, **kwargs):
        engine = FakeEngine()
        created.append((url, kwargs, engine))
        return engine

    settings = Settings(
        mysql_host="db.local",
        mysql_port=3307,
        mysql_user="reporter",
        mysql_password="secret",
        mysql_database="campus",
        sqlalchemy_pool_size=11,
        sqlalchemy_max_overflow=17,
        sqlalchemy_pool_recycle=1200,
        sqlalchemy_pool_pre_ping=True,
    )
    factory = SQLAlchemyMySQLConnectionFactory(settings=settings, engine_factory=fake_engine_factory)

    first = factory.open("campus")
    second = factory.open("campus")

    assert first == "raw-connection-1"
    assert second == "raw-connection-2"
    assert len(created) == 1
    url, kwargs, engine = created[0]
    assert url == "mysql+mysqlconnector://reporter:secret@db.local:3307/campus"
    assert kwargs == {
        "pool_pre_ping": True,
        "pool_recycle": 1200,
        "pool_size": 11,
        "max_overflow": 17,
    }
    assert engine.raw_connection_calls == 2
