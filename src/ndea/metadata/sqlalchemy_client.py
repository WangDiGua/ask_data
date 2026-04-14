from __future__ import annotations

from collections.abc import Callable
from threading import Lock
from urllib.parse import quote_plus

from sqlalchemy import create_engine
from sqlalchemy.engine import Engine

from ndea.config import Settings

EngineFactory = Callable[..., Engine]

_factory_cache: dict[str, "SQLAlchemyMySQLConnectionFactory"] = {}
_factory_lock = Lock()


def build_sqlalchemy_mysql_url(
    settings: Settings,
    database: str | None = None,
) -> str:
    user = quote_plus(settings.mysql_user)
    password = quote_plus(settings.mysql_password)
    auth = user if not settings.mysql_password else f"{user}:{password}"
    resolved_database = database or settings.mysql_database
    return (
        f"mysql+mysqlconnector://{auth}"
        f"@{settings.mysql_host}:{settings.mysql_port}/{resolved_database}"
    )


class SQLAlchemyMySQLConnectionFactory:
    def __init__(
        self,
        settings: Settings,
        engine_factory: EngineFactory | None = None,
    ) -> None:
        self._settings = settings
        self._engine_factory = engine_factory or create_engine
        self._engines: dict[str, Engine] = {}
        self._lock = Lock()

    def open(self, database: str | None = None):
        engine = self._engine_for(database)
        return engine.raw_connection()

    def _engine_for(self, database: str | None = None) -> Engine:
        resolved_database = database or self._settings.mysql_database
        with self._lock:
            engine = self._engines.get(resolved_database)
            if engine is not None:
                return engine
            engine = self._engine_factory(
                build_sqlalchemy_mysql_url(self._settings, database=resolved_database),
                pool_pre_ping=self._settings.sqlalchemy_pool_pre_ping,
                pool_recycle=self._settings.sqlalchemy_pool_recycle,
                pool_size=self._settings.sqlalchemy_pool_size,
                max_overflow=self._settings.sqlalchemy_max_overflow,
            )
            self._engines[resolved_database] = engine
            return engine


def get_sqlalchemy_connection_factory(
    settings: Settings,
) -> SQLAlchemyMySQLConnectionFactory:
    key = "|".join(
        [
            settings.mysql_host,
            str(settings.mysql_port),
            settings.mysql_user,
            settings.mysql_database,
            settings.mysql_connection_backend,
            str(settings.sqlalchemy_pool_size),
            str(settings.sqlalchemy_max_overflow),
            str(settings.sqlalchemy_pool_recycle),
            str(settings.sqlalchemy_pool_pre_ping),
        ]
    )
    with _factory_lock:
        factory = _factory_cache.get(key)
        if factory is None:
            factory = SQLAlchemyMySQLConnectionFactory(settings=settings)
            _factory_cache[key] = factory
        return factory


def open_sqlalchemy_connection(
    settings: Settings,
    database: str | None = None,
):
    return get_sqlalchemy_connection_factory(settings).open(database=database)
