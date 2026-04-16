from typing import Any

from pydantic import BaseModel

from ndea.config import Settings


class MySQLConnectionInfo(BaseModel):
    host: str
    port: int
    connect_timeout: int
    read_timeout: int
    user: str
    password: str
    database: str


def build_mysql_connection_info(settings: Settings, database: str | None = None) -> MySQLConnectionInfo:
    return MySQLConnectionInfo(
        host=settings.mysql_host,
        port=settings.mysql_port,
        connect_timeout=settings.mysql_connect_timeout,
        read_timeout=settings.mysql_read_timeout,
        user=settings.mysql_user,
        password=settings.mysql_password,
        database=database or settings.mysql_database,
    )


def build_mysql_connect_kwargs(settings: Settings, database: str | None = None) -> dict[str, Any]:
    info = build_mysql_connection_info(settings, database=database)
    return {
        "host": info.host,
        "port": info.port,
        "user": info.user,
        "password": info.password,
        "database": info.database or None,
        "connection_timeout": info.connect_timeout,
        "read_timeout": info.read_timeout,
    }


def open_mysql_connection(settings: Settings, database: str | None = None):
    if settings.mysql_connection_backend.lower() == "sqlalchemy":
        from ndea.metadata.sqlalchemy_client import open_sqlalchemy_connection

        return open_sqlalchemy_connection(settings, database=database)
    try:
        import mysql.connector
    except ImportError as exc:
        raise RuntimeError(
            "mysql-connector-python is required when NDEA_MYSQL_CONNECTION_BACKEND is not 'sqlalchemy'"
        ) from exc
    return mysql.connector.connect(**build_mysql_connect_kwargs(settings, database=database))
