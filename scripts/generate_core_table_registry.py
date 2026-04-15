from __future__ import annotations

import json
from pathlib import Path

import site
import sys

ROOT = Path(__file__).resolve().parents[1]
site.addsitedir(str(ROOT / ".pydeps310"))
sys.path.insert(0, str(ROOT / "src"))

import mysql.connector

from ndea.config import Settings  # noqa: E402


CORE_TABLE_FIELDS: dict[str, tuple[str, ...]] = {
    "dcstu": ("YXMC", "PYCCMC", "ZZMMMC", "XSLBMC", "XBMC", "SFZX"),
    "dcemp": ("SZDWMC", "RYZTMC", "XB"),
    "dcorg": ("DWMC", "SFSY"),
    "t_bsdt_jzgygcg": ("NF", "PCDW", "ZC", "XZZW", "CFRWLX", "CFGJHDQ"),
    "t_bsdt_xsygcg": ("NF", "PCDW", "CFRWLX", "CFGJHDQ"),
    "t_gjc_lfzj": ("ND", "GJHDQ", "ZW", "ZC", "LFMD", "ZQDW"),
}


def main() -> None:
    settings = Settings()
    connection = mysql.connector.connect(
        host=settings.mysql_host,
        port=settings.mysql_port,
        user=settings.mysql_user,
        password=settings.mysql_password,
        database=settings.mysql_database,
    )
    cursor = connection.cursor(dictionary=True)
    try:
        payload: dict[str, object] = {
            "database": settings.mysql_database,
            "tables": {},
        }
        for table_name, sample_fields in CORE_TABLE_FIELDS.items():
            cursor.execute(
                """
                SELECT COLUMN_NAME, COLUMN_COMMENT
                FROM information_schema.COLUMNS
                WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s
                ORDER BY ORDINAL_POSITION
                """,
                (settings.mysql_database, table_name),
            )
            columns = [
                {
                    "name": row["COLUMN_NAME"],
                    "comment": row["COLUMN_COMMENT"] or "",
                }
                for row in cursor.fetchall()
            ]
            samples: dict[str, list[str]] = {}
            for field_name in sample_fields:
                cursor.execute(
                    f"""
                    SELECT {field_name} AS value
                    FROM {table_name}
                    WHERE {field_name} IS NOT NULL AND {field_name} <> ''
                    GROUP BY {field_name}
                    ORDER BY COUNT(*) DESC
                    LIMIT 10
                    """
                )
                samples[field_name] = [str(row["value"]) for row in cursor.fetchall()]

            payload["tables"][table_name] = {
                "columns": columns,
                "sample_values": samples,
            }

        output_path = ROOT / "docs" / "core_table_registry.generated.json"
        output_path.write_text(
            json.dumps(payload, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        print(f"Wrote {output_path}")
    finally:
        cursor.close()
        connection.close()


if __name__ == "__main__":
    main()
