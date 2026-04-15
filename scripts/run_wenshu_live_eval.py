from __future__ import annotations

import json
import site
import sys
import urllib.request
from decimal import Decimal
from pathlib import Path
from typing import Any

site.addsitedir(r"D:\LantuConnect\ask_data\.pydeps310")
sys.path.insert(0, r"D:\LantuConnect\ask_data\src")

import mysql.connector

from ndea.config import Settings
from ndea.tools.query_workflow import get_query_workflow_service


def embed_texts(base_url: str, model: str, texts: list[str]) -> list[list[float]]:
    payload = json.dumps({"model": model, "input": texts}, ensure_ascii=False).encode("utf-8")
    request = urllib.request.Request(
        url=f"{base_url.rstrip('/')}/api/embed",
        data=payload,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(request, timeout=120) as response:
        body = json.loads(response.read().decode("utf-8"))
    embeddings = body.get("embeddings")
    if not isinstance(embeddings, list) or len(embeddings) != len(texts):
        raise RuntimeError("Embedding service returned unexpected payload")
    return embeddings


def open_mysql(settings: Settings):
    return mysql.connector.connect(
        host=settings.mysql_host,
        port=settings.mysql_port,
        user=settings.mysql_user,
        password=settings.mysql_password,
        database=settings.mysql_database,
        charset="utf8mb4",
        use_unicode=True,
    )


def normalize_scalar(value: Any) -> Any:
    if isinstance(value, Decimal):
        return int(value) if value == value.to_integral_value() else float(value)
    return value


def normalize_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    normalized = [{key: normalize_scalar(value) for key, value in row.items()} for row in rows]
    return sorted(normalized, key=lambda row: json.dumps(row, ensure_ascii=False, sort_keys=True))


def read_execution_rows(payload: Any) -> list[dict[str, Any]]:
    execution = getattr(payload, "execution", None) or {}
    if not isinstance(execution, dict):
        return []
    table = execution.get("table") or {}
    if isinstance(table, dict):
        rows = table.get("rows")
        if isinstance(rows, list):
            return [row for row in rows if isinstance(row, dict)]
    return []


def fetch_expected_rows(settings: Settings, sql: str) -> list[dict[str, Any]]:
    conn = open_mysql(settings)
    cur = conn.cursor(dictionary=True)
    cur.execute(sql)
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return rows


def main() -> None:
    settings = Settings()
    fixtures_path = Path("tests/evals/wenshu_live_fixtures.json")
    cases = json.loads(fixtures_path.read_text(encoding="utf-8"))
    workflow = get_query_workflow_service()

    planner_hits = 0
    golden_hits = 0
    golden_eligible = 0
    result_hits = 0
    execution_hits = 0
    failures: list[dict[str, Any]] = []

    for case in cases:
        query_text = case["query_text"]
        query_vector = embed_texts(settings.embedding_base_url, settings.embedding_model, [query_text])[0]
        payload = workflow.run(
            query_text=query_text,
            query_vector=query_vector,
            database=settings.mysql_database,
            execute=True,
        )

        expected_metric_id = case["expected_metric_id"]
        expected_tables = set(case.get("expected_tables", []))
        expected_dimensions = set(case.get("expected_dimension_ids", []))
        actual_tables = set(getattr(payload.plan, "candidate_tables", []))
        actual_dimensions = {dimension.dimension_id for dimension in getattr(payload.plan, "dimensions", [])}

        planner_ok = (
            getattr(payload.plan, "metric_id", None) == expected_metric_id
            and expected_tables.issubset(actual_tables)
            and expected_dimensions.issubset(actual_dimensions)
        )
        planner_hits += int(planner_ok)

        expected_selected_asset = case.get("expected_selected_sql_asset_id")
        golden_ok = None
        if expected_selected_asset:
            golden_eligible += 1
            golden_ok = getattr(payload.plan, "selected_sql_asset_id", None) == expected_selected_asset
            golden_hits += int(bool(golden_ok))

        execution = getattr(payload, "execution", None) or {}
        execution_ok = bool(getattr(payload, "executed", False) and isinstance(execution, dict) and execution.get("allowed"))
        execution_hits += int(execution_ok)

        expected_rows = normalize_rows(fetch_expected_rows(settings, case["expected_sql"]))
        actual_rows = normalize_rows(read_execution_rows(payload))
        result_ok = execution_ok and actual_rows == expected_rows
        result_hits += int(result_ok)

        if not (planner_ok and execution_ok and result_ok and (golden_ok is None or golden_ok)):
            failures.append(
                {
                    "query_text": query_text,
                    "planner_ok": planner_ok,
                    "golden_ok": golden_ok,
                    "execution_ok": execution_ok,
                    "result_ok": result_ok,
                    "selected_sql_asset_id": getattr(payload.plan, "selected_sql_asset_id", None),
                    "actual_metric_id": getattr(payload.plan, "metric_id", None),
                    "actual_tables": sorted(actual_tables),
                    "actual_dimensions": sorted(actual_dimensions),
                    "actual_sql": (execution.get("sql") if isinstance(execution, dict) else None),
                }
            )

    total = len(cases)
    summary = {
        "total_cases": total,
        "planner_hit_rate": round(planner_hits / total, 4),
        "golden_sql_hit_rate": round(golden_hits / golden_eligible, 4) if golden_eligible else None,
        "execution_success_rate": round(execution_hits / total, 4),
        "result_accuracy_rate": round(result_hits / total, 4),
        "failed_cases": failures,
    }
    print(json.dumps(summary, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
