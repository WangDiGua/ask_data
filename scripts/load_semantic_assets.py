from __future__ import annotations

import json
import site
import sys
import urllib.request
from dataclasses import dataclass
from typing import Any

site.addsitedir(r"D:\LantuConnect\ask_data\.pydeps310")
sys.path.insert(0, r"D:\LantuConnect\ask_data\src")

import mysql.connector
from pymilvus import MilvusClient

from ndea.config import Settings


@dataclass
class Asset:
    asset_id: str
    asset_type: str
    title: str
    text: str
    source: str
    metadata: dict[str, Any]


TABLE_COMMENT_FALLBACKS = {
    "dcemp": "教职工基础信息表",
    "dcorg": "组织机构基础信息表",
    "dcstu": "学生基础信息表",
    "t_bsdt_jzgygcg": "教职工因公出国（境）记录表",
    "t_bsdt_xsygcg": "学生因公出国（境）记录表",
    "t_gjc_lfzj": "来访专家记录表",
}

SAMPLE_QUERIES = {
    "dcemp": {
        "status_distribution": "SELECT RYZTMC, COUNT(*) AS total FROM dcemp GROUP BY RYZTMC ORDER BY total DESC LIMIT 8",
        "org_distribution": "SELECT SZDWMC, COUNT(*) AS total FROM dcemp GROUP BY SZDWMC ORDER BY total DESC LIMIT 12",
        "gender_distribution": "SELECT XB, COUNT(*) AS total FROM dcemp GROUP BY XB ORDER BY total DESC LIMIT 8",
    },
    "dcorg": {
        "active_distribution": "SELECT sfsy, COUNT(*) AS total FROM dcorg GROUP BY sfsy ORDER BY total DESC LIMIT 8",
        "name_distribution": "SELECT dwmc, COUNT(*) AS total FROM dcorg GROUP BY dwmc ORDER BY total DESC LIMIT 12",
    },
    "dcstu": {
        "college_distribution": "SELECT YXMC, COUNT(*) AS total FROM dcstu GROUP BY YXMC ORDER BY total DESC LIMIT 12",
        "major_distribution": "SELECT ZYMC, COUNT(*) AS total FROM dcstu GROUP BY ZYMC ORDER BY total DESC LIMIT 12",
        "level_distribution": "SELECT PYCCMC, COUNT(*) AS total FROM dcstu GROUP BY PYCCMC ORDER BY total DESC LIMIT 8",
        "gender_distribution": "SELECT XBMC, COUNT(*) AS total FROM dcstu GROUP BY XBMC ORDER BY total DESC LIMIT 8",
        "political_distribution": "SELECT ZZMMMC, COUNT(*) AS total FROM dcstu GROUP BY ZZMMMC ORDER BY total DESC LIMIT 12",
    },
    "t_bsdt_jzgygcg": {
        "year_distribution": "SELECT NF, COUNT(*) AS total FROM t_bsdt_jzgygcg GROUP BY NF ORDER BY NF DESC LIMIT 8",
        "country_distribution": "SELECT CFGJHDQ, COUNT(*) AS total FROM t_bsdt_jzgygcg GROUP BY CFGJHDQ ORDER BY total DESC LIMIT 12",
        "task_distribution": "SELECT CFRWLX, COUNT(*) AS total FROM t_bsdt_jzgygcg GROUP BY CFRWLX ORDER BY total DESC LIMIT 12",
        "org_distribution": "SELECT PCDW, COUNT(*) AS total FROM t_bsdt_jzgygcg GROUP BY PCDW ORDER BY total DESC LIMIT 12",
        "title_distribution": "SELECT ZC, COUNT(*) AS total FROM t_bsdt_jzgygcg GROUP BY ZC ORDER BY total DESC LIMIT 12",
    },
    "t_bsdt_xsygcg": {
        "year_distribution": "SELECT NF, COUNT(*) AS total FROM t_bsdt_xsygcg GROUP BY NF ORDER BY NF DESC LIMIT 8",
        "country_distribution": "SELECT CFGJHDQ, COUNT(*) AS total FROM t_bsdt_xsygcg GROUP BY CFGJHDQ ORDER BY total DESC LIMIT 12",
        "category_distribution": "SELECT CFRWLX, COUNT(*) AS total FROM t_bsdt_xsygcg GROUP BY CFRWLX ORDER BY total DESC LIMIT 12",
        "org_distribution": "SELECT PCDW, COUNT(*) AS total FROM t_bsdt_xsygcg GROUP BY PCDW ORDER BY total DESC LIMIT 12",
    },
    "t_gjc_lfzj": {
        "year_distribution": "SELECT ND, COUNT(*) AS total FROM t_gjc_lfzj GROUP BY ND ORDER BY ND DESC LIMIT 10",
        "country_distribution": "SELECT GJHDQ, COUNT(*) AS total FROM t_gjc_lfzj GROUP BY GJHDQ ORDER BY total DESC LIMIT 12",
        "purpose_distribution": "SELECT LFMD, COUNT(*) AS total FROM t_gjc_lfzj GROUP BY LFMD ORDER BY total DESC LIMIT 12",
        "inviter_distribution": "SELECT YQRDW, COUNT(*) AS total FROM t_gjc_lfzj GROUP BY YQRDW ORDER BY total DESC LIMIT 12",
        "continent_distribution": "SELECT ZB, COUNT(*) AS total FROM t_gjc_lfzj GROUP BY ZB ORDER BY total DESC LIMIT 8",
        "submitter_org_distribution": "SELECT dcorg.dwmc, COUNT(*) AS total FROM t_gjc_lfzj JOIN dcorg ON t_gjc_lfzj.SBRDWBM = dcorg.xndwdm GROUP BY dcorg.dwmc ORDER BY total DESC LIMIT 12",
    },
}


def embed_texts(base_url: str, model: str, texts: list[str], batch_size: int = 24) -> list[list[float]]:
    embeddings: list[list[float]] = []
    for start in range(0, len(texts), batch_size):
        chunk = texts[start : start + batch_size]
        payload = json.dumps({"model": model, "input": chunk}, ensure_ascii=False).encode("utf-8")
        request = urllib.request.Request(
            url=f"{base_url.rstrip('/')}/api/embed",
            data=payload,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urllib.request.urlopen(request, timeout=120) as response:
            body = json.loads(response.read().decode("utf-8"))
        chunk_embeddings = body.get("embeddings")
        if not isinstance(chunk_embeddings, list) or len(chunk_embeddings) != len(chunk):
            raise RuntimeError("Embedding service returned unexpected payload")
        embeddings.extend(chunk_embeddings)
    return embeddings


def open_mysql(settings: Settings):
    return mysql.connector.connect(
        host=settings.mysql_host,
        port=settings.mysql_port,
        user=settings.mysql_user,
        password=settings.mysql_password,
        database=settings.mysql_database,
        charset="utf8mb4",
        collation="utf8mb4_0900_ai_ci",
        use_unicode=True,
    )


def fetch_schema_snapshot(settings: Settings) -> dict[str, Any]:
    conn = open_mysql(settings)
    cur = conn.cursor(dictionary=True)
    table_names = tuple(TABLE_COMMENT_FALLBACKS)
    placeholders = ", ".join(["%s"] * len(table_names))
    cur.execute(
        f"SELECT table_name, table_comment FROM information_schema.tables WHERE table_schema=%s AND table_name IN ({placeholders}) ORDER BY table_name",
        (settings.mysql_database, *table_names),
    )
    tables = [{str(key).lower(): value for key, value in row.items()} for row in cur.fetchall()]
    cur.execute(
        f"SELECT table_name, column_name, column_comment FROM information_schema.columns WHERE table_schema=%s AND table_name IN ({placeholders}) ORDER BY table_name, ordinal_position",
        (settings.mysql_database, *table_names),
    )
    columns_by_table: dict[str, list[dict[str, Any]]] = {}
    for row in cur.fetchall():
        normalized = {str(key).lower(): value for key, value in row.items()}
        columns_by_table.setdefault(normalized["table_name"], []).append(normalized)
    samples: dict[str, dict[str, list[dict[str, Any]]]] = {}
    for table_name, queries in SAMPLE_QUERIES.items():
        samples[table_name] = {}
        for label, sql in queries.items():
            cur.execute(sql)
            samples[table_name][label] = cur.fetchall()
    cur.close()
    conn.close()
    return {"tables": tables, "columns_by_table": columns_by_table, "samples": samples}


def sample_values(snapshot: dict[str, Any], table_name: str, label: str, field_name: str) -> list[str]:
    rows = snapshot["samples"].get(table_name, {}).get(label, [])
    values = [row[field_name].strip() for row in rows if isinstance(row.get(field_name), str) and row[field_name].strip()]
    return list(dict.fromkeys(values))


def build_assets(snapshot: dict[str, Any], settings: Settings) -> list[Asset]:
    del settings
    tables = snapshot["tables"]
    columns_by_table = snapshot["columns_by_table"]
    assets: list[Asset] = []

    for table in tables:
        table_name = table["table_name"]
        table_comment = table["table_comment"] or TABLE_COMMENT_FALLBACKS.get(table_name, table_name)
        columns = columns_by_table[table_name]
        column_desc = "；".join(f"{item['column_name']}（{item['column_comment'] or '未注释'}）" for item in columns)
        sample_desc = " | ".join(
            f"{label}: {'；'.join(json.dumps(row, ensure_ascii=False) for row in rows[:4])}"
            for label, rows in snapshot["samples"].get(table_name, {}).items()
            if rows
        )
        assets.append(Asset(f"schema:{table_name}", "schema", table_comment, f"{table_comment}。表名：{table_name}。字段：{column_desc}。样例分布：{sample_desc}", "semantic_bootstrap_v2", {"table_name": table_name, "table_comment": table_comment, "columns": [{"column_name": item["column_name"], "column_comment": item["column_comment"] or ""} for item in columns]}))

    metric_specs = {
        "active_staff_count": ("在岗教职工人数", "统计当前在岗的教职工数量，可按单位、性别分析。", ["教职工人数", "在岗人数", "在职教职工人数", "教师人数"], "dcemp", "COUNT(*)", ["dcemp.RYZTMC = '在岗'"], ["staff_org_name", "staff_gender", "staff_org_active_flag"], None, [], ["employee_org_join"], "faculty", ["统计在岗教职工人数", "按单位统计在岗教职工人数", "烟台研究院有多少在岗教职工"]),
        "active_student_count": ("在校学生人数", "统计当前在校学生数量，可按学院、专业、培养层次、性别、政治面貌分析。", ["学生人数", "在校生人数", "在籍学生人数", "学生规模"], "dcstu", "COUNT(*)", ["dcstu.SFZX = '是'"], ["student_college_name", "student_major_name", "student_level_name", "student_gender_name", "student_political_status_name"], None, [], [], "student", ["统计在校学生人数", "按学院统计在校学生人数", "计算机科学与技术专业有多少在校学生"]),
        "organization_count": ("组织机构数量", "统计组织机构记录数量，可按启用状态分析。", ["单位数量", "机构数量", "部门数量", "组织数量"], "dcorg", "COUNT(*)", [], ["org_active_flag", "org_name"], None, [], [], "organization", ["统计组织机构数量", "按启用状态统计组织机构数量"]),
        "teacher_outbound_count": ("教职工因公出国记录数", "统计教职工因公出国（境）记录数，可按年度、国家、学院、任务类型、职称分析。", ["教职工出访次数", "教师出访次数", "教职工出国记录数"], "t_bsdt_jzgygcg", "COUNT(*)", [], ["teacher_outbound_year", "teacher_outbound_country", "teacher_outbound_org_name", "teacher_outbound_task_type", "teacher_outbound_title"], "t_bsdt_jzgygcg.NF", ["year"], ["teacher_outbound_org_join"], "faculty", ["按年度统计教职工因公出国记录数", "按国家统计教职工因公出国记录数", "2025年烟台研究院教职工因公出国记录数"]),
        "student_outbound_count": ("学生因公出国记录数", "统计学生因公出国（境）记录数，可按年度、国家、学院、学生类别分析。", ["学生出访次数", "学生出国记录数", "学生因公出访次数"], "t_bsdt_xsygcg", "COUNT(*)", [], ["student_outbound_year", "student_outbound_country", "student_outbound_org_name", "student_outbound_category"], "t_bsdt_xsygcg.NF", ["year"], ["student_outbound_org_join"], "student", ["按年度统计学生因公出国记录数", "按国家统计学生因公出国记录数", "2025年烟台研究院学生因公出国记录数"]),
        "visiting_expert_count": ("来访专家人数", "统计来访专家记录数，可按年度、国家、邀请单位、申报单位、来访目的、洲别分析。", ["来访专家记录数", "外专来访人数", "外国专家来访人数"], "t_gjc_lfzj", "COUNT(*)", [], ["visiting_expert_year", "visiting_expert_country", "visiting_expert_inviter_org_name", "visiting_expert_submitter_org_name", "visiting_expert_purpose", "visiting_expert_continent"], "t_gjc_lfzj.ND", ["year"], ["visiting_expert_submitter_org_join"], "expert", ["按国家统计来访专家人数", "按来访目的统计来访专家人数", "按申报单位统计来访专家人数"]),
    }
    for metric_id, spec in metric_specs.items():
        name, description, aliases, base_table, measure_expression, default_filters, available_dimensions, time_field, supported_time_grains, join_path_ids, entity_scope, examples = spec
        assets.append(Asset(f"metric:{metric_id}", "metric", name, description, "semantic_bootstrap_v2", {"table_name": base_table}))
        assets.append(Asset(f"metric_contract:{metric_id}", "metric_contract", name, f"{name}业务口径。", "semantic_bootstrap_v2", {"metric_contract": {"metric_id": metric_id, "name": name, "aliases": aliases, "business_definition": description, "base_table": base_table, "measure_expression": measure_expression, "default_filters": default_filters, "available_dimensions": available_dimensions, "time_field": time_field, "supported_time_grains": supported_time_grains, "join_path_ids": join_path_ids, "entity_scope": entity_scope, "entity_scope_options": [entity_scope], "requires_entity_scope": False, "requires_time_scope": False, "example_questions": examples}}))

    dimension_specs = [
        ("staff_org_name", "教职工所在单位", ["单位", "所在单位", "学院", "部门"], "dcemp", "SZDWMC", "dcemp.SZDWMC", "org_name", sample_values(snapshot, "dcemp", "org_distribution", "SZDWMC")),
        ("staff_gender", "教职工性别", ["性别", "男女"], "dcemp", "XB", "dcemp.XB", "gender", sample_values(snapshot, "dcemp", "gender_distribution", "XB")),
        ("staff_org_active_flag", "教职工所在单位启用状态", ["单位启用状态", "组织启用状态", "是否启用"], "dcorg", "sfsy", "dcorg.sfsy", "is_active", sample_values(snapshot, "dcorg", "active_distribution", "sfsy")),
        ("student_college_name", "学生学院", ["学院", "院系", "所在学院"], "dcstu", "YXMC", "dcstu.YXMC", "college_name", sample_values(snapshot, "dcstu", "college_distribution", "YXMC")),
        ("student_major_name", "学生专业", ["专业", "专业名称"], "dcstu", "ZYMC", "dcstu.ZYMC", "major_name", sample_values(snapshot, "dcstu", "major_distribution", "ZYMC")),
        ("student_level_name", "培养层次", ["培养层次", "学历层次", "层次"], "dcstu", "PYCCMC", "dcstu.PYCCMC", "level_name", sample_values(snapshot, "dcstu", "level_distribution", "PYCCMC")),
        ("student_gender_name", "学生性别", ["性别", "男女"], "dcstu", "XBMC", "dcstu.XBMC", "gender_name", sample_values(snapshot, "dcstu", "gender_distribution", "XBMC")),
        ("student_political_status_name", "政治面貌", ["政治面貌", "政治身份"], "dcstu", "ZZMMMC", "dcstu.ZZMMMC", "political_status_name", sample_values(snapshot, "dcstu", "political_distribution", "ZZMMMC")),
        ("org_name", "组织机构名称", ["单位名称", "组织名称", "机构名称"], "dcorg", "dwmc", "dcorg.dwmc", "org_name", sample_values(snapshot, "dcorg", "name_distribution", "dwmc")),
        ("org_active_flag", "组织启用状态", ["是否启用", "启用状态", "是否使用"], "dcorg", "sfsy", "dcorg.sfsy", "is_active", sample_values(snapshot, "dcorg", "active_distribution", "sfsy")),
        ("teacher_outbound_year", "教职工出访年度", ["年度", "年份", "按年", "分年度"], "t_bsdt_jzgygcg", "NF", "t_bsdt_jzgygcg.NF", "year", sample_values(snapshot, "t_bsdt_jzgygcg", "year_distribution", "NF")),
        ("teacher_outbound_country", "教职工出访国家地区", ["国家", "国家地区", "出访国家"], "t_bsdt_jzgygcg", "CFGJHDQ", "t_bsdt_jzgygcg.CFGJHDQ", "country_region", sample_values(snapshot, "t_bsdt_jzgygcg", "country_distribution", "CFGJHDQ")),
        ("teacher_outbound_org_name", "教职工出访学院", ["学院", "单位", "派出单位"], "dcorg", "dwmc", "dcorg.dwmc", "org_name", sample_values(snapshot, "t_bsdt_jzgygcg", "org_distribution", "PCDW")),
        ("teacher_outbound_task_type", "教职工出访任务类型", ["任务类型", "出访类型", "任务类别"], "t_bsdt_jzgygcg", "CFRWLX", "t_bsdt_jzgygcg.CFRWLX", "task_type", sample_values(snapshot, "t_bsdt_jzgygcg", "task_distribution", "CFRWLX")),
        ("teacher_outbound_title", "教职工职称", ["职称", "教师职称"], "t_bsdt_jzgygcg", "ZC", "t_bsdt_jzgygcg.ZC", "title_name", sample_values(snapshot, "t_bsdt_jzgygcg", "title_distribution", "ZC")),
        ("student_outbound_year", "学生出访年度", ["年度", "年份", "按年", "分年度"], "t_bsdt_xsygcg", "NF", "t_bsdt_xsygcg.NF", "year", sample_values(snapshot, "t_bsdt_xsygcg", "year_distribution", "NF")),
        ("student_outbound_country", "学生出访国家地区", ["国家", "国家地区", "出访国家"], "t_bsdt_xsygcg", "CFGJHDQ", "t_bsdt_xsygcg.CFGJHDQ", "country_region", sample_values(snapshot, "t_bsdt_xsygcg", "country_distribution", "CFGJHDQ")),
        ("student_outbound_org_name", "学生出访学院", ["学院", "单位", "派出单位"], "dcorg", "dwmc", "dcorg.dwmc", "org_name", sample_values(snapshot, "t_bsdt_xsygcg", "org_distribution", "PCDW")),
        ("student_outbound_category", "学生出访类别", ["学生类别", "层次", "学历层次", "任务类型"], "t_bsdt_xsygcg", "CFRWLX", "t_bsdt_xsygcg.CFRWLX", "student_category", sample_values(snapshot, "t_bsdt_xsygcg", "category_distribution", "CFRWLX")),
        ("visiting_expert_year", "来访专家年度", ["年度", "年份", "按年", "分年度"], "t_gjc_lfzj", "ND", "t_gjc_lfzj.ND", "year", sample_values(snapshot, "t_gjc_lfzj", "year_distribution", "ND")),
        ("visiting_expert_country", "来访专家国家地区", ["国家", "国家地区", "来源国家"], "t_gjc_lfzj", "GJHDQ", "t_gjc_lfzj.GJHDQ", "country_region", sample_values(snapshot, "t_gjc_lfzj", "country_distribution", "GJHDQ")),
        ("visiting_expert_inviter_org_name", "来访专家邀请单位", ["邀请单位", "学院", "邀请学院"], "t_gjc_lfzj", "YQRDW", "t_gjc_lfzj.YQRDW", "org_name", sample_values(snapshot, "t_gjc_lfzj", "inviter_distribution", "YQRDW")),
        ("visiting_expert_submitter_org_name", "来访专家申报单位", ["申报单位", "填报单位", "报送单位"], "dcorg", "dwmc", "dcorg.dwmc", "org_name", sample_values(snapshot, "t_gjc_lfzj", "submitter_org_distribution", "dwmc")),
        ("visiting_expert_purpose", "来访目的", ["来访目的", "访问目的"], "t_gjc_lfzj", "LFMD", "t_gjc_lfzj.LFMD", "visit_purpose", sample_values(snapshot, "t_gjc_lfzj", "purpose_distribution", "LFMD")),
        ("visiting_expert_continent", "来访专家洲别", ["洲别", "大洲"], "t_gjc_lfzj", "ZB", "t_gjc_lfzj.ZB", "continent", sample_values(snapshot, "t_gjc_lfzj", "continent_distribution", "ZB")),
    ]
    for dimension_id, name, aliases, table_name, column_name, expression, output_alias, values in dimension_specs:
        assets.append(Asset(f"dimension_contract:{dimension_id}", "dimension_contract", name, f"{name}维度，来源表 {table_name} 字段 {column_name}。常见值：{'、'.join(values[:6]) or '无'}。", "semantic_bootstrap_v2", {"dimension_contract": {"dimension_id": dimension_id, "name": name, "aliases": aliases, "table": table_name, "column": column_name, "expression": expression, "groupable": True, "output_alias": output_alias, "sample_values": values}}))

    join_specs = [
        ("employee_org_join", "dcemp", "dcorg", "dcemp.SZDWDM = dcorg.xndwdm", "JOIN dcorg ON dcemp.SZDWDM = dcorg.xndwdm", "教职工表关联组织机构表"),
        ("teacher_outbound_org_join", "t_bsdt_jzgygcg", "dcorg", "t_bsdt_jzgygcg.DWDM = dcorg.xndwdm", "JOIN dcorg ON t_bsdt_jzgygcg.DWDM = dcorg.xndwdm", "教职工因公出国记录关联组织机构表"),
        ("student_outbound_org_join", "t_bsdt_xsygcg", "dcorg", "t_bsdt_xsygcg.DWDM = dcorg.xndwdm", "JOIN dcorg ON t_bsdt_xsygcg.DWDM = dcorg.xndwdm", "学生因公出国记录关联组织机构表"),
        ("visiting_expert_submitter_org_join", "t_gjc_lfzj", "dcorg", "t_gjc_lfzj.SBRDWBM = dcorg.xndwdm", "JOIN dcorg ON t_gjc_lfzj.SBRDWBM = dcorg.xndwdm", "来访专家按申报单位编码关联组织机构表"),
    ]
    for join_id, left_table, right_table, join_condition, join_sql, semantic_meaning in join_specs:
        assets.append(Asset(f"join_path:{join_id}", "join_path", semantic_meaning, f"{left_table} 与 {right_table} 可通过 {join_condition} 关联。", "semantic_bootstrap_v2", {"join_path_contract": {"join_id": join_id, "left_table": left_table, "right_table": right_table, "join_type": "INNER", "join_condition": join_condition, "join_sql": join_sql, "cardinality": "many_to_one", "semantic_meaning": semantic_meaning, "disabled": False}}))

    for semantic_id, name, field in [("teacher_outbound_year", "教职工出访年度", "t_bsdt_jzgygcg.NF"), ("student_outbound_year", "学生出访年度", "t_bsdt_xsygcg.NF"), ("visiting_expert_year", "来访专家年度", "t_gjc_lfzj.ND")]:
        assets.append(Asset(f"time_semantics:{semantic_id}", "time_semantics", name, f"{name}时间语义，对应字段 {field}，适合按年统计或同比分析。", "semantic_bootstrap_v2", {"time_semantics": {"semantic_id": semantic_id, "name": name, "aliases": ["年度", "年份", "按年", "分年度"], "field": field, "supported_grains": ["year"], "default_grain": "year", "comparison_modes": ["yoy"]}}))

    def add_golden(asset_suffix: str, question: str, sql: str, metric_id: str, dimensions: list[str], tables_used: list[str], entity_scope: str, time_grains: list[str] | None = None, specific_filters: list[str] | None = None) -> None:
        metadata: dict[str, Any] = {"metric_id": metric_id, "dimensions": dimensions, "entity_scope": entity_scope, "tables": tables_used, "question": question, "sql": sql}
        if time_grains:
            metadata["time_grains"] = time_grains
        if specific_filters:
            metadata["specific_filters"] = specific_filters
        assets.append(Asset(f"golden_sql:{asset_suffix}", "golden_sql", question, f"问题：{question}。SQL：{sql}", "semantic_bootstrap_v2", metadata))

    add_golden("active_staff_total", "统计在岗教职工人数", "SELECT COUNT(*) AS total FROM dcemp WHERE dcemp.RYZTMC = '在岗'", "active_staff_count", [], ["dcemp"], "faculty")
    add_golden("active_staff_by_org", "按单位统计在岗教职工人数", "SELECT dcemp.SZDWMC AS org_name, COUNT(*) AS total FROM dcemp WHERE dcemp.RYZTMC = '在岗' GROUP BY dcemp.SZDWMC ORDER BY total DESC", "active_staff_count", ["staff_org_name"], ["dcemp"], "faculty")
    add_golden("active_staff_by_gender", "按性别统计在岗教职工人数", "SELECT dcemp.XB AS gender, COUNT(*) AS total FROM dcemp WHERE dcemp.RYZTMC = '在岗' GROUP BY dcemp.XB ORDER BY total DESC", "active_staff_count", ["staff_gender"], ["dcemp"], "faculty")
    add_golden("active_staff_yantai_total", "烟台研究院在岗教职工人数", "SELECT COUNT(*) AS total FROM dcemp WHERE dcemp.RYZTMC = '在岗' AND dcemp.SZDWMC = '烟台研究院'", "active_staff_count", [], ["dcemp"], "faculty", specific_filters=["烟台研究院"])
    add_golden("active_staff_female_total", "统计女教职工人数", "SELECT COUNT(*) AS total FROM dcemp WHERE dcemp.RYZTMC = '在岗' AND dcemp.XB = '女'", "active_staff_count", [], ["dcemp"], "faculty", specific_filters=["女"])
    add_golden("active_students_total", "统计在校学生人数", "SELECT COUNT(*) AS total FROM dcstu WHERE dcstu.SFZX = '是'", "active_student_count", [], ["dcstu"], "student")
    add_golden("active_students_by_college", "按学院统计在校学生人数", "SELECT dcstu.YXMC AS college_name, COUNT(*) AS total FROM dcstu WHERE dcstu.SFZX = '是' GROUP BY dcstu.YXMC ORDER BY total DESC", "active_student_count", ["student_college_name"], ["dcstu"], "student")
    add_golden("active_students_by_major", "按专业统计在校学生人数", "SELECT dcstu.ZYMC AS major_name, COUNT(*) AS total FROM dcstu WHERE dcstu.SFZX = '是' GROUP BY dcstu.ZYMC ORDER BY total DESC", "active_student_count", ["student_major_name"], ["dcstu"], "student")
    add_golden("active_students_by_level", "按培养层次统计在校学生人数", "SELECT dcstu.PYCCMC AS level_name, COUNT(*) AS total FROM dcstu WHERE dcstu.SFZX = '是' GROUP BY dcstu.PYCCMC ORDER BY total DESC", "active_student_count", ["student_level_name"], ["dcstu"], "student")
    add_golden("active_students_by_gender", "按性别统计在校学生人数", "SELECT dcstu.XBMC AS gender_name, COUNT(*) AS total FROM dcstu WHERE dcstu.SFZX = '是' GROUP BY dcstu.XBMC ORDER BY total DESC", "active_student_count", ["student_gender_name"], ["dcstu"], "student")
    add_golden("active_students_by_political_status", "按政治面貌统计在校学生人数", "SELECT dcstu.ZZMMMC AS political_status_name, COUNT(*) AS total FROM dcstu WHERE dcstu.SFZX = '是' GROUP BY dcstu.ZZMMMC ORDER BY total DESC", "active_student_count", ["student_political_status_name"], ["dcstu"], "student")
    add_golden("active_students_yantai_total", "烟台研究院在校学生人数", "SELECT COUNT(*) AS total FROM dcstu WHERE dcstu.SFZX = '是' AND dcstu.YXMC = '烟台研究院'", "active_student_count", [], ["dcstu"], "student", specific_filters=["烟台研究院"])
    add_golden("active_students_cs_total", "计算机科学与技术专业在校学生人数", "SELECT COUNT(*) AS total FROM dcstu WHERE dcstu.SFZX = '是' AND dcstu.ZYMC = '计算机科学与技术'", "active_student_count", [], ["dcstu"], "student", specific_filters=["计算机科学与技术"])
    add_golden("active_students_undergraduate_total", "本科在校学生人数", "SELECT COUNT(*) AS total FROM dcstu WHERE dcstu.SFZX = '是' AND dcstu.PYCCMC = '本科'", "active_student_count", [], ["dcstu"], "student", specific_filters=["本科"])
    add_golden("active_students_female_total", "女学生人数", "SELECT COUNT(*) AS total FROM dcstu WHERE dcstu.SFZX = '是' AND dcstu.XBMC = '女'", "active_student_count", [], ["dcstu"], "student", specific_filters=["女"])
    add_golden("organization_total", "统计组织机构数量", "SELECT COUNT(*) AS total FROM dcorg", "organization_count", [], ["dcorg"], "organization")
    add_golden("organization_active_total", "统计启用组织机构数量", "SELECT COUNT(*) AS total FROM dcorg WHERE dcorg.sfsy = '是'", "organization_count", [], ["dcorg"], "organization", specific_filters=["是"])
    add_golden("organization_by_active_flag", "按启用状态统计组织机构数量", "SELECT dcorg.sfsy AS is_active, COUNT(*) AS total FROM dcorg GROUP BY dcorg.sfsy ORDER BY total DESC", "organization_count", ["org_active_flag"], ["dcorg"], "organization")
    add_golden("teacher_outbound_total", "统计教职工因公出国记录数", "SELECT COUNT(*) AS total FROM t_bsdt_jzgygcg", "teacher_outbound_count", [], ["t_bsdt_jzgygcg"], "faculty")
    add_golden("teacher_outbound_by_year", "按年度统计教职工因公出国记录数", "SELECT t_bsdt_jzgygcg.NF AS year, COUNT(*) AS total FROM t_bsdt_jzgygcg GROUP BY t_bsdt_jzgygcg.NF ORDER BY year DESC", "teacher_outbound_count", ["teacher_outbound_year"], ["t_bsdt_jzgygcg"], "faculty", ["year"])
    add_golden("teacher_outbound_by_country", "按国家统计教职工因公出国记录数", "SELECT t_bsdt_jzgygcg.CFGJHDQ AS country_region, COUNT(*) AS total FROM t_bsdt_jzgygcg GROUP BY t_bsdt_jzgygcg.CFGJHDQ ORDER BY total DESC", "teacher_outbound_count", ["teacher_outbound_country"], ["t_bsdt_jzgygcg"], "faculty")
    add_golden("teacher_outbound_by_org", "按学院统计教职工因公出国记录数", "SELECT dcorg.dwmc AS org_name, COUNT(*) AS total FROM t_bsdt_jzgygcg JOIN dcorg ON t_bsdt_jzgygcg.DWDM = dcorg.xndwdm GROUP BY dcorg.dwmc ORDER BY total DESC", "teacher_outbound_count", ["teacher_outbound_org_name"], ["t_bsdt_jzgygcg", "dcorg"], "faculty")
    add_golden("teacher_outbound_by_task_type", "按任务类型统计教职工因公出国记录数", "SELECT t_bsdt_jzgygcg.CFRWLX AS task_type, COUNT(*) AS total FROM t_bsdt_jzgygcg GROUP BY t_bsdt_jzgygcg.CFRWLX ORDER BY total DESC", "teacher_outbound_count", ["teacher_outbound_task_type"], ["t_bsdt_jzgygcg"], "faculty")
    add_golden("teacher_outbound_by_title", "按职称统计教职工因公出国记录数", "SELECT t_bsdt_jzgygcg.ZC AS title_name, COUNT(*) AS total FROM t_bsdt_jzgygcg GROUP BY t_bsdt_jzgygcg.ZC ORDER BY total DESC", "teacher_outbound_count", ["teacher_outbound_title"], ["t_bsdt_jzgygcg"], "faculty")
    add_golden("student_outbound_total", "统计学生因公出国记录数", "SELECT COUNT(*) AS total FROM t_bsdt_xsygcg", "student_outbound_count", [], ["t_bsdt_xsygcg"], "student")
    add_golden("student_outbound_by_year", "按年度统计学生因公出国记录数", "SELECT t_bsdt_xsygcg.NF AS year, COUNT(*) AS total FROM t_bsdt_xsygcg GROUP BY t_bsdt_xsygcg.NF ORDER BY year DESC", "student_outbound_count", ["student_outbound_year"], ["t_bsdt_xsygcg"], "student", ["year"])
    add_golden("student_outbound_by_country", "按国家统计学生因公出国记录数", "SELECT t_bsdt_xsygcg.CFGJHDQ AS country_region, COUNT(*) AS total FROM t_bsdt_xsygcg GROUP BY t_bsdt_xsygcg.CFGJHDQ ORDER BY total DESC", "student_outbound_count", ["student_outbound_country"], ["t_bsdt_xsygcg"], "student")
    add_golden("student_outbound_by_org", "按学院统计学生因公出国记录数", "SELECT dcorg.dwmc AS org_name, COUNT(*) AS total FROM t_bsdt_xsygcg JOIN dcorg ON t_bsdt_xsygcg.DWDM = dcorg.xndwdm GROUP BY dcorg.dwmc ORDER BY total DESC", "student_outbound_count", ["student_outbound_org_name"], ["t_bsdt_xsygcg", "dcorg"], "student")
    add_golden("student_outbound_by_category", "按学生类别统计学生因公出国记录数", "SELECT t_bsdt_xsygcg.CFRWLX AS student_category, COUNT(*) AS total FROM t_bsdt_xsygcg GROUP BY t_bsdt_xsygcg.CFRWLX ORDER BY total DESC", "student_outbound_count", ["student_outbound_category"], ["t_bsdt_xsygcg"], "student")
    add_golden("visiting_expert_total", "统计来访专家人数", "SELECT COUNT(*) AS total FROM t_gjc_lfzj", "visiting_expert_count", [], ["t_gjc_lfzj"], "expert")
    add_golden("visiting_expert_by_year", "按年度统计来访专家人数", "SELECT t_gjc_lfzj.ND AS year, COUNT(*) AS total FROM t_gjc_lfzj GROUP BY t_gjc_lfzj.ND ORDER BY year DESC", "visiting_expert_count", ["visiting_expert_year"], ["t_gjc_lfzj"], "expert", ["year"])
    add_golden("visiting_expert_by_country", "按国家统计来访专家人数", "SELECT t_gjc_lfzj.GJHDQ AS country_region, COUNT(*) AS total FROM t_gjc_lfzj GROUP BY t_gjc_lfzj.GJHDQ ORDER BY total DESC", "visiting_expert_count", ["visiting_expert_country"], ["t_gjc_lfzj"], "expert")
    add_golden("visiting_expert_by_inviter_org", "按邀请单位统计来访专家人数", "SELECT t_gjc_lfzj.YQRDW AS org_name, COUNT(*) AS total FROM t_gjc_lfzj GROUP BY t_gjc_lfzj.YQRDW ORDER BY total DESC", "visiting_expert_count", ["visiting_expert_inviter_org_name"], ["t_gjc_lfzj"], "expert")
    add_golden("visiting_expert_by_submitter_org", "按申报单位统计来访专家人数", "SELECT dcorg.dwmc AS org_name, COUNT(*) AS total FROM t_gjc_lfzj JOIN dcorg ON t_gjc_lfzj.SBRDWBM = dcorg.xndwdm GROUP BY dcorg.dwmc ORDER BY total DESC", "visiting_expert_count", ["visiting_expert_submitter_org_name"], ["t_gjc_lfzj", "dcorg"], "expert")
    add_golden("visiting_expert_by_purpose", "按来访目的统计来访专家人数", "SELECT t_gjc_lfzj.LFMD AS visit_purpose, COUNT(*) AS total FROM t_gjc_lfzj GROUP BY t_gjc_lfzj.LFMD ORDER BY total DESC", "visiting_expert_count", ["visiting_expert_purpose"], ["t_gjc_lfzj"], "expert")
    add_golden("visiting_expert_by_continent", "按洲别统计来访专家人数", "SELECT t_gjc_lfzj.ZB AS continent, COUNT(*) AS total FROM t_gjc_lfzj GROUP BY t_gjc_lfzj.ZB ORDER BY total DESC", "visiting_expert_count", ["visiting_expert_continent"], ["t_gjc_lfzj"], "expert")

    for year in ("2025", "2024"):
        add_golden(f"teacher_outbound_{year}_total", f"{year}年教职工因公出国记录数", f"SELECT COUNT(*) AS total FROM t_bsdt_jzgygcg WHERE t_bsdt_jzgygcg.NF = '{year}'", "teacher_outbound_count", [], ["t_bsdt_jzgygcg"], "faculty", specific_filters=[year])
        add_golden(f"student_outbound_{year}_total", f"{year}年学生因公出国记录数", f"SELECT COUNT(*) AS total FROM t_bsdt_xsygcg WHERE t_bsdt_xsygcg.NF = '{year}'", "student_outbound_count", [], ["t_bsdt_xsygcg"], "student", specific_filters=[year])
        add_golden(f"visiting_expert_{year}_total", f"{year}年来访专家人数", f"SELECT COUNT(*) AS total FROM t_gjc_lfzj WHERE t_gjc_lfzj.ND = '{year}'", "visiting_expert_count", [], ["t_gjc_lfzj"], "expert", specific_filters=[year])
    add_golden("teacher_outbound_2025_by_country", "2025年按国家统计教职工因公出国记录数", "SELECT t_bsdt_jzgygcg.CFGJHDQ AS country_region, COUNT(*) AS total FROM t_bsdt_jzgygcg WHERE t_bsdt_jzgygcg.NF = '2025' GROUP BY t_bsdt_jzgygcg.CFGJHDQ ORDER BY total DESC", "teacher_outbound_count", ["teacher_outbound_country"], ["t_bsdt_jzgygcg"], "faculty", specific_filters=["2025"])
    add_golden("teacher_outbound_2025_by_org", "2025年按学院统计教职工因公出国记录数", "SELECT dcorg.dwmc AS org_name, COUNT(*) AS total FROM t_bsdt_jzgygcg JOIN dcorg ON t_bsdt_jzgygcg.DWDM = dcorg.xndwdm WHERE t_bsdt_jzgygcg.NF = '2025' GROUP BY dcorg.dwmc ORDER BY total DESC", "teacher_outbound_count", ["teacher_outbound_org_name"], ["t_bsdt_jzgygcg", "dcorg"], "faculty", specific_filters=["2025"])
    add_golden("student_outbound_2025_by_country", "2025年按国家统计学生因公出国记录数", "SELECT t_bsdt_xsygcg.CFGJHDQ AS country_region, COUNT(*) AS total FROM t_bsdt_xsygcg WHERE t_bsdt_xsygcg.NF = '2025' GROUP BY t_bsdt_xsygcg.CFGJHDQ ORDER BY total DESC", "student_outbound_count", ["student_outbound_country"], ["t_bsdt_xsygcg"], "student", specific_filters=["2025"])
    add_golden("student_outbound_2025_by_org", "2025年按学院统计学生因公出国记录数", "SELECT dcorg.dwmc AS org_name, COUNT(*) AS total FROM t_bsdt_xsygcg JOIN dcorg ON t_bsdt_xsygcg.DWDM = dcorg.xndwdm WHERE t_bsdt_xsygcg.NF = '2025' GROUP BY dcorg.dwmc ORDER BY total DESC", "student_outbound_count", ["student_outbound_org_name"], ["t_bsdt_xsygcg", "dcorg"], "student", specific_filters=["2025"])
    add_golden("visiting_expert_2025_by_country", "2025年按国家统计来访专家人数", "SELECT t_gjc_lfzj.GJHDQ AS country_region, COUNT(*) AS total FROM t_gjc_lfzj WHERE t_gjc_lfzj.ND = '2025' GROUP BY t_gjc_lfzj.GJHDQ ORDER BY total DESC", "visiting_expert_count", ["visiting_expert_country"], ["t_gjc_lfzj"], "expert", specific_filters=["2025"])
    add_golden("visiting_expert_2025_by_purpose", "2025年按来访目的统计来访专家人数", "SELECT t_gjc_lfzj.LFMD AS visit_purpose, COUNT(*) AS total FROM t_gjc_lfzj WHERE t_gjc_lfzj.ND = '2025' GROUP BY t_gjc_lfzj.LFMD ORDER BY total DESC", "visiting_expert_count", ["visiting_expert_purpose"], ["t_gjc_lfzj"], "expert", specific_filters=["2025"])

    return assets


def upsert_assets(settings: Settings, assets: list[Asset]) -> None:
    client = MilvusClient(uri=settings.milvus_uri, db_name=settings.milvus_database)
    texts = [f"{asset.title}\n{asset.text}\n{json.dumps(asset.metadata, ensure_ascii=False)}" for asset in assets]
    embeddings = embed_texts(settings.embedding_base_url, settings.embedding_model, texts)
    payloads: list[dict[str, Any]] = []
    for asset, embedding in zip(assets, embeddings, strict=True):
        record: dict[str, Any] = {"asset_id": asset.asset_id, "asset_type": asset.asset_type, "title": asset.title, "text": asset.text, "source": asset.source, "metadata": asset.metadata, settings.embedding_vector_name: embedding}
        if asset.asset_type == "golden_sql":
            record["question"] = asset.metadata.get("question", asset.title)
            record["sql"] = asset.metadata.get("sql", "")
            record["notes"] = asset.text
            record["tables"] = asset.metadata.get("tables", [])
        payloads.append(record)
    client.upsert(collection_name=settings.milvus_collection, data=payloads)
    client.load_collection(settings.milvus_collection)
    print(f"upserted_assets={len(payloads)}")
    print(client.get_collection_stats(settings.milvus_collection))
    client.close()


def main() -> None:
    settings = Settings()
    snapshot = fetch_schema_snapshot(settings)
    assets = build_assets(snapshot, settings)
    upsert_assets(settings, assets)


if __name__ == "__main__":
    main()
