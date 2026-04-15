from __future__ import annotations

import site
import sys

import mysql.connector


site.addsitedir(r"D:\LantuConnect\ask_data\.pydeps310")
sys.path.insert(0, r"D:\LantuConnect\ask_data\src")

from ndea.config import Settings


TABLE_COMMENTS = {
    "dcemp": "employee_basic_info",
    "dcorg": "organization_basic_info",
    "dcstu": "student_basic_info",
    "t_bsdt_jzgygcg": "teacher_outbound_visit_records",
    "t_bsdt_xsygcg": "student_outbound_visit_records",
    "t_gjc_lfzj": "visiting_expert_records",
}


COLUMN_COMMENTS = {
    "dcemp": {
        "XB": "gender",
        "XM": "name",
        "SZDWDM": "org_code",
        "SZDWMC": "org_name",
        "XGH": "employee_no",
        "RYZTDM": "staff_status_code",
        "RYZTMC": "staff_status_name",
        "DATA1": "ext_field_1",
        "DATA2": "ext_field_2",
        "data3": "ext_field_3",
        "isenable": "is_enabled",
        "uuid": "global_uuid",
        "orders": "sort_order",
        "author": "created_by",
        "authorname": "created_by_name",
        "created": "created_at",
        "editor": "updated_by",
        "editorname": "updated_by_name",
        "lastupdated": "updated_at",
        "ddid": "source_data_id",
    },
    "dcorg": {
        "wid": "record_id",
        "dwmc": "org_name",
        "xndwdm": "internal_org_code",
        "sfsy": "is_active",
        "data1": "ext_field_1",
        "data2": "ext_field_2",
        "data3": "ext_field_3",
        "isenable": "is_enabled",
        "uuid": "global_uuid",
        "orders": "sort_order",
        "author": "created_by",
        "authorname": "created_by_name",
        "created": "created_at",
        "editor": "updated_by",
        "editorname": "updated_by_name",
        "lastupdated": "updated_at",
        "ddid": "source_data_id",
    },
    "dcstu": {
        "XM": "name",
        "PYCCMC": "training_level_name",
        "SFZX": "is_on_campus",
        "XGH": "student_no",
        "XSLBMC": "student_category_name",
        "ZZMMMC": "political_status_name",
        "YXMC": "college_name",
        "ZYMC": "major_name",
        "XB": "gender",
        "XBDM": "gender_code",
        "XBMC": "gender_name",
        "DATA1": "ext_field_1",
        "DATA2": "ext_field_2",
        "DATA3": "ext_field_3",
        "isenable": "is_enabled",
        "uuid": "global_uuid",
        "orders": "sort_order",
        "author": "created_by",
        "authorname": "created_by_name",
        "created": "created_at",
        "editor": "updated_by",
        "editorname": "updated_by_name",
        "lastupdated": "updated_at",
        "ddid": "source_data_id",
    },
    "t_bsdt_jzgygcg": {
        "WID": "record_id",
        "ID": "business_id",
        "ZGH": "employee_no",
        "XM": "name",
        "DWDM": "org_code",
        "CJR": "created_by",
        "NF": "year",
        "PCDW": "dispatch_org_name",
        "XZZW": "admin_position",
        "ZC": "professional_title",
        "PJH": "approval_no",
        "CFRWLX": "visit_task_type",
        "CFGJHDQ": "destination_country_region",
        "CFNF": "departure_date",
        "CJSJ": "exit_time",
        "RJSJ": "entry_time",
        "YQRDWZWMC": "inviter_org_name_cn",
        "YQRDWYWMC": "inviter_org_name_en",
        "CLRQ": "processed_date",
        "CZLX": "operation_type",
    },
    "t_bsdt_xsygcg": {
        "WID": "record_id",
        "ID": "business_id",
        "ZGH": "student_no_or_staff_no",
        "DWDM": "org_code",
        "NF": "year",
        "XM": "name",
        "CJR": "created_by",
        "PCDW": "dispatch_org_name",
        "PJH": "approval_no",
        "CFRWLX": "visit_task_type",
        "CFGJHDQ": "destination_country_region",
        "CFNF": "departure_date",
        "CJSJ": "exit_time",
        "RJSJ": "entry_time",
        "YQRDWZWMC": "inviter_org_name_cn",
        "YQRDWYWMC": "inviter_org_name_en",
        "CLRQ": "processed_date",
        "CZLX": "operation_type",
    },
    "t_gjc_lfzj": {
        "ID": "business_id",
        "ZJXM": "expert_name",
        "GJHDQ": "country_region",
        "XB": "gender",
        "CSNY": "birth_date",
        "ZJHM": "document_no",
        "ZJYXQ": "document_expiry_date",
        "ZGXL": "highest_education",
        "SSXK": "discipline_name",
        "ZW": "job_position",
        "ZC": "professional_title",
        "QTZC": "other_title",
        "GZDWYW": "employer_name_en",
        "GZDWZW": "employer_name_cn",
        "LXDZ": "contact_address",
        "LXDH": "phone_number",
        "CZ": "fax",
        "DZYJ": "email",
        "ND": "year",
        "YQLFKSSJ": "visit_start_time",
        "YQLFJSSJ": "visit_end_time",
        "LFMD": "visit_purpose",
        "HZLY": "cooperation_source",
        "GCCZJLX": "funding_type",
        "CJGJHYDBSF": "conference_delegate_identity",
        "RJKA": "entry_port",
        "CJKA": "exit_port",
        "ZJYCLFKSRQ": "original_visit_start_date",
        "ZJYCLFJSRQ": "original_visit_end_date",
        "BLQZSZSG": "visa_embassy",
        "ZSDD": "hotel_address",
        "ZSDDDH": "hotel_phone",
        "SFWSFWX": "is_with_external_school",
        "ZQDW": "host_org",
        "SFWZJGMSWC": "is_tax_free_expert",
        "SQGMSWCLY": "tax_free_reason",
        "FYLY": "fund_source",
        "FYJE": "fund_amount",
        "XMMC": "project_name",
        "XMFZR": "project_leader",
        "CWZH": "finance_account",
        "YQRXM": "inviter_name",
        "YQRDW": "inviter_org",
        "ZJGZFS": "expert_work_mode",
        "SBR": "applicant_name",
        "ZB": "continent",
        "SBRGH": "applicant_no",
        "SBRDWBM": "applicant_org_code",
        "YQRGH": "inviter_no",
        "WID": "record_id",
        "CZLX": "operation_type",
        "CLRQ": "processed_date",
    },
}


def main() -> None:
    settings = Settings()
    conn = mysql.connector.connect(
        host=settings.mysql_host,
        port=settings.mysql_port,
        user=settings.mysql_user,
        password=settings.mysql_password,
        database="wenshu_db",
        charset="utf8mb4",
        collation="utf8mb4_0900_ai_ci",
        use_unicode=True,
    )
    cur = conn.cursor()
    cur.execute("SET NAMES utf8mb4 COLLATE utf8mb4_0900_ai_ci")

    cur.execute(
        """
        SELECT table_name, column_name, column_type, is_nullable, column_default, extra
        FROM information_schema.columns
        WHERE table_schema=%s
        ORDER BY table_name, ordinal_position
        """,
        ("wenshu_db",),
    )
    columns = cur.fetchall()

    for table_name, table_comment in TABLE_COMMENTS.items():
        cur.execute(f"ALTER TABLE `{table_name}` COMMENT = %s", (table_comment,))

    for table_name, column_name, column_type, is_nullable, column_default, extra in columns:
        comment = COLUMN_COMMENTS.get(table_name, {}).get(column_name)
        if comment is None:
            continue

        null_sql = "NULL" if is_nullable == "YES" else "NOT NULL"
        default_sql = ""
        if column_default is not None:
            escaped = str(column_default).replace("'", "''")
            default_sql = f" DEFAULT '{escaped}'"
        extra_sql = f" {extra}" if extra else ""
        sql = (
            f"ALTER TABLE `{table_name}` MODIFY COLUMN `{column_name}` {column_type} "
            f"{null_sql}{default_sql}{extra_sql} COMMENT %s"
        )
        cur.execute(sql, (comment,))

    conn.commit()
    cur.close()
    conn.close()
    print("mysql comments repaired")


if __name__ == "__main__":
    main()
