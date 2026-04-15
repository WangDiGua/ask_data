from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(frozen=True)
class CoreFieldDefinition:
    field_id: str
    label: str
    expression: str
    output_alias: str
    aliases: tuple[str, ...]
    table: str
    modes: tuple[str, ...] = ("aggregate", "roster", "detail")
    sample_values: tuple[str, ...] = ()
    value_aliases: dict[str, tuple[str, ...]] = field(default_factory=dict)
    is_time: bool = False


@dataclass(frozen=True)
class CoreTableDefinition:
    table: str
    label: str
    role: str
    aliases: tuple[str, ...]
    identifier_label: str | None = None
    identifier_column: str | None = None
    default_filters: tuple[str, ...] = ()
    default_projection: tuple[str, ...] = ()
    default_sort: tuple[str, ...] = ()
    time_field: str | None = None
    supported_modes: tuple[str, ...] = ("aggregate", "roster", "detail")
    fields: tuple[CoreFieldDefinition, ...] = ()


JOIN_RULES: dict[tuple[str, str], tuple[str, str, str]] = {
    (
        "t_bsdt_jzgygcg",
        "dcorg",
    ): (
        "teacher_outbound_org",
        "JOIN dcorg ON t_bsdt_jzgygcg.DWDM = dcorg.xndwdm",
        "dcorg",
    ),
    (
        "t_bsdt_xsygcg",
        "dcorg",
    ): (
        "student_outbound_org",
        "JOIN dcorg ON t_bsdt_xsygcg.DWDM = dcorg.xndwdm",
        "dcorg",
    ),
    (
        "t_gjc_lfzj",
        "dcorg",
    ): (
        "visiting_expert_submitter_org",
        "JOIN dcorg ON t_gjc_lfzj.SBRDWBM = dcorg.xndwdm",
        "dcorg",
    ),
}


CORE_TABLES: tuple[CoreTableDefinition, ...] = (
    CoreTableDefinition(
        table="dcstu",
        label="在校学生",
        role="person",
        aliases=("学生", "在校学生", "学生信息", "学生名单", "学生明细"),
        identifier_label="学号",
        identifier_column="XGH",
        default_filters=("dcstu.SFZX = '是'",),
        default_projection=(
            "student_no",
            "name",
            "college_name",
            "major_name",
            "level_name",
            "status_name",
        ),
        default_sort=("dcstu.YXMC ASC", "dcstu.XM ASC"),
        supported_modes=("aggregate", "roster", "detail", "attribute"),
        fields=(
            CoreFieldDefinition("student_no", "学号", "dcstu.XGH", "student_no", ("学号", "学工号"), "dcstu", ("roster", "detail")),
            CoreFieldDefinition("name", "姓名", "dcstu.XM", "name", ("姓名", "名字"), "dcstu", ("roster", "detail", "attribute")),
            CoreFieldDefinition(
                "college_name",
                "学院",
                "dcstu.YXMC",
                "college_name",
                ("学院", "院系", "所在学院", "所在院系"),
                "dcstu",
                sample_values=(
                    "烟台研究院",
                    "信息与电气工程学院",
                    "工学院",
                    "人文与发展学院",
                    "经济管理学院",
                    "食品科学与营养工程学院",
                    "农学院",
                    "资源与环境学院",
                    "动物医学院",
                    "动物科学技术学院",
                ),
            ),
            CoreFieldDefinition("major_name", "专业", "dcstu.ZYMC", "major_name", ("专业", "专业名称"), "dcstu"),
            CoreFieldDefinition("level_name", "培养层次", "dcstu.PYCCMC", "level_name", ("培养层次", "层次", "学历层次"), "dcstu"),
            CoreFieldDefinition(
                "status_name",
                "状态",
                "dcstu.SFZX",
                "status_name",
                ("状态", "在校状态", "是否在校"),
                "dcstu",
                sample_values=("是", "否"),
                value_aliases={"是": ("在校", "当前在校", "有效"), "否": ("不在校", "离校")},
            ),
            CoreFieldDefinition("gender_name", "性别", "dcstu.XBMC", "gender_name", ("性别",), "dcstu", sample_values=("男", "女")),
            CoreFieldDefinition(
                "political_status_name",
                "政治面貌",
                "dcstu.ZZMMMC",
                "political_status_name",
                ("政治面貌", "党员", "团员"),
                "dcstu",
                sample_values=(
                    "共青团员",
                    "中国共产党党员",
                    "中国共产主义青年团团员",
                    "群众",
                    "中国共产党预备党员",
                    "中共党员",
                    "中共预备党员",
                ),
                value_aliases={
                    "中国共产党党员": ("党员",),
                    "中共党员": ("党员",),
                    "中国共产党预备党员": ("预备党员",),
                    "中共预备党员": ("预备党员",),
                    "共青团员": ("团员",),
                    "中国共产主义青年团团员": ("团员",),
                },
            ),
            CoreFieldDefinition("student_category_name", "学生类别", "dcstu.XSLBMC", "student_category_name", ("学生类别", "类别"), "dcstu"),
        ),
    ),
    CoreTableDefinition(
        table="dcemp",
        label="在岗教职工",
        role="person",
        aliases=("教师", "老师", "教职工", "在岗教师", "教师名单", "教师明细"),
        identifier_label="工号",
        identifier_column="XGH",
        default_filters=("dcemp.RYZTMC = '在岗'",),
        default_projection=("staff_no", "name", "org_name", "status_name", "gender_name"),
        default_sort=("dcemp.SZDWMC ASC", "dcemp.XM ASC"),
        supported_modes=("aggregate", "roster", "detail", "attribute"),
        fields=(
            CoreFieldDefinition("staff_no", "工号", "dcemp.XGH", "staff_no", ("工号", "教工号", "职工号"), "dcemp", ("roster", "detail")),
            CoreFieldDefinition("name", "姓名", "dcemp.XM", "name", ("姓名", "名字"), "dcemp", ("roster", "detail", "attribute")),
            CoreFieldDefinition(
                "org_name",
                "所在单位",
                "dcemp.SZDWMC",
                "org_name",
                ("所在单位", "所属单位", "单位", "部门"),
                "dcemp",
                sample_values=(
                    "后勤实体",
                    "烟台研究院",
                    "动物医学院",
                    "动物科学技术学院",
                    "生物学院",
                    "农学院",
                    "资源与环境学院",
                    "人文与发展学院",
                    "理学院",
                    "食品科学与营养工程学院",
                ),
            ),
            CoreFieldDefinition(
                "status_name",
                "状态",
                "dcemp.RYZTMC",
                "status_name",
                ("状态", "人员状态", "在岗状态"),
                "dcemp",
                sample_values=("在岗",),
                value_aliases={"在岗": ("当前在岗", "有效教师")},
            ),
            CoreFieldDefinition("gender_name", "性别", "dcemp.XB", "gender_name", ("性别",), "dcemp", sample_values=("男", "女")),
        ),
    ),
    CoreTableDefinition(
        table="dcorg",
        label="组织机构",
        role="organization",
        aliases=("组织机构", "机构", "单位", "部门", "组织"),
        identifier_label="组织编码",
        identifier_column="xndwdm",
        default_projection=("org_code", "org_name", "is_active"),
        default_sort=("dcorg.dwmc ASC",),
        supported_modes=("aggregate", "roster", "detail"),
        fields=(
            CoreFieldDefinition("org_code", "组织编码", "dcorg.xndwdm", "org_code", ("组织编码", "单位编码", "部门编码"), "dcorg", ("roster", "detail")),
            CoreFieldDefinition("org_name", "组织名称", "dcorg.dwmc", "org_name", ("组织名称", "单位名称", "部门名称", "名称"), "dcorg"),
            CoreFieldDefinition(
                "is_active",
                "是否使用",
                "dcorg.sfsy",
                "is_active",
                ("是否使用", "启用状态", "有效状态"),
                "dcorg",
                sample_values=("是", "否"),
                value_aliases={"是": ("启用", "有效"), "否": ("停用", "无效")},
            ),
        ),
    ),
    CoreTableDefinition(
        table="t_bsdt_jzgygcg",
        label="教师因公出国记录",
        role="business_record",
        aliases=("教师出国", "老师出国", "因公出国", "出国记录", "出访记录", "教师出访"),
        identifier_label="工号",
        identifier_column="ZGH",
        default_projection=(
            "name",
            "year",
            "dispatch_org_name",
            "title_name",
            "position_name",
            "mission_type",
            "country_region",
            "depart_date",
            "start_date",
            "return_date",
            "approval_number",
        ),
        default_sort=("t_bsdt_jzgygcg.CFNF DESC", "t_bsdt_jzgygcg.CJSJ DESC"),
        time_field="t_bsdt_jzgygcg.NF",
        supported_modes=("aggregate", "record", "roster", "detail", "attribute"),
        fields=(
            CoreFieldDefinition("staff_no", "工号", "t_bsdt_jzgygcg.ZGH", "staff_no", ("工号", "教工号", "职工号"), "t_bsdt_jzgygcg", ("roster", "detail", "record")),
            CoreFieldDefinition("name", "姓名", "t_bsdt_jzgygcg.XM", "name", ("姓名", "名字"), "t_bsdt_jzgygcg", ("roster", "detail", "record", "attribute")),
            CoreFieldDefinition("year", "年份", "t_bsdt_jzgygcg.NF", "year", ("年份", "年度", "哪一年"), "t_bsdt_jzgygcg", is_time=True),
            CoreFieldDefinition("dispatch_org_name", "派出单位", "t_bsdt_jzgygcg.PCDW", "dispatch_org_name", ("派出单位",), "t_bsdt_jzgygcg"),
            CoreFieldDefinition("title_name", "职称", "t_bsdt_jzgygcg.ZC", "title_name", ("职称",), "t_bsdt_jzgygcg", ("aggregate", "roster", "detail", "attribute")),
            CoreFieldDefinition("position_name", "行政职务", "t_bsdt_jzgygcg.XZZW", "position_name", ("行政职务", "职务"), "t_bsdt_jzgygcg"),
            CoreFieldDefinition(
                "mission_type",
                "出访任务类型",
                "t_bsdt_jzgygcg.CFRWLX",
                "mission_type",
                ("出访任务类型", "任务类型", "出访类型"),
                "t_bsdt_jzgygcg",
                sample_values=(
                    "学术访问",
                    "学术会议",
                    "学术访问;学术会议",
                    "带领学生团组出访",
                    "其他",
                    "管理工作访问",
                    "国家公派（仅限于留基委项目）",
                ),
            ),
            CoreFieldDefinition(
                "country_region",
                "出访国家地区",
                "t_bsdt_jzgygcg.CFGJHDQ",
                "country_region",
                ("国家", "地区", "国家地区", "出访国家", "出访地区"),
                "t_bsdt_jzgygcg",
                sample_values=("美国", "日本", "德国", "英国", "泰国", "韩国", "巴西", "荷兰", "坦桑尼亚", "澳大利亚", "中国-澳门"),
                value_aliases={"中国-澳门": ("澳门",)},
            ),
            CoreFieldDefinition("depart_date", "出发日期", "t_bsdt_jzgygcg.CFNF", "depart_date", ("出发日期",), "t_bsdt_jzgygcg", is_time=True),
            CoreFieldDefinition("start_date", "出境时间", "t_bsdt_jzgygcg.CJSJ", "start_date", ("出境时间", "成行日期"), "t_bsdt_jzgygcg", is_time=True),
            CoreFieldDefinition("return_date", "入境时间", "t_bsdt_jzgygcg.RJSJ", "return_date", ("入境时间", "返回日期"), "t_bsdt_jzgygcg", is_time=True),
            CoreFieldDefinition("host_org_name", "邀请单位", "t_bsdt_jzgygcg.YQRDWZWMC", "host_org_name", ("邀请单位",), "t_bsdt_jzgygcg"),
            CoreFieldDefinition("approval_number", "批件号", "t_bsdt_jzgygcg.PJH", "approval_number", ("批件号",), "t_bsdt_jzgygcg"),
            CoreFieldDefinition("org_name", "组织名称", "dcorg.dwmc", "org_name", ("单位名称", "组织名称"), "dcorg", sample_values=("烟台研究院",)),
        ),
    ),
    CoreTableDefinition(
        table="t_bsdt_xsygcg",
        label="学生因公出国记录",
        role="business_record",
        aliases=("学生出国", "学生出访", "学生出国记录", "学生出访记录"),
        identifier_label="学号",
        identifier_column="ZGH",
        default_projection=(
            "name",
            "year",
            "dispatch_org_name",
            "mission_type",
            "country_region",
            "depart_date",
            "start_date",
            "return_date",
            "approval_number",
        ),
        default_sort=("t_bsdt_xsygcg.CFNF DESC", "t_bsdt_xsygcg.CJSJ DESC"),
        time_field="t_bsdt_xsygcg.NF",
        supported_modes=("aggregate", "record", "roster", "detail"),
        fields=(
            CoreFieldDefinition("student_no", "学号", "t_bsdt_xsygcg.ZGH", "student_no", ("学号", "学工号"), "t_bsdt_xsygcg", ("roster", "detail", "record")),
            CoreFieldDefinition("name", "姓名", "t_bsdt_xsygcg.XM", "name", ("姓名", "名字"), "t_bsdt_xsygcg", ("roster", "detail", "record")),
            CoreFieldDefinition("year", "年份", "t_bsdt_xsygcg.NF", "year", ("年份", "年度", "哪一年"), "t_bsdt_xsygcg", is_time=True),
            CoreFieldDefinition("dispatch_org_name", "派出单位", "t_bsdt_xsygcg.PCDW", "dispatch_org_name", ("派出单位",), "t_bsdt_xsygcg"),
            CoreFieldDefinition(
                "mission_type",
                "出访任务类型",
                "t_bsdt_xsygcg.CFRWLX",
                "mission_type",
                ("出访任务类型", "任务类型", "出访类型"),
                "t_bsdt_xsygcg",
            ),
            CoreFieldDefinition(
                "country_region",
                "出访国家地区",
                "t_bsdt_xsygcg.CFGJHDQ",
                "country_region",
                ("国家", "地区", "国家地区", "出访国家", "出访地区"),
                "t_bsdt_xsygcg",
            ),
            CoreFieldDefinition("depart_date", "出发日期", "t_bsdt_xsygcg.CFNF", "depart_date", ("出发日期",), "t_bsdt_xsygcg", is_time=True),
            CoreFieldDefinition("start_date", "出境时间", "t_bsdt_xsygcg.CJSJ", "start_date", ("出境时间", "成行日期"), "t_bsdt_xsygcg", is_time=True),
            CoreFieldDefinition("return_date", "入境时间", "t_bsdt_xsygcg.RJSJ", "return_date", ("入境时间", "返回日期"), "t_bsdt_xsygcg", is_time=True),
            CoreFieldDefinition("host_org_name", "邀请单位", "t_bsdt_xsygcg.YQRDWZWMC", "host_org_name", ("邀请单位",), "t_bsdt_xsygcg"),
            CoreFieldDefinition("approval_number", "批件号", "t_bsdt_xsygcg.PJH", "approval_number", ("批件号",), "t_bsdt_xsygcg"),
            CoreFieldDefinition("org_name", "组织名称", "dcorg.dwmc", "org_name", ("单位名称", "组织名称"), "dcorg"),
        ),
    ),
    CoreTableDefinition(
        table="t_gjc_lfzj",
        label="来访专家记录",
        role="business_record",
        aliases=("来访专家", "外专", "专家来访", "来访专家记录", "专家名单"),
        identifier_label="来访专家",
        default_projection=(
            "name",
            "country_region",
            "title_name",
            "position_name",
            "visit_purpose",
            "start_date",
            "end_date",
            "org_name",
            "submitter_name",
            "inviter_name",
        ),
        default_sort=("t_gjc_lfzj.YQLFKSSJ DESC",),
        time_field="t_gjc_lfzj.ND",
        supported_modes=("aggregate", "roster", "detail", "record", "attribute"),
        fields=(
            CoreFieldDefinition("name", "专家姓名", "t_gjc_lfzj.ZJXM", "name", ("专家姓名", "姓名", "名字"), "t_gjc_lfzj", ("roster", "detail", "record", "attribute")),
            CoreFieldDefinition("country_region", "国家地区", "t_gjc_lfzj.GJHDQ", "country_region", ("国家", "地区", "国家地区"), "t_gjc_lfzj", sample_values=("美国", "德国", "巴西", "英国", "澳大利亚", "中国", "荷兰", "日本", "加拿大", "韩国")),
            CoreFieldDefinition("gender_name", "性别", "t_gjc_lfzj.XB", "gender_name", ("性别",), "t_gjc_lfzj", sample_values=("男", "女")),
            CoreFieldDefinition("title_name", "职称", "t_gjc_lfzj.ZC", "title_name", ("职称",), "t_gjc_lfzj", ("aggregate", "roster", "detail", "attribute")),
            CoreFieldDefinition("position_name", "职务", "t_gjc_lfzj.ZW", "position_name", ("职务", "行政职务"), "t_gjc_lfzj"),
            CoreFieldDefinition("year", "年份", "t_gjc_lfzj.ND", "year", ("年份", "年度", "哪一年"), "t_gjc_lfzj", is_time=True),
            CoreFieldDefinition(
                "visit_purpose",
                "来访目的",
                "t_gjc_lfzj.LFMD",
                "visit_purpose",
                ("来访目的", "访问目的"),
                "t_gjc_lfzj",
                sample_values=("学术访问", "讲座讲学", "合作研究", "学术会议", "培训进修", "专业教学"),
            ),
            CoreFieldDefinition("start_date", "来访开始时间", "t_gjc_lfzj.YQLFKSSJ", "start_date", ("来访开始时间", "开始时间"), "t_gjc_lfzj", is_time=True),
            CoreFieldDefinition("end_date", "来访结束时间", "t_gjc_lfzj.YQLFJSSJ", "end_date", ("来访结束时间", "结束时间"), "t_gjc_lfzj", is_time=True),
            CoreFieldDefinition("org_name", "主请单位", "t_gjc_lfzj.ZQDW", "org_name", ("主请单位", "邀请单位"), "t_gjc_lfzj"),
            CoreFieldDefinition("submitter_name", "申报人", "t_gjc_lfzj.SBR", "submitter_name", ("申报人",), "t_gjc_lfzj"),
            CoreFieldDefinition("submitter_org_name", "申报单位", "dcorg.dwmc", "submitter_org_name", ("申报单位", "申报人单位"), "dcorg"),
            CoreFieldDefinition("inviter_name", "邀请人", "t_gjc_lfzj.YQRXM", "inviter_name", ("邀请人",), "t_gjc_lfzj"),
        ),
    ),
)


TABLE_BY_NAME = {table.table: table for table in CORE_TABLES}


def get_core_table(table_name: str) -> CoreTableDefinition | None:
    return TABLE_BY_NAME.get(table_name)


def iter_fields(table: CoreTableDefinition) -> tuple[CoreFieldDefinition, ...]:
    return table.fields


def field_by_id(table: CoreTableDefinition, field_id: str) -> CoreFieldDefinition | None:
    for item in table.fields:
        if item.field_id == field_id:
            return item
    return None


def tables_for_query(query_text: str) -> list[CoreTableDefinition]:
    lowered = query_text.lower()
    matched: list[CoreTableDefinition] = []
    for table in CORE_TABLES:
        if any(alias.lower() in lowered for alias in table.aliases):
            matched.append(table)
    return matched


def join_rule(base_table: str, target_table: str) -> tuple[str, str, str] | None:
    return JOIN_RULES.get((base_table, target_table))
