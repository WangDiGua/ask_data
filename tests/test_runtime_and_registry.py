from __future__ import annotations

from pathlib import Path

from ndea.config import Settings
from ndea.planning.core_registry import field_by_id, get_core_table


def test_core_registry_uses_clean_campus_labels() -> None:
    registry_text = Path("src/ndea/planning/core_registry.py").read_text(encoding="utf-8")

    assert "\ufffd" not in registry_text

    student_table = get_core_table("dcstu")
    assert student_table is not None
    assert student_table.label == "在校学生"
    assert "学生" in student_table.aliases
    assert "在校学生" in student_table.aliases
    assert student_table.default_filters == ("dcstu.SFZX = '是'",)

    college_field = field_by_id(student_table, "college_name")
    assert college_field is not None
    assert college_field.label == "学院"
    assert "烟台研究院" in college_field.sample_values

    faculty_table = get_core_table("dcemp")
    assert faculty_table is not None
    assert faculty_table.label == "在岗教职工"
    assert faculty_table.default_filters == ("dcemp.RYZTMC = '在岗'",)


def test_query_service_v2_wires_llamaindex_factory(monkeypatch) -> None:
    import ndea.services.query_service_v2 as service_module

    captured: dict[str, object] = {}

    monkeypatch.setattr(service_module, "open_mysql_connection", lambda settings, database: object())

    class FakeMetadataIntrospector:
        def __init__(self, connection_factory) -> None:
            self.connection_factory = connection_factory

    class FakeSchemaRepository:
        def __init__(self, introspector) -> None:
            self.introspector = introspector

    class FakeNoop:
        def __init__(self, *args, **kwargs) -> None:
            self.args = args
            self.kwargs = kwargs

    class FakeCandidateSQLGenerator:
        def __init__(self, nl2sql_engine) -> None:
            captured["nl2sql_engine"] = nl2sql_engine

    class FakeNL2SQLEngine:
        def __init__(self, enabled: bool, query_engine=None) -> None:
            captured["enabled"] = enabled
            captured["query_engine"] = query_engine

    class FakeGraph:
        def __init__(self, **kwargs) -> None:
            captured["graph_kwargs"] = kwargs

    monkeypatch.setattr(service_module, "MetadataIntrospector", FakeMetadataIntrospector)
    monkeypatch.setattr(service_module, "MySQLSchemaRepository", FakeSchemaRepository)
    monkeypatch.setattr(service_module, "EmbeddingService", FakeNoop)
    monkeypatch.setattr(service_module, "InteractionService", FakeNoop)
    monkeypatch.setattr(service_module, "IntentParser", FakeNoop)
    monkeypatch.setattr(service_module, "CampusSemanticResolver", FakeNoop)
    monkeypatch.setattr(service_module, "SchemaResolver", FakeNoop)
    monkeypatch.setattr(service_module, "CandidatePlanBuilder", FakeNoop)
    monkeypatch.setattr(service_module, "CandidateSQLGenerator", FakeCandidateSQLGenerator)
    monkeypatch.setattr(service_module, "LlamaIndexNL2SQLEngine", FakeNL2SQLEngine)
    monkeypatch.setattr(service_module, "SQLVerifier", FakeNoop)
    monkeypatch.setattr(service_module, "CandidateRanker", FakeNoop)
    monkeypatch.setattr(service_module, "QueryExecutorV2", FakeNoop)
    monkeypatch.setattr(service_module, "MySQLLearningStore", FakeNoop)
    monkeypatch.setattr(service_module, "MilvusLearningSync", FakeNoop)
    monkeypatch.setattr(service_module, "LangfuseTracer", FakeNoop)
    monkeypatch.setattr(service_module, "QueryGraphV2", FakeGraph)
    monkeypatch.setattr(service_module, "get_guarded_query_service", lambda: object())
    monkeypatch.setattr(service_module, "build_llamaindex_query_engine", lambda settings: "factory-engine")

    service_module.QueryServiceV2(Settings(nl2sql_engine="llamaindex"))

    assert captured["enabled"] is True
    assert captured["query_engine"] == "factory-engine"
    assert captured["nl2sql_engine"] is not None
    assert "sql_generator" in captured["graph_kwargs"]
