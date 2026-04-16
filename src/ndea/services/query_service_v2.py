from __future__ import annotations

from ndea.adapters import (
    EmbeddingService,
    LangfuseTracer,
    LlamaIndexNL2SQLEngine,
    build_llamaindex_query_engine,
)
from ndea.config import Settings
from ndea.execution import QueryExecutorV2
from ndea.generation import CandidateSQLGenerator
from ndea.interaction import InteractionService
from ndea.learning import MilvusLearningSync, MySQLLearningStore
from ndea.metadata.introspector import MetadataIntrospector
from ndea.metadata.mysql_client import open_mysql_connection
from ndea.orchestration.query_graph_v2 import QueryGraphV2
from ndea.planning.candidate_plan_builder import CandidatePlanBuilder
from ndea.query_v2 import QueryRequestV2, QueryResponseV2
from ndea.ranking import CandidateRanker
from ndea.resolution import SchemaResolver
from ndea.resolution.mysql_schema_repository import MySQLSchemaRepository
from ndea.semantic.campus_semantic_resolver import CampusSemanticResolver
from ndea.tools.query_executor import get_guarded_query_service
from ndea.understanding import IntentParser
from ndea.verification import SQLVerifier


class QueryServiceV2:
    def __init__(self, settings: Settings | None = None) -> None:
        self._settings = settings or Settings()
        connection_factory = lambda: open_mysql_connection(self._settings, database=self._settings.mysql_database or "")
        introspector = MetadataIntrospector(connection_factory=connection_factory)
        schema_repository = MySQLSchemaRepository(introspector)
        self._embedding_service = EmbeddingService(self._settings)
        llamaindex_query_engine = build_llamaindex_query_engine(self._settings)
        self._graph = QueryGraphV2(
            interaction_service=InteractionService(),
            intent_parser=IntentParser(),
            semantic_resolver=CampusSemanticResolver(),
            schema_resolver=SchemaResolver(schema_repository),
            plan_builder=CandidatePlanBuilder(),
            sql_generator=CandidateSQLGenerator(
                nl2sql_engine=LlamaIndexNL2SQLEngine(
                    enabled=self._settings.nl2sql_engine.lower() == "llamaindex",
                    query_engine=llamaindex_query_engine,
                )
            ),
            sql_verifier=SQLVerifier(schema_repository=schema_repository),
            ranker=CandidateRanker(),
            executor=QueryExecutorV2(get_guarded_query_service()),
            learning_store=MySQLLearningStore(self._settings),
            milvus_sync=MilvusLearningSync(),
            tracer=LangfuseTracer(self._settings),
        )

    def run(self, request: QueryRequestV2) -> QueryResponseV2:
        self._embedding_service.embed_query(request.query_text)
        return self._graph.run(request)

    def stream(self, request: QueryRequestV2):
        self._embedding_service.embed_query(request.query_text)
        return self._graph.stream(request)
