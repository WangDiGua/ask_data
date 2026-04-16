from __future__ import annotations

import json
from collections.abc import Callable
from typing import Any

from ndea.config import Settings
from ndea.query_v2 import LearningEvent, PromotionCandidate, QueryResponseV2


class LearningStore:
    def record(
        self,
        response: QueryResponseV2,
        feedback_events: list[LearningEvent] | None = None,
    ) -> tuple[list[LearningEvent], list[PromotionCandidate]]:
        raise NotImplementedError


class MySQLLearningStore(LearningStore):
    def __init__(
        self,
        settings: Settings,
        connection_factory: Callable[[str], Any] | None = None,
    ) -> None:
        self._settings = settings
        self._uses_default_connection_factory = connection_factory is None
        self._connection_factory = connection_factory or self._default_connection_factory
        self._schema_ready = False

    def record(
        self,
        response: QueryResponseV2,
        feedback_events: list[LearningEvent] | None = None,
    ) -> tuple[list[LearningEvent], list[PromotionCandidate]]:
        events = [
            LearningEvent(
                event_type="query_session_recorded",
                session_id=response.session_id,
                payload={
                    "confidence": response.confidence,
                    "sql": response.sql,
                    "executed": response.executed,
                    "clarification_required": response.clarification.required,
                },
            )
        ]
        if response.clarification.required:
            events.append(
                LearningEvent(
                    event_type="clarification_requested",
                    session_id=response.session_id,
                    payload={
                        "question": response.clarification.question,
                        "reason": response.clarification.reason,
                    },
                )
            )
        if feedback_events:
            events.extend(feedback_events)
        promotions = self._promotions_for(response)
        return events, promotions

    def bootstrap_schema(self) -> list[str]:
        database = self._settings.learning_mysql_database
        return [
            f"CREATE DATABASE IF NOT EXISTS `{database}` CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci",
            f"CREATE TABLE IF NOT EXISTS `{database}`.`query_session` (session_id VARCHAR(64) PRIMARY KEY, payload JSON NOT NULL, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP)",
            f"CREATE TABLE IF NOT EXISTS `{database}`.`interaction_turn` (id BIGINT AUTO_INCREMENT PRIMARY KEY, session_id VARCHAR(64) NOT NULL, payload JSON NOT NULL, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, KEY idx_interaction_turn_session_id (session_id))",
            f"CREATE TABLE IF NOT EXISTS `{database}`.`ir_snapshot` (id BIGINT AUTO_INCREMENT PRIMARY KEY, session_id VARCHAR(64) NOT NULL, payload JSON NOT NULL, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, KEY idx_ir_snapshot_session_id (session_id))",
            f"CREATE TABLE IF NOT EXISTS `{database}`.`plan_candidate` (id BIGINT AUTO_INCREMENT PRIMARY KEY, session_id VARCHAR(64) NOT NULL, payload JSON NOT NULL, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, KEY idx_plan_candidate_session_id (session_id))",
            f"CREATE TABLE IF NOT EXISTS `{database}`.`sql_candidate` (id BIGINT AUTO_INCREMENT PRIMARY KEY, session_id VARCHAR(64) NOT NULL, payload JSON NOT NULL, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, KEY idx_sql_candidate_session_id (session_id))",
            f"CREATE TABLE IF NOT EXISTS `{database}`.`execution_result` (id BIGINT AUTO_INCREMENT PRIMARY KEY, session_id VARCHAR(64) NOT NULL, payload JSON NOT NULL, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, KEY idx_execution_result_session_id (session_id))",
            f"CREATE TABLE IF NOT EXISTS `{database}`.`feedback_event` (id BIGINT AUTO_INCREMENT PRIMARY KEY, session_id VARCHAR(64) NOT NULL, payload JSON NOT NULL, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, KEY idx_feedback_event_session_id (session_id))",
            f"CREATE TABLE IF NOT EXISTS `{database}`.`alias_memory` (id BIGINT AUTO_INCREMENT PRIMARY KEY, canonical_key VARCHAR(255) NOT NULL, payload JSON NOT NULL, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, KEY idx_alias_memory_canonical_key (canonical_key))",
            f"CREATE TABLE IF NOT EXISTS `{database}`.`value_synonym_memory` (id BIGINT AUTO_INCREMENT PRIMARY KEY, canonical_key VARCHAR(255) NOT NULL, payload JSON NOT NULL, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, KEY idx_value_synonym_memory_canonical_key (canonical_key))",
            f"CREATE TABLE IF NOT EXISTS `{database}`.`clarification_memory` (id BIGINT AUTO_INCREMENT PRIMARY KEY, canonical_key VARCHAR(255) NOT NULL, payload JSON NOT NULL, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, KEY idx_clarification_memory_canonical_key (canonical_key))",
            f"CREATE TABLE IF NOT EXISTS `{database}`.`sql_case_memory` (id BIGINT AUTO_INCREMENT PRIMARY KEY, canonical_key VARCHAR(255) NOT NULL, payload JSON NOT NULL, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, KEY idx_sql_case_memory_canonical_key (canonical_key))",
            f"CREATE TABLE IF NOT EXISTS `{database}`.`promotion_queue` (id BIGINT AUTO_INCREMENT PRIMARY KEY, session_id VARCHAR(64) NOT NULL, payload JSON NOT NULL, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, KEY idx_promotion_queue_session_id (session_id))",
        ]

    def ensure_schema(self) -> None:
        if self._schema_ready:
            return
        connection = self._open_admin_connection()
        try:
            with connection.cursor() as cursor:
                for statement in self.bootstrap_schema():
                    cursor.execute(statement)
            connection.commit()
            self._schema_ready = True
        finally:
            connection.close()

    def persist_response(
        self,
        response: QueryResponseV2,
        events: list[LearningEvent] | None = None,
        promotions: list[PromotionCandidate] | None = None,
    ) -> None:
        self.ensure_schema()
        database = self._settings.learning_mysql_database
        events = events or []
        promotions = promotions or self._promotions_for(response)
        payloads = response.model_dump(mode="json")
        connection = self._open_learning_connection(database)
        try:
            with connection.cursor() as cursor:
                cursor.execute(
                    f"INSERT INTO `{database}`.`query_session` (session_id, payload) VALUES (%s, %s) "
                    "ON DUPLICATE KEY UPDATE payload = VALUES(payload)",
                    (response.session_id, self._json(payloads)),
                )
                for table_name in (
                    "interaction_turn",
                    "ir_snapshot",
                    "plan_candidate",
                    "sql_candidate",
                    "execution_result",
                    "feedback_event",
                    "promotion_queue",
                ):
                    cursor.execute(
                        f"DELETE FROM `{database}`.`{table_name}` WHERE session_id = %s",
                        (response.session_id,),
                    )

                cursor.execute(
                    f"INSERT INTO `{database}`.`interaction_turn` (session_id, payload) VALUES (%s, %s)",
                    (
                        response.session_id,
                        self._json(response.interpretation.interaction.model_dump(mode="json")),
                    ),
                )
                cursor.execute(
                    f"INSERT INTO `{database}`.`ir_snapshot` (session_id, payload) VALUES (%s, %s)",
                    (
                        response.session_id,
                        self._json(response.interpretation.ir.model_dump(mode="json")),
                    ),
                )

                for item in self._plan_payloads(response):
                    cursor.execute(
                        f"INSERT INTO `{database}`.`plan_candidate` (session_id, payload) VALUES (%s, %s)",
                        (response.session_id, self._json(item)),
                    )
                for item in self._sql_payloads(response):
                    cursor.execute(
                        f"INSERT INTO `{database}`.`sql_candidate` (session_id, payload) VALUES (%s, %s)",
                        (response.session_id, self._json(item)),
                    )
                cursor.execute(
                    f"INSERT INTO `{database}`.`execution_result` (session_id, payload) VALUES (%s, %s)",
                    (
                        response.session_id,
                        self._json(
                            {
                                "executed": response.executed,
                                "sql": response.sql,
                                "audit": response.audit,
                                "answer": response.answer.model_dump(mode="json"),
                                "table": response.table.model_dump(mode="json") if response.table else None,
                                "clarification": response.clarification.model_dump(mode="json"),
                            }
                        ),
                    ),
                )
                for event in events:
                    cursor.execute(
                        f"INSERT INTO `{database}`.`feedback_event` (session_id, payload) VALUES (%s, %s)",
                        (response.session_id, self._json(event.model_dump(mode="json"))),
                    )
                for promotion in promotions:
                    cursor.execute(
                        f"INSERT INTO `{database}`.`promotion_queue` (session_id, payload) VALUES (%s, %s)",
                        (response.session_id, self._json(promotion.model_dump(mode="json"))),
                    )
                self._persist_memory_assets(cursor, database, response, events, promotions)
            connection.commit()
        finally:
            connection.close()

    def _default_connection_factory(self, database: str) -> Any:
        from ndea.metadata.mysql_client import open_mysql_connection

        return open_mysql_connection(self._settings, database=database)

    def _open_admin_connection(self) -> Any:
        if not self._uses_default_connection_factory:
            admin_database = self._settings.mysql_database or self._settings.learning_mysql_database
            return self._connection_factory(admin_database)
        try:
            import mysql.connector
            from ndea.metadata.mysql_client import build_mysql_connect_kwargs

            return mysql.connector.connect(
                **build_mysql_connect_kwargs(
                    self._settings,
                    database=self._settings.mysql_database or None,
                )
            )
        except Exception:
            admin_database = self._settings.mysql_database or self._settings.learning_mysql_database
            return self._connection_factory(admin_database)

    def _open_learning_connection(self, database: str) -> Any:
        if not self._uses_default_connection_factory:
            return self._connection_factory(database)
        try:
            import mysql.connector
            from ndea.metadata.mysql_client import build_mysql_connect_kwargs

            return mysql.connector.connect(
                **build_mysql_connect_kwargs(
                    self._settings,
                    database=database,
                )
            )
        except Exception:
            return self._connection_factory(database)

    def _plan_payloads(self, response: QueryResponseV2) -> list[dict[str, Any]]:
        debug_plans = response.debug.get("plans")
        if isinstance(debug_plans, list) and debug_plans:
            return [item for item in debug_plans if isinstance(item, dict)]
        if response.interpretation.selected_plan is None:
            return []
        return [response.interpretation.selected_plan.model_dump(mode="json")]

    def _sql_payloads(self, response: QueryResponseV2) -> list[dict[str, Any]]:
        debug_sql_candidates = response.debug.get("sql_candidates")
        if isinstance(debug_sql_candidates, list) and debug_sql_candidates:
            return [item for item in debug_sql_candidates if isinstance(item, dict)]
        if response.interpretation.selected_sql is None:
            return []
        return [response.interpretation.selected_sql.model_dump(mode="json")]

    def _json(self, payload: Any) -> str:
        return json.dumps(payload, ensure_ascii=False)

    def _promotions_for(self, response: QueryResponseV2) -> list[PromotionCandidate]:
        promotions: list[PromotionCandidate] = []
        if response.confidence >= 0.9 and response.sql:
            promotions.append(
                PromotionCandidate(
                    promotion_type="sql_case",
                    session_id=response.session_id,
                    confidence=response.confidence,
                    payload={
                        "query_text": response.interpretation.interaction.query_text,
                        "sql": response.sql,
                        "confidence": response.confidence,
                    },
                )
            )
        interpretation = response.interpretation
        if interpretation.ir.entity_scope:
            promotions.append(
                PromotionCandidate(
                    promotion_type="alias_memory",
                    session_id=response.session_id,
                    confidence=max(0.6, response.confidence),
                    payload={
                        "canonical_key": interpretation.ir.entity_scope,
                        "query_text": interpretation.interaction.query_text,
                        "rewritten_query_text": interpretation.interaction.rewritten_query_text,
                    },
                )
            )
        if interpretation.ir.filters:
            for item in interpretation.ir.filters:
                promotions.append(
                    PromotionCandidate(
                        promotion_type="value_synonym",
                        session_id=response.session_id,
                        confidence=max(0.55, response.confidence),
                        payload={
                            "canonical_key": item,
                            "query_text": interpretation.interaction.query_text,
                            "entity_scope": interpretation.ir.entity_scope,
                        },
                    )
                )
        if response.clarification.required and response.clarification.question:
            promotions.append(
                PromotionCandidate(
                    promotion_type="clarification_pattern",
                    session_id=response.session_id,
                    confidence=max(0.5, response.confidence),
                    payload={
                        "canonical_key": response.clarification.reason or "clarification",
                        "question": response.clarification.question,
                        "query_text": interpretation.interaction.query_text,
                    },
                )
            )
        return promotions

    def _persist_memory_assets(
        self,
        cursor: Any,
        database: str,
        response: QueryResponseV2,
        events: list[LearningEvent],
        promotions: list[PromotionCandidate],
    ) -> None:
        inserted_keys: set[tuple[str, str]] = set()
        for promotion in promotions:
            target = self._memory_target_for_promotion(promotion)
            if target is None:
                continue
            table_name, canonical_key = target
            dedupe_key = (table_name, canonical_key)
            if dedupe_key in inserted_keys:
                continue
            inserted_keys.add(dedupe_key)
            cursor.execute(
                f"INSERT INTO `{database}`.`{table_name}` (canonical_key, payload) VALUES (%s, %s)",
                (
                    canonical_key,
                    self._json(
                        {
                            "session_id": response.session_id,
                            "confidence": promotion.confidence,
                            "payload": promotion.payload,
                        }
                    ),
                ),
            )

        if response.sql and response.confidence >= 0.9:
            cursor.execute(
                f"INSERT INTO `{database}`.`sql_case_memory` (canonical_key, payload) VALUES (%s, %s)",
                (
                    response.interpretation.interaction.query_text,
                    self._json(
                        {
                            "session_id": response.session_id,
                            "sql": response.sql,
                            "confidence": response.confidence,
                            "answer": response.answer.model_dump(mode="json"),
                        }
                    ),
                ),
            )

        if response.clarification.required and response.clarification.question:
            cursor.execute(
                f"INSERT INTO `{database}`.`clarification_memory` (canonical_key, payload) VALUES (%s, %s)",
                (
                    response.clarification.reason or "clarification",
                    self._json(
                        {
                            "session_id": response.session_id,
                            "question": response.clarification.question,
                            "events": [event.model_dump(mode='json') for event in events],
                        }
                    ),
                ),
            )

    def _memory_target_for_promotion(self, promotion: PromotionCandidate) -> tuple[str, str] | None:
        canonical_key = str(promotion.payload.get("canonical_key") or "").strip()
        if promotion.promotion_type == "alias_memory" and canonical_key:
            return "alias_memory", canonical_key
        if promotion.promotion_type == "value_synonym" and canonical_key:
            return "value_synonym_memory", canonical_key
        if promotion.promotion_type == "clarification_pattern" and canonical_key:
            return "clarification_memory", canonical_key
        if promotion.promotion_type == "sql_case" and canonical_key:
            return "sql_case_memory", canonical_key
        return None
