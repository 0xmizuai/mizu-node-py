from typing import Optional
from psycopg2.extensions import connection
from psycopg2 import sql

from mizu_node.db.common import with_transaction
from mizu_node.types.classifier import ClassifierConfig


@with_transaction
def store_config(db: connection, config: ClassifierConfig) -> int:
    with db.cursor() as cur:
        cur.execute(
            sql.SQL(
                """
                INSERT INTO classifier_config 
                    (name, embedding_model, labels, publisher)
                VALUES (%s, %s, %s::jsonb, %s)
                RETURNING id
                """
            ),
            (
                config.name,
                config.embedding_model,
                config.model_dump_json(include={"labels"}),
                config.publisher,
            ),
        )
        return cur.fetchone()[0]


@with_transaction
def get_config(db: connection, id: int) -> Optional[ClassifierConfig]:
    with db.cursor() as cur:
        cur.execute(
            sql.SQL(
                """
                SELECT name, embedding_model, labels, publisher
                FROM classifier_config
                WHERE id = %s
                """
            ),
            (id,),
        )
        row = cur.fetchone()
        if not row:
            return None

        return ClassifierConfig(
            name=row[0],
            embedding_model=row[1],  # Using alias defined in model
            labels=row[2]["labels"],
            publisher=row[3],
        )


@with_transaction
def list_configs(db: connection, ids: list[int]) -> list[ClassifierConfig]:
    with db.cursor() as cur:
        cur.execute(
            sql.SQL(
                """
                SELECT name, embedding_model, labels, publisher
                FROM classifier_config
                WHERE id = ANY(%s)
                ORDER BY created_at DESC
                """
            ),
            (ids,),
        )
        return [
            ClassifierConfig(
                name=row[0],
                embeddingModel=row[1],
                labels=row[2]["labels"],
                publisher=row[3],
            )
            for row in cur.fetchall()
        ]


@with_transaction
def list_owned_configs(db: connection, publisher: str) -> list[ClassifierConfig]:
    with db.cursor() as cur:
        cur.execute(
            sql.SQL(
                """
                SELECT name, embedding_model, labels, publisher
                FROM classifier_config
                WHERE publisher = %s
                ORDER BY created_at DESC
                """
            ),
            (publisher,),
        )
        return [
            ClassifierConfig(
                name=row[0],
                embeddingModel=row[1],
                labels=row[2]["labels"],
                publisher=row[3],
            )
            for row in cur.fetchall()
        ]


@with_transaction
def delete_config(db: connection, id: int) -> bool:
    with db.cursor() as cur:
        cur.execute(
            sql.SQL(
                """
                DELETE FROM classifier_config
                WHERE id = %s
                """
            ),
            (id,),
        )
        return cur.rowcount > 0
