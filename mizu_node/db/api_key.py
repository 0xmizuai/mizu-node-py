from typing import Optional, List
from datetime import datetime
from psycopg2.extensions import connection
from psycopg2 import sql

from mizu_node.db.common import with_transaction


@with_transaction
def create_api_key(
    db: connection, user_id: str, token: str, description: Optional[str] = None
) -> tuple[str, datetime]:
    """Create a new API key for a user"""
    with db.cursor() as cur:
        cur.execute(
            sql.SQL(
                """
                INSERT INTO api_key (token, user_id, description)
                VALUES (%s, %s, %s)
                RETURNING id
                """
            ),
            (token, user_id, description),
        )
        return cur.fetchone()[0]


@with_transaction
def get_user_id(db: connection, token: str) -> Optional[str]:
    """Get user_id for a given API key if it's active"""
    with db.cursor() as cur:
        cur.execute(
            sql.SQL(
                """
                UPDATE api_key 
                SET last_used_at = CURRENT_TIMESTAMP
                WHERE token = %s AND is_active = TRUE
                RETURNING user_id
                """
            ),
            (token,),
        )
        row = cur.fetchone()
        return row[0] if row else None


@with_transaction
def list_owned_keys(
    db: connection,
    user_id: str,
    include_inactive: bool = False,
    limit: int = 100,
    offset: int = 0,
) -> tuple[List[dict], int]:
    """List API keys for a user with pagination"""
    with db.cursor() as cur:
        # Get total count
        count_query = """
            SELECT COUNT(*) FROM api_key 
            WHERE user_id = %s
        """
        if not include_inactive:
            count_query += " AND is_active = TRUE"
        cur.execute(sql.SQL(count_query), (user_id,))
        total_count = cur.fetchone()[0]

        # Get keys
        query = """
            SELECT token, description, created_at, 
                   last_used_at, is_active
            FROM api_key
            WHERE user_id = %s
        """
        if not include_inactive:
            query += " AND is_active = TRUE"
        query += " ORDER BY created_at DESC LIMIT %s OFFSET %s"

        cur.execute(sql.SQL(query), (user_id, limit, offset))
        keys = [
            {
                "token": row[0],
                "description": row[1],
                "created_at": row[2],
                "last_used_at": row[3],
                "is_active": row[4],
            }
            for row in cur.fetchall()
        ]
        return keys, total_count


@with_transaction
def disable_key(db: connection, token: str, user_id: str) -> bool:
    """Disable an API key (requires user_id for security)"""
    with db.cursor() as cur:
        cur.execute(
            sql.SQL(
                """
                UPDATE api_key
                SET is_active = FALSE
                WHERE token = %s AND user_id = %s
                """
            ),
            (token, user_id),
        )
        return cur.rowcount > 0


@with_transaction
def disable_all_user_keys(db: connection, user_id: str) -> int:
    """Disable all API keys for a user"""
    with db.cursor() as cur:
        cur.execute(
            sql.SQL(
                """
                UPDATE api_key
                SET is_active = FALSE
                WHERE user_id = %s AND is_active = TRUE
                """
            ),
            (user_id,),
        )
        return cur.rowcount


@with_transaction
def get_key_info(db: connection, token: str) -> Optional[dict]:
    """Get detailed information about an API key"""
    with db.cursor() as cur:
        cur.execute(
            sql.SQL(
                """
                SELECT user_id, description, created_at, 
                       last_used_at, is_active
                FROM api_key
                WHERE token = %s
                """
            ),
            (token,),
        )
        row = cur.fetchone()
        if not row:
            return None

        return {
            "user_id": row[0],
            "description": row[1],
            "created_at": row[2],
            "last_used_at": row[3],
            "is_active": row[4],
        }
