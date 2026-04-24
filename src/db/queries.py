from typing import Optional

from psycopg.types.json import Jsonb

from src.db.connection import get_connection


def get_next_sk_id_curr() -> int:
    query = "SELECT nextval('sk_id_curr_seq');"

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query)
            value = cur.fetchone()[0]

    return int(value)


def insert_generated_client(client_json: dict) -> None:
    query = """
    INSERT INTO generated_clients ("SK_ID_CURR", client_data)
    VALUES (%s, %s)
    ON CONFLICT ("SK_ID_CURR") DO UPDATE
    SET client_data = EXCLUDED.client_data;
    """

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                query,
                (
                    int(client_json["SK_ID_CURR"]),
                    Jsonb(client_json),
                ),
            )
        conn.commit()


def get_generated_client_by_id(sk_id_curr: int) -> Optional[dict]:
    query = """
    SELECT client_data
    FROM generated_clients
    WHERE "SK_ID_CURR" = %s;
    """

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query, (int(sk_id_curr),))
            row = cur.fetchone()

    return row[0] if row else None


def list_recent_generated_clients(limit: int = 10) -> list[dict]:
    query = """
    SELECT "SK_ID_CURR", created_at, client_data
    FROM generated_clients
    ORDER BY created_at DESC
    LIMIT %s;
    """

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query, (int(limit),))
            rows = cur.fetchall()

    return [
        {
            "SK_ID_CURR": row[0],
            "created_at": row[1],
            "client_data": row[2],
        }
        for row in rows
    ]
