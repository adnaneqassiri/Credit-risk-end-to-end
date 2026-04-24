from src.db.connection import get_connection


def create_generated_clients_table():
    sequence_query = """
    CREATE SEQUENCE IF NOT EXISTS sk_id_curr_seq
    START 456256
    INCREMENT 1;
    """

    table_query = """
    CREATE TABLE IF NOT EXISTS generated_clients (
        id BIGSERIAL PRIMARY KEY,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        "SK_ID_CURR" BIGINT UNIQUE NOT NULL,
        client_data JSONB NOT NULL
    );
    """

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sequence_query)
            cur.execute(table_query)
        conn.commit()

    print("Sequence 'sk_id_curr_seq' and table 'generated_clients' are ready.")


if __name__ == "__main__":
    create_generated_clients_table()
