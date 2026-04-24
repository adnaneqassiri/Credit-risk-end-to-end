from src.db.connection import get_connection
from src.db.queries import ensure_gold_table, load_feature_dtypes


def create_gold_table_if_not_exists() -> None:
    feature_dtypes = load_feature_dtypes()

    with get_connection() as conn:
        with conn.cursor() as cur:
            ensure_gold_table(cur, feature_dtypes, table_name="gold")
            cur.execute("DROP TABLE IF EXISTS generated_clients;")
            cur.execute("DROP SEQUENCE IF EXISTS sk_id_curr_seq;")
        conn.commit()

    print("Table 'gold' is ready. Legacy generated_clients storage was removed if it existed.")


if __name__ == "__main__":
    create_gold_table_if_not_exists()
