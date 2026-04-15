from src.db.connection import get_connection


def create_predictions_table(table_name: str = "predictions_log"):
    query = f"""
    CREATE TABLE IF NOT EXISTS "{table_name}" (
        id BIGSERIAL PRIMARY KEY,
        predicted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        "SK_ID_CURR" BIGINT,
        prediction_score DOUBLE PRECISION,
        risk_class TEXT,
        model_version TEXT,
        input_features JSONB
    );
    """

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query)
        conn.commit()

    print(f"Table '{table_name}' créée avec succès.")


if __name__ == "__main__":
    create_predictions_table()