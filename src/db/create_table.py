import json
from src.db.connection import get_connection
from src.config import FEATURES_DTYPES_DIR

PANDAS_TO_SQL = {
    "int64": "BIGINT",
    "int32": "INT",
    "float64": "DOUBLE PRECISION",
    "float32": "REAL",
    "object": "TEXT",
    "category": "TEXT",
    "bool": "BOOLEAN",
    "datetime64[ns]": "TIMESTAMP",
}


def generate_create_predictions_table_sql(json_path, table_name) -> str:
    with open(json_path, "r", encoding="utf-8") as f:
        feature_dtypes = json.load(f)

    columns_sql = [
        'id BIGSERIAL PRIMARY KEY',
        'predicted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP',
        '"SK_ID_CURR" BIGINT',
        'prediction_score DOUBLE PRECISION',
        'risk_class TEXT',
        'model_version TEXT'
    ]

    for col, dtype in feature_dtypes.items():
        if col == "SK_ID_CURR":
            continue
        sql_type = PANDAS_TO_SQL.get(dtype, "TEXT")
        columns_sql.append(f'"{col}" {sql_type}')

    sql = f'''
    CREATE TABLE IF NOT EXISTS "{table_name}" (
        {", ".join(columns_sql)}
    );
    '''
    return sql


def create_table(json_path: str, table_name: str):
    create_sql = generate_create_predictions_table_sql(json_path, table_name)

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(create_sql)
        conn.commit()

    print(f"Table '{table_name}' créée avec succès.")


if __name__ == "__main__":
    create_table(FEATURES_DTYPES_DIR, "predictions_log")