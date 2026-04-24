import json
import math
from pathlib import Path
from typing import Optional

import pandas as pd
from psycopg import sql

from src.config import FEATURES_DTYPES_DIR
from src.db.connection import get_connection


def load_feature_dtypes(feature_dtypes_path=FEATURES_DTYPES_DIR) -> dict:
    path = Path(feature_dtypes_path)
    candidates = [path]

    if path.name == "feature_dtypes.json":
        candidates.append(path.with_name("features_dtypes.json"))
    elif path.name == "features_dtypes.json":
        candidates.append(path.with_name("feature_dtypes.json"))

    existing_path = next((candidate for candidate in candidates if candidate.exists()), None)
    if existing_path is None:
        searched = ", ".join(str(candidate) for candidate in candidates)
        raise FileNotFoundError(f"Feature dtypes file not found. Searched: {searched}")

    with existing_path.open("r", encoding="utf-8") as f:
        feature_dtypes = json.load(f)

    if not isinstance(feature_dtypes, dict) or not feature_dtypes:
        raise ValueError(f"Feature dtypes file is empty or invalid: {existing_path}")

    return feature_dtypes


def postgres_type_from_pandas_dtype(dtype: str) -> str:
    normalized = str(dtype).lower()

    if normalized in {"category", "object"} or normalized.startswith("string"):
        return "TEXT"
    if normalized.startswith("float"):
        return "DOUBLE PRECISION"
    if normalized.startswith("int") or normalized.startswith("uint"):
        return "BIGINT"
    if normalized in {"bool", "boolean"}:
        return "BOOLEAN"
    if "datetime" in normalized:
        return "TIMESTAMP"

    return "TEXT"


def clean_sql_value(value):
    if value is None:
        return None

    if isinstance(value, float) and math.isinf(value):
        return None

    try:
        if pd.isna(value):
            return None
    except (TypeError, ValueError):
        pass

    if hasattr(value, "item"):
        try:
            return value.item()
        except (TypeError, ValueError):
            return value

    return value


def align_gold_dataframe(df: pd.DataFrame, feature_dtypes: dict) -> pd.DataFrame:
    if "SK_ID_CURR" not in df.columns:
        raise ValueError("Column 'SK_ID_CURR' is required to load the gold table.")

    aligned_df = df.copy()
    if "TARGET" not in aligned_df.columns:
        aligned_df["TARGET"] = pd.NA

    missing_features = [col for col in feature_dtypes if col not in aligned_df.columns]
    for col in missing_features:
        aligned_df[col] = pd.NA

    ordered_columns = ["SK_ID_CURR", "TARGET", *feature_dtypes.keys()]
    aligned_df = aligned_df[ordered_columns]
    aligned_df.replace([float("inf"), float("-inf")], pd.NA, inplace=True)

    return aligned_df


def gold_column_definitions(feature_dtypes: dict, sk_id_primary_key: bool = False) -> list:
    sk_id_definition = "{} BIGINT"
    if sk_id_primary_key:
        sk_id_definition = "{} BIGINT PRIMARY KEY"

    columns = [
        sql.SQL(sk_id_definition).format(sql.Identifier("SK_ID_CURR")),
        sql.SQL("{} DOUBLE PRECISION").format(sql.Identifier("TARGET")),
    ]

    for column_name, dtype in feature_dtypes.items():
        columns.append(
            sql.SQL("{} {}").format(
                sql.Identifier(column_name),
                sql.SQL(postgres_type_from_pandas_dtype(dtype)),
            )
        )

    return columns


def create_gold_table(cursor, feature_dtypes: dict, table_name: str = "gold") -> None:
    cursor.execute(sql.SQL("DROP TABLE IF EXISTS {}").format(sql.Identifier(table_name)))
    cursor.execute(
        sql.SQL("CREATE TABLE {} ({})").format(
            sql.Identifier(table_name),
            sql.SQL(", ").join(gold_column_definitions(feature_dtypes, sk_id_primary_key=True)),
        )
    )


def ensure_gold_table(cursor, feature_dtypes: dict, table_name: str = "gold") -> None:
    cursor.execute(
        sql.SQL("CREATE TABLE IF NOT EXISTS {} ({})").format(
            sql.Identifier(table_name),
            sql.SQL(", ").join(gold_column_definitions(feature_dtypes, sk_id_primary_key=True)),
        )
    )

    cursor.execute(
        sql.SQL("ALTER TABLE {} ADD COLUMN IF NOT EXISTS {} BIGINT").format(
            sql.Identifier(table_name),
            sql.Identifier("SK_ID_CURR"),
        )
    )

    cursor.execute(
        sql.SQL("ALTER TABLE {} ADD COLUMN IF NOT EXISTS {} DOUBLE PRECISION").format(
            sql.Identifier(table_name),
            sql.Identifier("TARGET"),
        )
    )

    for column_name, dtype in feature_dtypes.items():
        cursor.execute(
            sql.SQL("ALTER TABLE {} ADD COLUMN IF NOT EXISTS {} {}").format(
                sql.Identifier(table_name),
                sql.Identifier(column_name),
                sql.SQL(postgres_type_from_pandas_dtype(dtype)),
            )
        )

    cursor.execute(
        sql.SQL("CREATE INDEX IF NOT EXISTS {} ON {} ({})").format(
            sql.Identifier(f"idx_{table_name}_sk_id_curr"),
            sql.Identifier(table_name),
            sql.Identifier("SK_ID_CURR"),
        )
    )


def copy_dataframe_to_table(cursor, df: pd.DataFrame, table_name: str) -> None:
    copy_query = sql.SQL("COPY {} ({}) FROM STDIN").format(
        sql.Identifier(table_name),
        sql.SQL(", ").join(sql.Identifier(col) for col in df.columns),
    )

    with cursor.copy(copy_query) as copy:
        for row in df.itertuples(index=False, name=None):
            copy.write_row([clean_sql_value(value) for value in row])


def insert_data(
    df: pd.DataFrame,
    table_name: str = "gold",
    feature_dtypes_path=FEATURES_DTYPES_DIR,
) -> None:
    feature_dtypes = load_feature_dtypes(feature_dtypes_path)
    gold_df = align_gold_dataframe(df, feature_dtypes)

    with get_connection() as conn:
        with conn.cursor() as cur:
            create_gold_table(cur, feature_dtypes, table_name=table_name)
            copy_dataframe_to_table(cur, gold_df, table_name=table_name)
            cur.execute(
                sql.SQL("CREATE INDEX IF NOT EXISTS {} ON {} ({})").format(
                    sql.Identifier(f"idx_{table_name}_sk_id_curr"),
                    sql.Identifier(table_name),
                    sql.Identifier("SK_ID_CURR"),
                )
            )
        conn.commit()

    print(f"{len(gold_df)} lignes insérées dans '{table_name}'.")


def insert_client_into_gold(
    client_json: dict,
    table_name: str = "gold",
    feature_dtypes_path=FEATURES_DTYPES_DIR,
) -> None:
    feature_dtypes = load_feature_dtypes(feature_dtypes_path)
    features = client_json.get("features", {})
    sk_id_curr = int(client_json["SK_ID_CURR"])

    columns = ["SK_ID_CURR", "TARGET", *feature_dtypes.keys()]
    values = [
        sk_id_curr,
        None,
        *[clean_sql_value(features.get(column_name)) for column_name in feature_dtypes],
    ]

    insert_query = sql.SQL("INSERT INTO {} ({}) VALUES ({})").format(
        sql.Identifier(table_name),
        sql.SQL(", ").join(sql.Identifier(column_name) for column_name in columns),
        sql.SQL(", ").join(sql.Placeholder() for _ in columns),
    )

    with get_connection() as conn:
        with conn.cursor() as cur:
            ensure_gold_table(cur, feature_dtypes, table_name=table_name)
            cur.execute(
                sql.SQL("DELETE FROM {} WHERE {} = %s").format(
                    sql.Identifier(table_name),
                    sql.Identifier("SK_ID_CURR"),
                ),
                (sk_id_curr,),
            )
            cur.execute(insert_query, values)
        conn.commit()


def get_gold_client_by_id(
    sk_id_curr: int,
    table_name: str = "gold",
    feature_dtypes_path=FEATURES_DTYPES_DIR,
) -> Optional[dict]:
    feature_dtypes = load_feature_dtypes(feature_dtypes_path)
    feature_names = list(feature_dtypes.keys())

    query = sql.SQL("SELECT {} FROM {} WHERE {} = %s LIMIT 1").format(
        sql.SQL(", ").join(sql.Identifier(column_name) for column_name in feature_names),
        sql.Identifier(table_name),
        sql.Identifier("SK_ID_CURR"),
    )

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query, (int(sk_id_curr),))
            row = cur.fetchone()

    if row is None:
        return None

    return {
        "SK_ID_CURR": int(sk_id_curr),
        "features": dict(zip(feature_names, row)),
    }


def get_next_sk_id_curr() -> int:
    feature_dtypes = load_feature_dtypes()

    with get_connection() as conn:
        with conn.cursor() as cur:
            ensure_gold_table(cur, feature_dtypes, table_name="gold")
            cur.execute(
                """
                SELECT GREATEST(COALESCE(MAX("SK_ID_CURR"), 456255), 456255) + 1
                FROM gold;
                """
            )
            value = cur.fetchone()[0]
        conn.commit()

    return int(value)
