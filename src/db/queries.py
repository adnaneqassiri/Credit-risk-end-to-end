import json
import math
import pandas as pd
from src.db.connection import get_connection


def get_risk_class(score: float) -> str:
    if score < 0.30:
        return "LOW"
    elif score < 0.60:
        return "MEDIUM"
    return "HIGH"


def clean_json_value(value):
    if pd.isna(value):
        return None

    if isinstance(value, float):
        if math.isinf(value) or math.isnan(value):
            return None

    return value


def insert_predictions_with_features(
    submission: pd.DataFrame,
    features_df: pd.DataFrame,
    model_version: str,
    table_name: str = "predictions_log"
):
    """
    submission must contain:
    - SK_ID_CURR
    - TARGET

    features_df must contain:
    - SK_ID_CURR
    - all feature columns used for inference
    """

    if submission.empty:
        print("Submission vide, rien à insérer.")
        return

    required_submission_cols = {"SK_ID_CURR", "TARGET"}
    missing_submission_cols = required_submission_cols - set(submission.columns)
    if missing_submission_cols:
        raise ValueError(f"Colonnes manquantes dans submission: {missing_submission_cols}")

    if "SK_ID_CURR" not in features_df.columns:
        raise ValueError("La colonne 'SK_ID_CURR' doit exister dans features_df.")

    merged_df = submission.merge(features_df, on="SK_ID_CURR", how="left")

    values = []
    for _, row in merged_df.iterrows():
        sk_id_curr = int(row["SK_ID_CURR"])
        prediction_score = float(row["TARGET"])
        risk_class = get_risk_class(prediction_score)

        input_features = row.drop(labels=["SK_ID_CURR", "TARGET"]).to_dict()

        cleaned_input_features = {
            k: clean_json_value(v)
            for k, v in input_features.items()
        }

        values.append(
            (
                sk_id_curr,
                prediction_score,
                risk_class,
                model_version,
                json.dumps(cleaned_input_features, default=str),
            )
        )

    query = f"""
        INSERT INTO "{table_name}"
        ("SK_ID_CURR", prediction_score, risk_class, model_version, input_features)
        VALUES (%s, %s, %s, %s, %s::jsonb);
    """

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.executemany(query, values)
        conn.commit()

    print(f"{len(values)} lignes insérées dans '{table_name}'.")


def get_training_data():
    pass


def get_client(client_id):
    pass