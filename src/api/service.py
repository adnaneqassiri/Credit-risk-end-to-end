import os
import json
import joblib
import logging
import numpy as np
import pandas as pd

from src.db.queries import insert_predictions_with_features

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.join(BASE_DIR, "..", "..")
ARTIFACTS_MODEL_DIR = os.path.join(PROJECT_ROOT, "artifacts", "model")

MODEL_PATH = os.path.join(ARTIFACTS_MODEL_DIR, "lgbm_model.pkl")
FEATURE_DTYPES_PATH = os.path.join(ARTIFACTS_MODEL_DIR, "feature_dtypes.json")
PARAMS_PATH = os.path.join(ARTIFACTS_MODEL_DIR, "params.json")


class PredictionService:
    def __init__(self):
        self.model = None
        self.feature_dtypes = None
        self.features = None
        self.model_version = "unknown"

    def load(self):
        logging.info("Loading model artifacts...")
        self.model = joblib.load(MODEL_PATH)

        with open(FEATURE_DTYPES_PATH, "r", encoding="utf-8") as f:
            self.feature_dtypes = json.load(f)

        self.features = list(self.feature_dtypes.keys())

        with open(PARAMS_PATH, "r", encoding="utf-8") as f:
            params = json.load(f)

        self.model_version = params.get("model_version", "1.0.0")
        logging.info("Prediction service loaded successfully")

    def get_risk_class(self, score: float) -> str:
        if score < 0.30:
            return "LOW"
        elif score < 0.60:
            return "MEDIUM"
        return "HIGH"

    def prepare_features_df(self, sk_id_curr: int, features: dict) -> pd.DataFrame:
        row = {"SK_ID_CURR": sk_id_curr}
        row.update(features)

        df = pd.DataFrame([row])

        # Add missing columns
        for col in self.features:
            if col not in df.columns:
                df[col] = np.nan

        # Keep exact order
        df = df[["SK_ID_CURR"] + self.features].copy()

        # Clean infinities
        df.replace([np.inf, -np.inf], np.nan, inplace=True)

        # Restore training dtypes
        for col, dtype in self.feature_dtypes.items():
            if col not in df.columns:
                continue

            try:
                if dtype in ["float64", "float32"]:
                    df[col] = pd.to_numeric(df[col], errors="coerce").astype(dtype)
                elif dtype in ["int64", "int32"]:
                    df[col] = pd.to_numeric(df[col], errors="coerce")
                    df[col] = df[col].astype(dtype)
                elif dtype == "bool":
                    df[col] = df[col].astype("boolean")
                elif dtype == "category":
                    df[col] = df[col].astype("category")
                elif dtype == "object":
                    df[col] = df[col].astype("string")
            except Exception as e:
                logging.warning("Could not cast column %s to %s: %s", col, dtype, e)

        return df

    def predict(self, sk_id_curr: int, features: dict, log_to_db: bool = True) -> dict:
        df = self.prepare_features_df(sk_id_curr, features)

        X = df.drop(columns=["SK_ID_CURR"])
        score = float(self.model.predict_proba(X)[:, 1][0])
        risk_class = self.get_risk_class(score)

        submission = pd.DataFrame(
            {
                "SK_ID_CURR": [sk_id_curr],
                "TARGET": [score],
            }
        )

        if log_to_db:
            insert_predictions_with_features(
                submission=submission,
                features_df=df,
                model_version=self.model_version,
                table_name="predictions_log"
            )

        return {
            "SK_ID_CURR": sk_id_curr,
            "prediction_score": score,
            "risk_class": risk_class,
            "model_version": self.model_version,
        }