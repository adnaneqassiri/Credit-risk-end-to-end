import json
import logging
from pathlib import Path

import joblib
import numpy as np
import pandas as pd

from src.db.queries import get_generated_client_by_id, insert_generated_client
from src.generator.client_generator import generate_client_json, save_client_json

BASE_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = BASE_DIR.parent.parent
ARTIFACTS_MODEL_DIR = PROJECT_ROOT / "artifacts" / "model"

MODEL_PATH = ARTIFACTS_MODEL_DIR / "lgbm_model.pkl"
FEATURE_DTYPES_PATH = ARTIFACTS_MODEL_DIR / "feature_dtypes.json"
PARAMS_PATH = ARTIFACTS_MODEL_DIR / "params.json"


class ClientNotFoundError(Exception):
    pass


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

    def generate_and_store_client(self) -> dict:
        client_json = generate_client_json()
        insert_generated_client(client_json)
        save_client_json(client_json)
        return client_json

    def get_client_by_id(self, sk_id_curr: int) -> dict:
        client_json = get_generated_client_by_id(sk_id_curr)
        if client_json is None:
            raise ClientNotFoundError(f"Client {sk_id_curr} was not found.")
        return client_json

    def get_risk_class(self, score: float) -> str:
        if score < 0.30:
            return "LOW"
        if score < 0.60:
            return "MEDIUM"
        return "HIGH"

    def prepare_features_df(self, sk_id_curr: int, features: dict) -> pd.DataFrame:
        row = {"SK_ID_CURR": sk_id_curr}
        row.update(features)

        df = pd.DataFrame([row])

        for col in self.features:
            if col not in df.columns:
                df[col] = np.nan

        df = df[["SK_ID_CURR"] + self.features].copy()
        df.replace([np.inf, -np.inf], np.nan, inplace=True)

        for col, dtype in self.feature_dtypes.items():
            if col not in df.columns:
                continue

            try:
                if dtype in ["float64", "float32"]:
                    df[col] = pd.to_numeric(df[col], errors="coerce").astype(dtype)
                elif dtype in ["int64", "int32"]:
                    df[col] = pd.to_numeric(df[col], errors="coerce").astype(dtype)
                elif dtype == "bool":
                    df[col] = df[col].astype("boolean")
                elif dtype == "category":
                    df[col] = df[col].astype("category")
                elif dtype in ["object", "string"]:
                    df[col] = df[col].astype("string")
            except Exception as e:
                logging.warning("Could not cast column %s to %s: %s", col, dtype, e)

        return df

    def predict_by_client_id(self, sk_id_curr: int) -> dict:
        client_json = self.get_client_by_id(sk_id_curr)
        features = client_json.get("features", {})

        df = self.prepare_features_df(sk_id_curr, features)
        x = df.drop(columns=["SK_ID_CURR"])

        score = float(self.model.predict_proba(x)[:, 1][0])
        risk_class = self.get_risk_class(score)

        return {
            "SK_ID_CURR": sk_id_curr,
            "prediction_score": score,
            "risk_class": risk_class,
            "model_version": self.model_version,
        }
