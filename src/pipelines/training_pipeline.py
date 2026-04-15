import os
import json
import joblib
import logging
import pandas as pd

from src.model import train_model
# from src.data.selection import drop_unused_columns
from src.config import TRAIN_DATA_DIR

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
ARTIFACTS_MODEL_DIR = os.path.join(BASE_DIR, "..", "..", "artifacts", "model")
ARTIFACTS_REPORTS_DIR = os.path.join(BASE_DIR, "..", "..", "artifacts", "reports")


def save_json(data, path: str) -> None:
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=4, ensure_ascii=False)


def main():
    logging.info("Starting training pipeline")

    train_path = os.path.join(TRAIN_DATA_DIR, "df_train_final.parquet")
    logging.info("Loading training data from %s", train_path)

    df_train = pd.read_parquet(train_path)
    logging.info("Training data loaded with shape: %s", df_train.shape)

    # logging.info("Dropping unused columns")
    # df_train = drop_unused_columns(df_train)
    # logging.info("Shape after dropping columns: %s", df_train.shape)

    required_cols = ["TARGET", "SK_ID_CURR"]
    missing_required = [col for col in required_cols if col not in df_train.columns]
    if missing_required:
        raise ValueError(f"Missing required columns in training data: {missing_required}")

    os.makedirs(ARTIFACTS_MODEL_DIR, exist_ok=True)
    os.makedirs(ARTIFACTS_REPORTS_DIR, exist_ok=True)

    logging.info("Starting model training")
    result = train_model(
        df_train=df_train,
        roc_train_plot_path=os.path.join(ARTIFACTS_REPORTS_DIR, "roc_train.png"),
        roc_val_plot_path=os.path.join(ARTIFACTS_REPORTS_DIR, "roc_validation.png"),
    )
    logging.info("Model training completed")

    if "model" not in result:
        raise ValueError("train_model(df_train) must return a dict containing at least the key 'model'.")

    model = result["model"]

    # Save model
    model_path = os.path.join(ARTIFACTS_MODEL_DIR, "lgbm_model.pkl")
    joblib.dump(model, model_path)
    logging.info("Model saved at %s", model_path)

    # Save training features
    features = result.get("features", df_train.drop(columns=["TARGET", "SK_ID_CURR"]).columns.tolist())
    features_path = os.path.join(ARTIFACTS_MODEL_DIR, "features.json")
    save_json(features, features_path)
    logging.info("Features saved at %s", features_path)

    # Save feature dtypes
    feature_df = df_train[features].copy()
    feature_dtypes = {col: str(dtype) for col, dtype in feature_df.dtypes.items()}
    feature_dtypes_path = os.path.join(ARTIFACTS_MODEL_DIR, "feature_dtypes.json")
    save_json(feature_dtypes, feature_dtypes_path)
    logging.info("Feature dtypes saved at %s", feature_dtypes_path)


    # Save metrics
    metrics = {
        "auc_train": result.get("auc_train"),
        "auc_val": result.get("auc_val"),
    }
    metrics_path = os.path.join(ARTIFACTS_MODEL_DIR, "metrics.json")
    save_json(metrics, metrics_path)
    logging.info("Metrics saved at %s", metrics_path)

    # Save params / metadata
    params = {
        "model_name": "LightGBM",
        "model_version": "1.0.0",
        "train_rows": int(df_train.shape[0]),
        "train_cols_after_selection": int(df_train.shape[1]),
        "n_features_used": len(features),
        "model_params": result.get("params", {}),
    }
    params_path = os.path.join(ARTIFACTS_MODEL_DIR, "params.json")
    save_json(params, params_path)
    logging.info("Params saved at %s", params_path)

    logging.info("Training pipeline finished successfully")


if __name__ == "__main__":
    main()