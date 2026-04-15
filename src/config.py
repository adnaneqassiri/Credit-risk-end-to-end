from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = ROOT_DIR / "data"
RAW_DATA_DIR = DATA_DIR / "raw"
INCOMING_DATA_DIR = RAW_DATA_DIR / "incoming"
ARCHIVE_DATA_DIR = RAW_DATA_DIR / "archive"
TRAIN_DATA_DIR = DATA_DIR / "gold"
ARTIFACTS_DIR = ROOT_DIR / "artifacts"
FEATURES_DTYPES_DIR = ROOT_DIR / "artifacts" / "model" / "feature_dtypes.json"
SUBMISSIONS_DIR = DATA_DIR / "submissions"
MODEL_PATH = ARTIFACTS_DIR / "lgbm_model.pkl"



DATABASE_URL = "postgresql://golduser:goldpassword@localhost:5432/mydatabase"