from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = ROOT_DIR / "data"
RAW_DATA_DIR = DATA_DIR / "raw"
TRAIN_DATA_DIR = DATA_DIR / "train_data"
ARTIFACTS_DIR = ROOT_DIR / "artifacts"
SUBMISSIONS_DIR = DATA_DIR / "submissions"
MODEL_PATH = ARTIFACTS_DIR / "lgbm_model.pkl"