import json
import random
from pathlib import Path
from typing import Any, Dict

from src.config import ROOT_DIR
from src.db.queries import get_next_sk_id_curr

ARTIFACTS_MODEL_DIR = ROOT_DIR / "artifacts" / "model"
FEATURE_DTYPES_PATH = ARTIFACTS_MODEL_DIR / "feature_dtypes.json"
GENERATED_CLIENTS_DIR = ROOT_DIR / "data" / "generated_clients"


CATEGORY_VALUES = {
    "NAME_CONTRACT_TYPE": ["Cash loans", "Revolving loans"],
    "CODE_GENDER": ["M", "F"],
    "FLAG_OWN_CAR": ["Y", "N"],
    "FLAG_OWN_REALTY": ["Y", "N"],
    "NAME_TYPE_SUITE": ["Unaccompanied", "Family", "Spouse, partner"],
    "NAME_INCOME_TYPE": ["Working", "Commercial associate", "Pensioner", "State servant"],
    "NAME_EDUCATION_TYPE": ["Secondary / secondary special", "Higher education", "Incomplete higher"],
    "NAME_FAMILY_STATUS": ["Single / not married", "Married", "Civil marriage", "Separated"],
    "NAME_HOUSING_TYPE": ["House / apartment", "With parents", "Municipal apartment", "Rented apartment"],
    "OCCUPATION_TYPE": ["Laborers", "Sales staff", "Core staff", "Managers", "Drivers"],
    "WEEKDAY_APPR_PROCESS_START": ["MONDAY", "TUESDAY", "WEDNESDAY", "THURSDAY", "FRIDAY"],
    "ORGANIZATION_TYPE": ["Business Entity Type 3", "Self-employed", "Government", "School", "Trade"],
}


def load_feature_dtypes() -> Dict[str, str]:
    with open(FEATURE_DTYPES_PATH, "r", encoding="utf-8") as f:
        return json.load(f)


def _is_ratio_feature(feature_name: str) -> bool:
    name = feature_name.upper()
    return "RATIO" in name or "RATE" in name or "PERCENT" in name or "EXT_SOURCE" in name


def _generate_numeric_value(feature_name: str, dtype: str) -> Any:
    name = feature_name.upper()

    if "FLAG" in name:
        return random.randint(0, 1)
    if "EXT_SOURCE" in name or _is_ratio_feature(name):
        return round(random.uniform(0.02, 0.98), 4)
    if "DAYS" in name:
        value = random.randint(-25000, -1)
        return float(value) if dtype in ["float64", "float32"] else value
    if "AMT" in name or "DEBT" in name or "CREDIT" in name or "INCOME" in name:
        return round(random.uniform(5000, 750000), 2)
    if "COUNT" in name or "CNT" in name or "NUMBER" in name or name.endswith("_NUM"):
        return random.randint(0, 8)

    if dtype in ["int64", "int32"]:
        return random.randint(0, 100)

    return round(random.uniform(0, 1000), 4)


def _generate_text_value(feature_name: str) -> str:
    if feature_name in CATEGORY_VALUES:
        return random.choice(CATEGORY_VALUES[feature_name])

    name = feature_name.upper()
    if "GENDER" in name:
        return random.choice(["M", "F"])
    if "CAR" in name or "REALTY" in name:
        return random.choice(["Y", "N"])
    if "CONTRACT" in name:
        return random.choice(["Cash loans", "Revolving loans"])

    return "Unknown"


def generate_feature_value(feature_name: str, dtype: str) -> Any:
    if dtype in ["int64", "int32", "float64", "float32"]:
        return _generate_numeric_value(feature_name, dtype)
    if dtype == "bool":
        return random.choice([True, False])
    if dtype in ["category", "object", "string"]:
        return _generate_text_value(feature_name)

    return None


def generate_client_json() -> dict:
    feature_dtypes = load_feature_dtypes()
    sk_id_curr = get_next_sk_id_curr()

    features = {
        feature_name: generate_feature_value(feature_name, dtype)
        for feature_name, dtype in feature_dtypes.items()
    }

    return {
        "SK_ID_CURR": sk_id_curr,
        "features": features,
    }


def save_client_json(client_json: dict) -> str:
    GENERATED_CLIENTS_DIR.mkdir(parents=True, exist_ok=True)

    sk_id_curr = client_json["SK_ID_CURR"]
    output_path = GENERATED_CLIENTS_DIR / f"client_{sk_id_curr}.json"

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(client_json, f, indent=2, ensure_ascii=False)

    return str(output_path)
