from typing import Any, Dict

from pydantic import BaseModel, Field


class GeneratedClientResponse(BaseModel):
    SK_ID_CURR: int
    features: Dict[str, Any]


class PredictByIdRequest(BaseModel):
    SK_ID_CURR: int = Field(..., ge=456256)


class PredictionResponse(BaseModel):
    SK_ID_CURR: int
    prediction_score: float
    risk_class: str
    model_version: str
