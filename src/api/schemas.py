from pydantic import BaseModel, Field
from typing import Dict, Any


class PredictionRequest(BaseModel):
    SK_ID_CURR: int
    features: Dict[str, Any] = Field(default_factory=dict)


class PredictionResponse(BaseModel):
    SK_ID_CURR: int
    prediction_score: float
    risk_class: str
    model_version: str