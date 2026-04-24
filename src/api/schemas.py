from pydantic import BaseModel, Field
from typing import Dict, Any


class PredictionRequest(BaseModel):
    SK_ID_CURR: int = Field(..., gt=456256, lt=999999)
    features: Dict[str, Any] = Field(
        default_factory=dict,
        example={
            "DEBT_RATIO": 0.42,
            "TOTAL_DEBT": 150000,
            "TOTAL_CREDIT": 350000,
            "ACTIVE_LOANS_COUNT": 2
        }
    )


class PredictionResponse(BaseModel):
    SK_ID_CURR: int
    prediction_score: float
    risk_class: str
    model_version: str