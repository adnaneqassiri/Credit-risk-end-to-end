import logging
from fastapi import FastAPI, HTTPException

from src.api.schemas import PredictionRequest, PredictionResponse
from src.api.service import PredictionService

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

app = FastAPI(
    title="Credit Risk Prediction API",
    version="1.0.0",
    description="API for credit default risk prediction using LightGBM"
)

prediction_service = PredictionService()


@app.on_event("startup")
def startup_event():
    prediction_service.load()


@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/model/info")
def model_info():
    return {
        "model_name": "LightGBM",
        "model_version": prediction_service.model_version,
        "n_features": len(prediction_service.features) if prediction_service.features else 0,
    }


@app.post("/predict", response_model=PredictionResponse)
def predict(request: PredictionRequest):
    try:
        result = prediction_service.predict(
            sk_id_curr=request.SK_ID_CURR,
            features=request.features,
            log_to_db=True,
        )
        return result
    except Exception as e:
        logging.exception("Prediction failed")
        raise HTTPException(status_code=500, detail=str(e))