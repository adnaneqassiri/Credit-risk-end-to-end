import logging

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware

from src.api.schemas import GeneratedClientResponse, PredictByIdRequest, PredictionResponse
from src.api.service import ClientNotFoundError, PredictionService

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)

app = FastAPI(
    title="Credit Risk Prediction API",
    version="1.0.0",
    description="API for credit default risk prediction using LightGBM",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["null"],
    allow_origin_regex=r"^https?://(localhost|127\.0\.0\.1)(:\d+)?$",
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
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


@app.post("/clients/generate", response_model=GeneratedClientResponse)
def generate_client():
    try:
        return prediction_service.generate_and_store_client()
    except Exception as e:
        logging.exception("Client generation failed")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/clients/{sk_id_curr}", response_model=GeneratedClientResponse)
def get_client(sk_id_curr: int):
    try:
        if sk_id_curr < 456256:
            raise HTTPException(status_code=422, detail="SK_ID_CURR must be greater than or equal to 456256.")
        return prediction_service.get_client_by_id(sk_id_curr)
    except ClientNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except HTTPException:
        raise
    except Exception as e:
        logging.exception("Client lookup failed")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/predict/by-id", response_model=PredictionResponse)
def predict_by_id(request: PredictByIdRequest):
    try:
        return prediction_service.predict_by_client_id(request.SK_ID_CURR)
    except ClientNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        logging.exception("Prediction failed")
        raise HTTPException(status_code=500, detail=str(e))
