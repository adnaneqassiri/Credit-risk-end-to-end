const API_BASE_URL = "http://127.0.0.1:8000";

const elements = {
  alert: document.querySelector("#alert"),
  checkStatusBtn: document.querySelector("#checkStatusBtn"),
  predictionForm: document.querySelector("#predictionForm"),
  predictBtn: document.querySelector("#predictBtn"),
  loadingText: document.querySelector("#loadingText"),
  apiBadge: document.querySelector("#apiBadge"),
  apiStatus: document.querySelector("#apiStatus"),
  modelName: document.querySelector("#modelName"),
  modelVersion: document.querySelector("#modelVersion"),
  featureCount: document.querySelector("#featureCount"),
  requestPreview: document.querySelector("#requestPreview"),
  resultCard: document.querySelector("#resultCard"),
  emptyResult: document.querySelector("#emptyResult"),
  resultContent: document.querySelector("#resultContent"),
  riskBadge: document.querySelector("#riskBadge"),
  scoreValue: document.querySelector("#scoreValue"),
  riskClass: document.querySelector("#riskClass"),
  resultClientId: document.querySelector("#resultClientId"),
  resultModelVersion: document.querySelector("#resultModelVersion"),
  businessMessage: document.querySelector("#businessMessage"),
  fields: {
    skId: document.querySelector("#skId"),
    debtRatio: document.querySelector("#debtRatio"),
    totalDebt: document.querySelector("#totalDebt"),
    totalCredit: document.querySelector("#totalCredit"),
    activeLoans: document.querySelector("#activeLoans"),
    additionalFeatures: document.querySelector("#additionalFeatures"),
  },
};

const riskMessages = {
  LOW: "This client presents a low credit default risk.",
  MEDIUM: "This client requires manual review.",
  HIGH: "This client presents a high probability of credit default.",
};

function showAlert(message, type = "error") {
  elements.alert.textContent = message;
  elements.alert.className = `alert ${type === "success" ? "success" : ""}`;
}

function hideAlert() {
  elements.alert.classList.add("hidden");
}

function parseNumberField(input, label) {
  if (input.value.trim() === "") {
    throw new Error(`${label} is required.`);
  }

  const value = Number(input.value);
  if (!Number.isFinite(value)) {
    throw new Error(`${label} must be a valid number.`);
  }

  return value;
}

function parseAdditionalFeatures() {
  const rawJson = elements.fields.additionalFeatures.value.trim();
  if (!rawJson) {
    return {};
  }

  let parsed;
  try {
    parsed = JSON.parse(rawJson);
  } catch (error) {
    throw new Error("Additional Features JSON is invalid. Please enter a valid JSON object.");
  }

  if (Array.isArray(parsed) || parsed === null || typeof parsed !== "object") {
    throw new Error("Additional Features JSON must be a JSON object.");
  }

  return parsed;
}

function buildPayload() {
  const skId = parseNumberField(elements.fields.skId, "SK_ID_CURR");
  if (!Number.isInteger(skId)) {
    throw new Error("SK_ID_CURR must be an integer.");
  }

  // The current FastAPI schema accepts SK_ID_CURR values greater than 456256.
  if (skId <= 456256) {
    throw new Error("SK_ID_CURR must be greater than 456256 for this API schema.");
  }

  return {
    SK_ID_CURR: skId,
    features: {
      DEBT_RATIO: parseNumberField(elements.fields.debtRatio, "DEBT_RATIO"),
      TOTAL_DEBT: parseNumberField(elements.fields.totalDebt, "TOTAL_DEBT"),
      TOTAL_CREDIT: parseNumberField(elements.fields.totalCredit, "TOTAL_CREDIT"),
      ACTIVE_LOANS_COUNT: parseNumberField(elements.fields.activeLoans, "ACTIVE_LOANS_COUNT"),
      ...parseAdditionalFeatures(),
    },
  };
}

function updateRequestPreview() {
  try {
    const payload = buildPayload();
    elements.requestPreview.textContent = JSON.stringify(payload, null, 2);
  } catch (error) {
    elements.requestPreview.textContent = error.message;
  }
}

function setApiBadge(status, type) {
  elements.apiBadge.textContent = status;
  elements.apiBadge.className = `badge badge-${type}`;
}

async function fetchJson(path, options = {}) {
  const response = await fetch(`${API_BASE_URL}${path}`, {
    headers: {
      "Content-Type": "application/json",
      ...(options.headers || {}),
    },
    ...options,
  });

  const body = await response.json().catch(() => null);

  if (!response.ok) {
    const detail = body?.detail;
    const message = Array.isArray(detail)
      ? detail.map((item) => item.msg).join(" ")
      : detail || `Server returned ${response.status}.`;
    throw new Error(message);
  }

  return body;
}

async function checkApiStatus() {
  hideAlert();
  elements.checkStatusBtn.disabled = true;

  try {
    const [health, info] = await Promise.all([
      fetchJson("/health"),
      fetchJson("/model/info"),
    ]);

    elements.apiStatus.textContent = health.status?.toUpperCase() || "UNKNOWN";
    elements.modelName.textContent = info.model_name || "-";
    elements.modelVersion.textContent = info.model_version || "-";
    elements.featureCount.textContent = info.n_features ?? "-";
    setApiBadge("Online", "ok");
    showAlert("API is available and model metadata was loaded.", "success");
  } catch (error) {
    elements.apiStatus.textContent = "Unavailable";
    elements.modelName.textContent = "-";
    elements.modelVersion.textContent = "-";
    elements.featureCount.textContent = "-";
    setApiBadge("Offline", "error");
    showAlert(`API unavailable: ${error.message}`);
  } finally {
    elements.checkStatusBtn.disabled = false;
  }
}

function resetResultClasses() {
  elements.resultCard.classList.remove("result-empty", "result-low", "result-medium", "result-high");
  elements.riskBadge.className = "badge badge-muted";
}

function renderPredictionResult(result) {
  const risk = String(result.risk_class || "").toUpperCase();
  const scorePercent = `${(Number(result.prediction_score) * 100).toFixed(2)}%`;

  resetResultClasses();
  elements.resultCard.classList.add(`result-${risk.toLowerCase()}`);
  elements.riskBadge.className = `badge badge-${risk.toLowerCase()}`;
  elements.riskBadge.textContent = risk || "Unknown";

  elements.emptyResult.classList.add("hidden");
  elements.resultContent.classList.remove("hidden");
  elements.scoreValue.textContent = scorePercent;
  elements.riskClass.textContent = risk;
  elements.resultClientId.textContent = result.SK_ID_CURR;
  elements.resultModelVersion.textContent = result.model_version || "-";
  elements.businessMessage.textContent = riskMessages[risk] || "Prediction completed.";
}

async function handlePredictionSubmit(event) {
  event.preventDefault();
  hideAlert();

  let payload;
  try {
    payload = buildPayload();
    updateRequestPreview();
  } catch (error) {
    showAlert(error.message);
    return;
  }

  elements.predictBtn.disabled = true;
  elements.loadingText.classList.remove("hidden");

  try {
    const result = await fetchJson("/predict", {
      method: "POST",
      body: JSON.stringify(payload),
    });
    renderPredictionResult(result);
  } catch (error) {
    showAlert(`Prediction failed: ${error.message}`);
  } finally {
    elements.predictBtn.disabled = false;
    elements.loadingText.classList.add("hidden");
  }
}

Object.values(elements.fields).forEach((field) => {
  field.addEventListener("input", updateRequestPreview);
});

elements.checkStatusBtn.addEventListener("click", checkApiStatus);
elements.predictionForm.addEventListener("submit", handlePredictionSubmit);

updateRequestPreview();
checkApiStatus();
