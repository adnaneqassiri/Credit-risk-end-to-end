const API_BASE_URL = "http://127.0.0.1:8000";

console.log("API base URL:", API_BASE_URL);

const elements = {
  alert: document.querySelector("#alert"),
  checkStatusBtn: document.querySelector("#checkStatusBtn"),
  generateBtn: document.querySelector("#generateBtn"),
  predictionForm: document.querySelector("#predictionForm"),
  predictBtn: document.querySelector("#predictBtn"),
  loadingText: document.querySelector("#loadingText"),
  apiBadge: document.querySelector("#apiBadge"),
  apiStatus: document.querySelector("#apiStatus"),
  modelName: document.querySelector("#modelName"),
  modelVersion: document.querySelector("#modelVersion"),
  featureCount: document.querySelector("#featureCount"),
  generatedClientId: document.querySelector("#generatedClientId"),
  jsonViewer: document.querySelector("#jsonViewer"),
  predictClientId: document.querySelector("#predictClientId"),
  resultCard: document.querySelector("#resultCard"),
  emptyResult: document.querySelector("#emptyResult"),
  resultContent: document.querySelector("#resultContent"),
  riskBadge: document.querySelector("#riskBadge"),
  scoreValue: document.querySelector("#scoreValue"),
  riskClass: document.querySelector("#riskClass"),
  resultClientId: document.querySelector("#resultClientId"),
  resultModelVersion: document.querySelector("#resultModelVersion"),
  businessMessage: document.querySelector("#businessMessage"),
};

const riskMessages = {
  LOW: "This applicant presents a low credit default risk.",
  MEDIUM: "This applicant requires manual review.",
  HIGH: "This applicant presents a high probability of credit default.",
};

function showAlert(message, type = "error") {
  elements.alert.textContent = message;
  elements.alert.className = `alert ${type === "success" ? "success" : ""}`;
}

function hideAlert() {
  elements.alert.classList.add("hidden");
}

function setLoading(element, isLoading) {
  element.disabled = isLoading;
}

function setApiBadge(status, type) {
  elements.apiBadge.textContent = status;
  elements.apiBadge.className = `badge badge-${type}`;
}

function buildApiErrorMessage(error) {
  if (error instanceof TypeError) {
    return [
      "API unreachable.",
      "Verify that FastAPI is running on http://127.0.0.1:8000.",
      "Verify CORS configuration.",
      `Browser error: ${error.message}`,
    ].join(" ");
  }

  return error.message;
}

async function apiFetch(path, options = {}) {
  const url = `${API_BASE_URL}${path}`;
  const requestOptions = {
    ...options,
    headers: {
      "Content-Type": "application/json",
      ...(options.headers || {}),
    },
  };

  console.log("API request:", requestOptions.method || "GET", url);

  let response;
  try {
    response = await fetch(url, requestOptions);
  } catch (error) {
    console.error("API fetch failed:", {
      url,
      message: error.message,
      error,
    });
    throw new Error(buildApiErrorMessage(error));
  }

  console.log("API response status:", response.status, response.statusText, url);

  const rawBody = await response.text();
  let body = null;

  if (rawBody) {
    try {
      body = JSON.parse(rawBody);
    } catch (error) {
      console.error("API response is not valid JSON:", {
        url,
        status: response.status,
        rawBody,
        error,
      });
      throw new Error("API returned a non-JSON response.");
    }
  }

  if (!response.ok) {
    console.error("API error response:", {
      url,
      status: response.status,
      rawBody,
      body,
    });

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
  setLoading(elements.checkStatusBtn, true);

  try {
    const health = await apiFetch("/health");
    const info = await apiFetch("/model/info");

    elements.apiStatus.textContent = health.status?.toUpperCase() || "UNKNOWN";
    elements.modelName.textContent = info.model_name || "-";
    elements.modelVersion.textContent = info.model_version || "-";
    elements.featureCount.textContent = info.n_features ?? "-";
    setApiBadge("Online", "ok");
    showAlert("API is available and model metadata was loaded.", "success");
  } catch (error) {
    console.error("API status check failed:", error.message);
    elements.apiStatus.textContent = "Unavailable";
    elements.modelName.textContent = "-";
    elements.modelVersion.textContent = "-";
    elements.featureCount.textContent = "-";
    setApiBadge("Offline", "error");
    showAlert(`Offline: ${error.message}`);
  } finally {
    setLoading(elements.checkStatusBtn, false);
  }
}

async function generateApplicant() {
  hideAlert();
  setLoading(elements.generateBtn, true);
  elements.generateBtn.textContent = "Generating...";

  try {
    const client = await apiFetch("/clients/generate", { method: "POST" });
    elements.generatedClientId.textContent = client.SK_ID_CURR;
    elements.predictClientId.value = client.SK_ID_CURR;
    elements.jsonViewer.textContent = JSON.stringify(client, null, 2);
    showAlert(`Applicant ${client.SK_ID_CURR} generated and stored successfully.`, "success");
  } catch (error) {
    console.error("Applicant generation failed:", error.message);
    showAlert(`Applicant generation failed: ${error.message}`);
  } finally {
    elements.generateBtn.textContent = "Generate New Applicant";
    setLoading(elements.generateBtn, false);
  }
}

function readPredictionClientId() {
  const rawValue = elements.predictClientId.value.trim();
  if (!rawValue) {
    throw new Error("SK_ID_CURR is required.");
  }

  const value = Number(rawValue);
  if (!Number.isInteger(value)) {
    throw new Error("SK_ID_CURR must be an integer.");
  }

  if (value < 1) {
    throw new Error("SK_ID_CURR must be a positive integer.");
  }

  return value;
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

  let skIdCurr;
  try {
    skIdCurr = readPredictionClientId();
  } catch (error) {
    showAlert(error.message);
    return;
  }

  elements.predictBtn.disabled = true;
  elements.loadingText.classList.remove("hidden");

  try {
    const result = await apiFetch("/predict/by-id", {
      method: "POST",
      body: JSON.stringify({
        SK_ID_CURR: Number(skIdCurr),
      }),
    });
    renderPredictionResult(result);
  } catch (error) {
    console.error("Prediction failed:", error.message);
    showAlert(`Prediction failed: ${error.message}`);
  } finally {
    elements.predictBtn.disabled = false;
    elements.loadingText.classList.add("hidden");
  }
}

elements.checkStatusBtn.addEventListener("click", checkApiStatus);
elements.generateBtn.addEventListener("click", generateApplicant);
elements.predictionForm.addEventListener("submit", handlePredictionSubmit);

checkApiStatus();
