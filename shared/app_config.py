import os
import json
from typing import Any, Dict
from pathlib import Path
from dotenv import load_dotenv

try:
    from google.cloud import secretmanager
except ImportError:
    secretmanager = None  # Safe fallback if not using GCP yet


# --- Environment Setup ---
ENVIRONMENT = os.getenv("ENVIRONMENT", "dev")
PROJECT_ID = os.getenv("GCP_PROJECT_ID", "analytics-project")

# --- Default values ---
APP_CONFIG_DEFAULT: Dict[str, Any] = {
    "CFB_API_KEY": "",
    "DB_USER": "root",
    "DB_PASSWORD": "",
    "DB_HOST": "localhost",
    "DB_PORT": "3306",
    "DB_NAME": "cfb",
    "OUTPUT_DIR": "./output_data",
    "GCP_SERVICE_ACCOUNT": {},
}

# --- Mapping of keys that live in Secret Manager ---
SECRETS_MAP = {
    "GCP_SERVICE_ACCOUNT": "gcp-analytics-service-account",
    "CFB_API_KEY": "cfb-analytics-api-key",
    # Add more secret IDs here when you start storing them securely
}


# --- Load local environment variables ---
if ENVIRONMENT != "production":
    load_dotenv()


# --- Helper to fetch a secret or fallback to env/default ---
def get_secret(config_key: str, project_id: str = PROJECT_ID, default_value: Any = None) -> Any:
    """
    Retrieves a configuration value from GCP Secret Manager (in prod)
    or from environment variables (in dev/staging).
    """
    secret_value = None

    if ENVIRONMENT == "production" and secretmanager and config_key in SECRETS_MAP:
        try:
            client = secretmanager.SecretManagerServiceClient()
            secret_id = SECRETS_MAP[config_key]
            name = f"projects/{project_id}/secrets/{secret_id}/versions/latest"
            response = client.access_secret_version(request={"name": name})
            secret_value = response.payload.data.decode("UTF-8")
        except Exception as e:
            print(f"[WARN] Failed to retrieve secret {config_key} from GCP: {e}")
            secret_value = os.getenv(config_key, default_value)
    else:
        secret_value = os.getenv(config_key, default_value)

    # Post-process complex values
    if config_key == "GCP_SERVICE_ACCOUNT":
        secret_value = handle_gcp_service_account(secret_value)

    return secret_value


def handle_gcp_service_account(value: Any) -> Dict[str, Any]:
    """Convert a path or JSON string to a dict for service account creds."""
    if not value:
        return {}
    if isinstance(value, dict):
        return value
    try:
        path = Path(value)
        if path.is_file():
            with path.open("r", encoding="utf-8") as f:
                return json.load(f)
    except Exception:
        pass
    try:
        return json.loads(value)
    except json.JSONDecodeError:
        print("[WARN] GCP service account value is not valid JSON.")
        return {}


# --- Load all config values into a single dict (optional convenience) ---
def get_app_config() -> Dict[str, Any]:
    """Builds the final config dictionary with secrets and defaults resolved."""
    config = {}
    for key, default in APP_CONFIG_DEFAULT.items():
        config[key] = get_secret(key, PROJECT_ID, default)
    return config
