import json
import os
from datetime import datetime, timezone
from dotenv import load_dotenv
from ingestion.base_client import BaseClient
from utils.validators import validate_response
from utils.logger import get_logger

load_dotenv()
logger = get_logger(__name__)

BASE_URL = "https://openexchangerates.org/api/latest.json"

class FXRatesIngester(BaseClient):

    def __init__(self):
        self.api_key = os.getenv("OPEN_EXCHANGE_APP_ID")
        if not self.api_key:
            raise EnvironmentError("OPEN_EXCHANGE_APP_ID not set in .env")

    def fetch(self) -> dict:
        params = {"app_id": self.api_key, "base": "USD", "symbols": "KES,EUR,GBP"}
        raw = self.get(BASE_URL, params=params)
        validate_response(
            data=raw,
            required_fields=["rates", "timestamp", "base"],
            source="fx_rates",
        )
        return raw

    def save(self, data: dict) -> str:
        ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        folder = "data/raw/fx_rates"
        os.makedirs(folder, exist_ok=True)
        filepath = f"{folder}/fx_ke_{ts}.json"

        with open(filepath, "w") as f:
            json.dump(data, f, indent=2)

        logger.info(f"Saved FX rates to {filepath}")
        return filepath

    def run(self):
        logger.info("Fetching FX rates")
        data = self.fetch()
        self.save(data)


if __name__ == "__main__":
    FXRatesIngester().run()