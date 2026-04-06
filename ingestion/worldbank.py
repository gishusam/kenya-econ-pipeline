import json
import os
from datetime import datetime, timezone
from ingestion.base_client import BaseClient
from utils.validators import validate_response
from utils.logger import get_logger

logger = get_logger(__name__)

BASE_URL = "https://api.worldbank.org/v2/country/KE/indicator"
INDICATORS = {
    "gdp": "NY.GDP.MKTP.CD",
    "inflation": "FP.CPI.TOTL.ZG",
}

class WorldBankIngester(BaseClient):

    def fetch_indicator(self, indicator_key: str) -> dict:
        indicator_code = INDICATORS[indicator_key]
        url = f"{BASE_URL}/{indicator_code}"
        params = {"format": "json", "per_page": 10, "mrv": 5}

        raw = self.get(url, params=params)

        # World Bank returns a list: [metadata, data]
        validate_response(
            data={"metadata": raw[0], "data": raw[1]},
            required_fields=["metadata", "data"],
            source=f"worldbank/{indicator_key}",
        )
        return raw

    def save(self, data: dict, indicator_key: str) -> str:
        date_str = datetime.now(timezone.utc).strftime("%Y%m%d")
        folder = "data/raw/worldbank"
        os.makedirs(folder, exist_ok=True)
        filepath = f"{folder}/{indicator_key}_ke_{date_str}.json"

        with open(filepath, "w") as f:
            json.dump(data, f, indent=2)

        logger.info(f"Saved {indicator_key} data to {filepath}")
        return filepath

    def run(self):
        for key in INDICATORS:
            logger.info(f"Fetching World Bank indicator: {key}")
            data = self.fetch_indicator(key)
            self.save(data, key)


if __name__ == "__main__":
    WorldBankIngester().run()