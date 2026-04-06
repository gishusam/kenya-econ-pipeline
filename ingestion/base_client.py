import requests
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from utils.logger import get_logger

logger = get_logger(__name__)

class BaseClient:
    TIMEOUT = 30  

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        retry=retry_if_exception_type(requests.exceptions.RequestException),
        reraise=True,
    )
    def get(self, url: str, params: dict = None) -> dict:
        logger.info(f"GET {url} | params={params}")
        response = requests.get(url, params=params, timeout=self.TIMEOUT)
        response.raise_for_status()
        logger.info(f"Response {response.status_code} from {url}")
        return response.json()