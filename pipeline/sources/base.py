from __future__ import annotations

from abc import ABC, abstractmethod

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from pipeline.models import Observation


class HttpSource(ABC):
    name: str
    timeout_seconds = 30

    def __init__(self, session: requests.Session | None = None):
        self.session = session or self._build_session()

    @staticmethod
    def _build_session() -> requests.Session:
        session = requests.Session()
        retry = Retry(
            total=3,
            connect=3,
            read=3,
            status=3,
            backoff_factor=1,
            status_forcelist=(429, 500, 502, 503, 504),
            allowed_methods=frozenset({"GET"}),
        )
        session.mount("https://", HTTPAdapter(max_retries=retry))
        session.headers.update({"User-Agent": "kenya-econ-pipeline/2.0 (+https://github.com/gishusam/kenya-econ-pipeline)"})
        return session

    def get_text(self, url: str) -> str:
        response = self.session.get(url, timeout=self.timeout_seconds)
        response.raise_for_status()
        return response.text

    def get_json(self, url: str, params: dict | None = None):
        response = self.session.get(url, params=params, timeout=self.timeout_seconds)
        response.raise_for_status()
        return response.json()

    @abstractmethod
    def fetch(self) -> list[Observation]:
        raise NotImplementedError
