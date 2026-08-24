import hashlib
import json

from pipeline.models import Observation, _normalize_decimal


def record_hash(observation: Observation) -> str:
    canonical = {
        "source": observation.source,
        "indicator_code": observation.indicator_code,
        "geography": observation.geography,
        "period_start": observation.period_start.isoformat(),
        "period_end": observation.period_end.isoformat(),
        "value": _normalize_decimal(observation.value),
        "unit": observation.unit,
        "currency": observation.currency,
    }
    payload = json.dumps(canonical, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()
