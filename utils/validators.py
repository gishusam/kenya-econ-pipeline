from utils.logger import get_logger

logger = get_logger(__name__)

class ValidationError(Exception):
    pass

def validate_response(data: dict, required_fields: list, source: str) -> None:
    missing = [f for f in required_fields if f not in data]
    if missing:
        msg = f"[{source}] Missing required fields: {missing}"
        logger.error(msg)
        raise ValidationError(msg)

    for field in required_fields:
        if data[field] is None:
            msg = f"[{source}] Field '{field}' is None — API may have returned empty data"
            logger.error(msg)
            raise ValidationError(msg)

    logger.info(f"[{source}] Validation passed for fields: {required_fields}")