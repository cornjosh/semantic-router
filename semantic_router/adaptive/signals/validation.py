"""Strict parsing of untrusted provider data without echoing its contents."""

import json
import math
from typing import Any

from semantic_router.adaptive.models import finite_json
from semantic_router.adaptive.signals.base import InvalidSignalResult


def strict_json(value: bytes | str) -> dict[str, Any]:
    def pairs(items):
        result = {}
        for key, item in items:
            if key in result:
                raise ValueError("duplicate key")
            result[key] = item
        return result

    try:
        result = json.loads(value, object_pairs_hook=pairs)
        if not isinstance(result, dict):
            raise ValueError("expected object")
        finite_json(result)
        return result
    except (ValueError, RecursionError):
        raise InvalidSignalResult("invalid_json") from None


def number(value: object, *, minimum: float = 0, maximum: float = 1) -> float:
    if not isinstance(value, (int, float)) or isinstance(value, bool):
        raise InvalidSignalResult("invalid_number")
    if not minimum <= value <= maximum:
        raise InvalidSignalResult("invalid_number")
    parsed = float(value)
    if not math.isfinite(parsed):
        raise InvalidSignalResult("invalid_number")
    return parsed
