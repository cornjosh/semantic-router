"""Policy consumes facts; extractors never choose a model or a shortcut."""

from collections.abc import Mapping

from pydantic import JsonValue

from semantic_router.adaptive.config import PolicyConfig, Predicate
from semantic_router.adaptive.models import PolicyMatch, SignalResult

_MISSING = object()


def _equal(left: JsonValue, right: JsonValue) -> bool:
    # Python bool is a subclass of int, but routing predicates must not equate
    # provider "true" with numeric 1, including inside arrays/objects.
    if isinstance(left, bool) or isinstance(right, bool):
        return type(left) is type(right) and left == right
    if isinstance(left, dict) and isinstance(right, dict):
        return left.keys() == right.keys() and all(
            _equal(left[key], right[key]) for key in left
        )
    if isinstance(left, list) and isinstance(right, list):
        return len(left) == len(right) and all(
            _equal(a, b) for a, b in zip(left, right)
        )
    return left == right


def _matches(predicate: Predicate, signals: Mapping[str, SignalResult]) -> bool:
    result = signals.get(predicate.signal)
    if result is None or result.status != "ok":
        return False
    actual: object = result.value
    for key in predicate.path.split("."):
        if not isinstance(actual, dict) or key not in actual:
            actual = _MISSING
            break
        actual = actual[key]
    if predicate.op == "exists":
        return (actual is not _MISSING) == predicate.value
    if actual is _MISSING:
        return False
    if predicate.op == "eq":
        return _equal(actual, predicate.value)  # type: ignore[arg-type]
    if predicate.op == "in":
        assert isinstance(predicate.value, list)
        return any(_equal(actual, item) for item in predicate.value)  # type: ignore[arg-type]
    if type(actual) not in (int, float):
        return False
    assert isinstance(actual, (int, float))
    assert isinstance(predicate.value, (int, float))
    if predicate.op == "gte":
        return actual >= predicate.value
    return actual <= predicate.value


class Policy:
    def __init__(self, *, config: PolicyConfig):
        self.config = config.model_copy(deep=True)

    def evaluate(
        self, *, signals: Mapping[str, SignalResult], final: bool = False
    ) -> PolicyMatch | None:
        for rule in self.config.rules:
            if all(_matches(predicate, signals) for predicate in rule.when):
                return PolicyMatch(
                    rule=rule.name, models=rule.models, shortcut=rule.shortcut
                )
        if final:
            return PolicyMatch(models=self.config.fallback_models)
        return None
