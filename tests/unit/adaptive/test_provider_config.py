"""Deployment mistakes are caught without constructing providers or needing keys."""

import pytest
from pydantic import ValidationError

from semantic_router.adaptive.config import SignalSpec
from semantic_router.adaptive.providers import ProviderConfig, ProviderRegistry
from semantic_router.adaptive.signals.options import EmbeddingOptions, QuestionOptions


@pytest.mark.parametrize(
    "endpoint",
    [
        "file:///secret",
        "ftp://host/path",
        "https://user:pass@host/path",
        "https://host/path?key=secret",
        "https://host/path#fragment",
        "http://host:invalid/path",
        "http://host/white space",
        "http:///path",
    ],
)
def test_endpoint_rejects_credentials_queries_and_non_http(endpoint):
    with pytest.raises(ValidationError):
        ProviderConfig(protocol="llm", model="fixture", endpoint=endpoint)


@pytest.mark.parametrize(
    "limits",
    [
        {"concurrency": 0},
        {"timeout_ms": 0},
        {"max_response_bytes": 0},
        {"max_request_bytes": -1},
        {"api_key_env": "key=value"},
    ],
)
def test_provider_rejects_invalid_limits(limits):
    with pytest.raises(ValidationError):
        ProviderConfig(
            protocol="embedding",
            endpoint="https://host/embed",
            model="fixture",
            **limits,
        )


def test_question_and_example_configuration_is_bounded():
    with pytest.raises(ValidationError):
        QuestionOptions.model_validate(
            {
                "provider": "p",
                "questions": {
                    "score_type": {"instructions": "x", "options": {"a": "A", "b": "B"}}
                },
            }
        )
    with pytest.raises(ValidationError):
        EmbeddingOptions.model_validate({"provider": "p", "examples": {"empty": []}})


@pytest.mark.parametrize("kind,provider", [("systemone", "test"), ("llm", "absent")])
def test_enabled_signals_validate_protocol_and_provider_at_startup(kind, provider):
    registry = ProviderRegistry(
        providers={
            "test": ProviderConfig(
                protocol="llm",
                endpoint="https://host/chat",
                model="fixture",
                api_key_env="UNSET_API_KEY",
            )
        }
    )
    spec = SignalSpec(
        name="semantic",
        kind=kind,
        parameters={
            "provider": provider,
            "questions": {
                "category": {"instructions": "x", "options": {"a": "A", "b": "B"}}
            },
        },
    )
    with pytest.raises(ValueError, match="provider"):
        registry.validate(signals=(spec,))


def test_disabled_invalid_provider_is_not_validated():
    registry = ProviderRegistry(providers={})
    registry.validate(signals=(SignalSpec(name="unused", kind="llm", enabled=False),))


def test_enabled_config_validation_does_not_read_credentials(monkeypatch):
    monkeypatch.delenv("UNSET_API_KEY", raising=False)
    registry = ProviderRegistry(
        providers={
            "test": ProviderConfig(
                protocol="embedding",
                endpoint="https://host/embed",
                model="fixture",
                api_key_env="UNSET_API_KEY",
            )
        }
    )
    registry.validate(
        signals=(
            SignalSpec(
                name="semantic", kind="embedding", parameters={"provider": "test"}
            ),
        )
    )
