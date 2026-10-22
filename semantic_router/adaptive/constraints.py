"""Hard catalog checks consume facts without influencing signal extraction."""

from typing import Any

from semantic_router.adaptive.service_config import ModelConfig


def account(*, facts: dict[str, Any], model: ModelConfig) -> dict[str, Any] | None:
    if (
        not model.enabled
        or (facts["requires_tools"] and not model.tools)
        or not set(facts["modalities"]) <= set(model.modalities)
        or facts["response_format"] not in model.response_formats
        or model.accounting.mode == "exact_required"
    ):
        return None
    output = facts["max_output_tokens"] or model.max_output_tokens
    if output > model.max_output_tokens:
        return None
    # Include all context, tool definitions/calls and response schema, not just
    # the text selected by a semantic signal. Byte count is a declared bound;
    # it is never labelled an exact tokenizer measurement.
    input_bound = facts["payload_bytes"] + model.accounting.framing_tokens
    input_bound += model.accounting.tokens_per_message * facts["message_count"]
    allowances = dict(model.accounting.media_tokens)
    for media, count in facts["media_counts"].items():
        if media not in allowances:
            return None
        input_bound += allowances[media] * count
    if input_bound + output > model.max_context_tokens:
        return None
    return {
        "method": "declared_utf8_upper_bound",
        "input_tokens_bound": input_bound,
        "output_tokens_reserved": output,
        "context_limit": model.max_context_tokens,
    }
