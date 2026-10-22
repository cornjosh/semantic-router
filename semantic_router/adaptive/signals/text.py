"""Semantic evidence selection, never a substitute for context-window accounting."""

import json

from pydantic import JsonValue

from semantic_router.adaptive.models import Message, RoutingRequest
from semantic_router.adaptive.signals.base import InvalidSignalResult
from semantic_router.adaptive.signals.options import TextOptions


def _content(message: Message) -> str:
    if isinstance(message.content, str):
        return message.content
    texts = []
    for part in message.content or ():
        text = part.get("text")
        if part.get("type") == "text" and isinstance(text, str):
            texts.append(text)
    return "\n".join(texts)


def select_text(*, request: RoutingRequest, options: TextOptions) -> str:
    if options.mode == "latest_user":
        latest = next(
            (
                message
                for message in reversed(request.messages)
                if message.role == "user"
            ),
            None,
        )
        text = _content(latest) if latest is not None else ""
    else:
        # Serialize role boundaries and tool calls as evidence, never execute them
        # or turn them into the classifier's actual system/tool instructions.
        messages = []
        for message in request.messages:
            item: dict[str, JsonValue] = {
                "role": message.role,
                "content": _content(message),
            }
            if message.tool_calls:
                item["tool_calls"] = list(message.tool_calls)
            if message.tool_call_id:
                item["tool_call_id"] = message.tool_call_id
            messages.append(item)
        text = json.dumps(messages, ensure_ascii=False)
    if not text.strip() or len(text) > options.max_chars:
        raise InvalidSignalResult("semantic_input_empty_or_too_large")
    return text
