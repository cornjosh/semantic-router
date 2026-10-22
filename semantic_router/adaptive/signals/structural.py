"""Extract request structure without assigning capabilities, scores or targets."""

from pydantic import JsonValue

from semantic_router.adaptive.models import RoutingRequest


class StructuralSignal:
    async def extract(self, request: RoutingRequest) -> dict[str, JsonValue]:
        modalities: set[str] = set()
        text_characters = 0
        for message in request.messages:
            if isinstance(message.content, str):
                text_characters += len(message.content)
                modalities.add("text")
            elif message.content is not None:
                for part in message.content:
                    kind = part.get("type")
                    if kind in ("text", "input_text"):
                        text = part.get("text")
                        if isinstance(text, str):
                            text_characters += len(text)
                            modalities.add("text")
                        else:
                            modalities.add("unknown")
                    elif kind in ("image_url", "input_image"):
                        modalities.add("image")
                    elif kind in ("input_audio", "audio"):
                        modalities.add("audio")
                    elif kind == "video_url":
                        modalities.add("video")
                    else:
                        modalities.add("unknown")

        has_tool_result = any(message.role == "tool" for message in request.messages)
        modality_values: list[JsonValue] = [item for item in sorted(modalities)]
        return {
            "requested_model": request.model,
            "has_tool_result": has_tool_result,
            "is_tool_continuation": request.messages[-1].role == "tool",
            "requires_tools": bool(request.tools)
            or has_tool_result
            or any(message.tool_calls for message in request.messages)
            or request.tool_choice not in (None, "none"),
            "modalities": modality_values,
            "response_format": (request.response_format or {}).get("type", "text"),
            "max_output_tokens": request.max_completion_tokens or request.max_tokens,
            "stream": request.stream,
            "text_characters": text_characters,
            "message_count": len(request.messages),
        }
