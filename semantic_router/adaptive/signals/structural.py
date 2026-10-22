"""Extract request structure without assigning capabilities, scores or targets."""

import json

from pydantic import JsonValue

from semantic_router.adaptive.models import RoutingRequest


class StructuralSignal:
    async def extract(self, request: RoutingRequest) -> dict[str, JsonValue]:
        modalities: set[str] = set()
        text_characters = 0
        media_counts: dict[str, int] = {}
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
                        media_counts["image"] = media_counts.get("image", 0) + 1
                    elif kind in ("input_audio", "audio"):
                        modalities.add("audio")
                        media_counts["audio"] = media_counts.get("audio", 0) + 1
                    elif kind == "video_url":
                        modalities.add("video")
                        media_counts["video"] = media_counts.get("video", 0) + 1
                    else:
                        modalities.add("unknown")

        has_tool_result = any(message.role == "tool" for message in request.messages)
        modality_values: list[JsonValue] = [item for item in sorted(modalities)]
        payload = request.model_dump(
            mode="json", include={"messages", "tools", "tool_choice", "response_format"}
        )
        return {
            "payload_bytes": len(
                json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode(
                    "utf-8"
                )
            ),
            "media_counts": {name: count for name, count in media_counts.items()},
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
