"""Signal-specific trusted options. No model destinations or policy thresholds."""

from __future__ import annotations

from typing import Literal

from pydantic import Field, model_validator

from semantic_router.adaptive.models import Contract, Name

RESERVED = {
    "answers",
    "score_type",
    "confidence",
    "provider_model",
    "text_selection",
    "text_characters",
}


class TextOptions(Contract):
    mode: Literal["latest_user", "conversation"] = "latest_user"
    max_chars: int = Field(default=16384, gt=0, le=1048576, strict=True)


class SignalOptions(Contract):
    provider: Name
    text: TextOptions = Field(default_factory=TextOptions)


class ChoiceQuestion(Contract):
    instructions: str = Field(min_length=1, max_length=4096)
    options: dict[Name, str] = Field(min_length=2, max_length=255)

    @model_validator(mode="after")
    def descriptions(self) -> ChoiceQuestion:
        if any(
            not value.strip() or len(value) > 4096 for value in self.options.values()
        ):
            raise ValueError("option descriptions must be nonempty and bounded")
        return self


class QuestionOptions(SignalOptions):
    questions: dict[Name, ChoiceQuestion] = Field(min_length=1, max_length=32)
    max_completion_tokens: int = Field(default=512, gt=0, le=8192, strict=True)

    @model_validator(mode="after")
    def reserved_keys(self) -> QuestionOptions:
        if RESERVED & self.questions.keys():
            raise ValueError("question uses a reserved metadata key")
        return self


class EmbeddingOptions(SignalOptions):
    examples: dict[Name, tuple[str, ...]] = Field(default_factory=dict, max_length=128)
    dimensions: int | None = Field(default=None, gt=0, le=65536, strict=True)

    @model_validator(mode="after")
    def bounded_examples(self) -> EmbeddingOptions:
        texts = [text for group in self.examples.values() for text in group]
        if (
            any(not group for group in self.examples.values())
            or len(texts) > 1024
            or any(
                not text.strip() or len(text) > self.text.max_chars for text in texts
            )
        ):
            raise ValueError("embedding examples must be nonempty and bounded")
        return self
