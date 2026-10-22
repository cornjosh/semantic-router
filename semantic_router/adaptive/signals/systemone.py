"""TypeSafe/System One choice facts, compatible with arbitrary full endpoints."""

from typing import TYPE_CHECKING

from pydantic import JsonValue

from semantic_router.adaptive.models import RoutingRequest
from semantic_router.adaptive.signals.base import InvalidSignalResult
from semantic_router.adaptive.signals.options import QuestionOptions
from semantic_router.adaptive.signals.text import select_text
from semantic_router.adaptive.signals.validation import number
from semantic_router.decision_models.base import choice

if TYPE_CHECKING:
    from semantic_router.adaptive.providers import HTTPProvider


class SystemOneSignal:
    def __init__(self, *, provider: "HTTPProvider", options: QuestionOptions):
        self.provider, self.options = provider, options

    async def extract(self, *, request: RoutingRequest) -> dict[str, JsonValue]:
        text = select_text(request=request, options=self.options.text)
        response = await self.provider.post(
            body={
                "model": self.provider.config.model,
                "state": text,
                "questions": {
                    name: choice(instructions=q.instructions, options=q.options)
                    for name, q in self.options.questions.items()
                },
            }
        )
        answers = response.get("answers")
        if (
            not isinstance(answers, dict)
            or answers.keys() != self.options.questions.keys()
        ):
            raise InvalidSignalResult("incomplete_answers")
        facts: dict[str, JsonValue] = {}
        validated: dict[str, JsonValue] = {}
        for name, question in self.options.questions.items():
            answer = answers[name]
            if not isinstance(answer, dict) or answer.get("type") != "choice":
                raise InvalidSignalResult("invalid_answer_type")
            probs = answer.get("probabilities")
            if not isinstance(probs, dict) or probs.keys() != question.options.keys():
                raise InvalidSignalResult("incomplete_probabilities")
            probs = {key: number(value) for key, value in probs.items()}
            if abs(sum(probs.values()) - 1.0) > 0.01:
                raise InvalidSignalResult("invalid_probability_sum")
            label = answer.get("choice")
            if (
                not isinstance(label, str)
                or label not in probs
                or probs[label] < max(probs.values()) - 1e-8
            ):
                raise InvalidSignalResult("inconsistent_choice")
            confidence = number(answer.get("confidence"))
            facts[name] = label
            validated[name] = {"probabilities": probs, "confidence": confidence}
        model = response.get("model", self.provider.config.model)
        if not isinstance(model, str) or not model or len(model) > 256:
            raise InvalidSignalResult("invalid_model")
        facts.update(
            {
                "answers": validated,
                "score_type": "provider_probability",
                "provider_model": model,
                "text_selection": self.options.text.mode,
                "text_characters": len(text),
            }
        )
        return facts
