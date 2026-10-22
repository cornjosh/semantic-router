"""Embedding evidence, optionally compared with immutable category examples."""

import asyncio
from typing import TYPE_CHECKING, Any

import numpy as np
from pydantic import JsonValue

from semantic_router.adaptive.models import RoutingRequest
from semantic_router.adaptive.signals.base import InvalidSignalResult
from semantic_router.adaptive.signals.options import EmbeddingOptions
from semantic_router.adaptive.signals.text import select_text
from semantic_router.adaptive.signals.validation import number
from semantic_router.linear import similarity_matrix

if TYPE_CHECKING:
    from semantic_router.adaptive.providers import HTTPProvider


class EmbeddingSignal:
    def __init__(self, *, provider: "HTTPProvider", options: EmbeddingOptions):
        self.provider, self.options = provider, options
        self._examples: np.ndarray | None = None
        self._init_lock = asyncio.Lock()
        self._model: str | None = None

    async def _encode(self, texts: list[str]) -> tuple[np.ndarray, str]:
        body: dict[str, Any] = {
            "model": self.provider.config.model,
            "input": texts,
            "encoding_format": "float",
        }
        if self.options.dimensions is not None:
            body["dimensions"] = self.options.dimensions
        response = await self.provider.post(body=body)
        data = response.get("data")
        if not isinstance(data, list) or len(data) != len(texts):
            raise InvalidSignalResult("incomplete_embeddings")
        by_index: dict[int, list[float]] = {}
        dimension = self.options.dimensions
        for item in data:
            if not isinstance(item, dict):
                raise InvalidSignalResult("invalid_embedding")
            index, vector = item.get("index"), item.get("embedding")
            if (
                type(index) is not int
                or not 0 <= index < len(texts)
                or index in by_index
            ):
                raise InvalidSignalResult("invalid_embedding_index")
            if not isinstance(vector, list) or not vector or len(vector) > 65536:
                raise InvalidSignalResult("invalid_embedding_vector")
            # A numerical bound avoids overflow in the shared cosine kernel.
            values = [number(value, minimum=-1e100, maximum=1e100) for value in vector]
            if dimension is None:
                dimension = len(values)
            if len(values) != dimension or np.linalg.norm(values) < 1e-100:
                raise InvalidSignalResult("invalid_embedding_dimension_or_norm")
            by_index[index] = values
        model = response.get("model", self.provider.config.model)
        if not isinstance(model, str) or not model or len(model) > 256:
            raise InvalidSignalResult("invalid_model")
        return np.array([by_index[i] for i in range(len(texts))]), model

    async def extract(self, *, request: RoutingRequest) -> dict[str, JsonValue]:
        text = select_text(request=request, options=self.options.text)
        query = None
        if self.options.examples and self._examples is None:
            async with self._init_lock:
                if self._examples is None:
                    texts = [
                        text
                        for group in self.options.examples.values()
                        for text in group
                    ]
                    vectors, model = await self._encode([*texts, text])
                    self._examples, self._model = vectors[:-1], model
                    query = vectors[-1]
        if query is None:
            vectors, model = await self._encode([text])
            query = vectors[0]
        facts: dict[str, JsonValue] = {
            "dimensions": len(query),
            "provider_model": model,
            "text_selection": self.options.text.mode,
            "text_characters": len(text),
        }
        if self._examples is None:
            facts.update(
                {"embedding": query.tolist(), "score_type": "embedding_vector"}
            )
            return facts
        if model != self._model or len(query) != self._examples.shape[1]:
            raise InvalidSignalResult("embedding_model_or_dimension_changed")
        similarities = similarity_matrix(xq=query, index=self._examples)
        scores: dict[str, float] = {}
        offset = 0
        for category, examples in self.options.examples.items():
            score = float(np.max(similarities[offset : offset + len(examples)]))
            scores[category] = max(-1.0, min(1.0, score))
            offset += len(examples)
        category = max(scores, key=lambda name: scores[name])
        facts.update(
            {
                "category": category,
                "scores": {name: value for name, value in scores.items()},
                "score_type": "cosine_similarity",
            }
        )
        return facts
