"""Versioned wire envelopes. Generation requests/responses never traverse here."""

from typing import Literal

from semantic_router.adaptive.models import Contract, Identifier, RoutingRequest
from semantic_router.adaptive.state.base import Feedback, Observation


class RouteEnvelope(Contract):
    schema_version: Literal["1"]
    request: RoutingRequest


class ObservationEnvelope(Contract):
    schema_version: Literal["1"]
    tenant_id: Identifier
    event: Observation


class FeedbackEnvelope(Contract):
    schema_version: Literal["1"]
    tenant_id: Identifier
    event: Feedback
