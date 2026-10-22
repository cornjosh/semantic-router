"""Only already-qualified models/clusters enter optimization or exploration."""

from datetime import datetime, timedelta, timezone

import pytest
from pydantic import ValidationError

from semantic_router.adaptive.selection import (
    ClusterOption,
    LearningConfig,
    LoadConfig,
    NoCandidates,
    choose_cluster,
    choose_model,
)
from semantic_router.adaptive.state.base import (
    ClusterState,
    ModelArm,
    Observation,
    QualityCounts,
)


def test_quality_feedback_can_change_model_choice():
    arms = (ModelArm(model_id="a", version="1"), ModelArm(model_id="b", version="1"))
    first = choose_model(candidates=arms, counts={}, config=LearningConfig(), draw=0.9)
    assert first.model_id == "a"
    second = choose_model(
        candidates=arms,
        counts={"a": QualityCounts(failures=2)},
        config=LearningConfig(),
        draw=0.9,
    )
    assert second.model_id == "b"
    assert second.propensity == 1
    assert second.scores == {"a": 0.25, "b": 0.5}


@pytest.mark.parametrize(
    "draw,selected,propensity", [(0.01, "a", 0.9), (0.15, "b", 0.1), (0.8, "a", 0.9)]
)
def test_epsilon_greedy_records_actual_selection_probability(
    draw, selected, propensity
):
    selection = choose_model(
        candidates=(
            ModelArm(model_id="a", version="1"),
            ModelArm(model_id="b", version="1"),
        ),
        counts={},
        config=LearningConfig(epsilon=0.2),
        draw=draw,
    )
    assert selection.model_id == selected
    assert selection.propensity == pytest.approx(propensity)


def test_exploration_cannot_add_an_ineligible_model():
    selected = choose_model(
        candidates=(ModelArm(model_id="only", version="v1"),),
        counts={"forbidden": QualityCounts(successes=100)},
        config=LearningConfig(epsilon=1),
        draw=0.9,
    )
    assert selected.model_id == "only"
    assert selected.propensity == 1
    with pytest.raises(NoCandidates):
        choose_model(candidates=(), counts={}, config=LearningConfig())


def test_same_model_multiple_clusters_shift_with_latency_and_inflight():
    now = datetime.now(timezone.utc)
    clusters = (
        ClusterOption(cluster_id="east", model_id="m"),
        ClusterOption(cluster_id="west", model_id="m"),
    )
    states = {
        "east": ClusterState(
            observed_at=now,
            ttft_ms=1000,
            latency_ms=5000,
            samples=3,
            last_dispatch_at=now,
        ),
        "west": ClusterState(
            observed_at=now,
            ttft_ms=100,
            latency_ms=500,
            samples=3,
            last_dispatch_at=now,
        ),
    }
    assert (
        choose_cluster(
            model_id="m",
            candidates=clusters,
            states=states,
            config=LoadConfig(),
            now=now,
        ).cluster_id
        == "west"
    )
    states["west"] = states["west"].model_copy(update={"inflight": 100})
    assert (
        choose_cluster(
            model_id="m",
            candidates=clusters,
            states=states,
            config=LoadConfig(),
            now=now,
        ).cluster_id
        == "east"
    )


def test_missing_and_stale_metrics_use_priors_not_zero_and_cold_clusters_get_probes():
    now = datetime.now(timezone.utc)
    clusters = (
        ClusterOption(cluster_id="cold", model_id="m", prior_ttft_ms=1000),
        ClusterOption(cluster_id="warm", model_id="m"),
    )
    states = {
        "cold": ClusterState(last_dispatch_at=now),
        "warm": ClusterState(
            observed_at=now, ttft_ms=10, latency_ms=20, samples=1, last_dispatch_at=now
        ),
    }
    assert (
        choose_cluster(
            model_id="m",
            candidates=clusters,
            states=states,
            config=LoadConfig(),
            now=now,
        ).cluster_id
        == "warm"
    )
    probe = choose_cluster(
        model_id="m",
        candidates=clusters,
        states=states,
        config=LoadConfig(stale_after_s=300, probe_interval_s=30),
        now=now + timedelta(seconds=31),
    )
    assert probe.cluster_id == "cold"
    assert probe.reason == "probe"
    stale = {
        name: value.model_copy(
            update={
                "observed_at": now - timedelta(seconds=600),
                "last_dispatch_at": now,
            }
        )
        for name, value in states.items()
    }
    # Both use configured priors; a stale extremely fast sample cannot win forever.
    assert (
        choose_cluster(
            model_id="m",
            candidates=clusters,
            states=stale,
            config=LoadConfig(),
            now=now,
        ).cluster_id
        == "warm"
    )


def test_cooldown_disabled_wrong_model_and_active_telemetry():
    now = datetime.now(timezone.utc)
    candidates = (
        ClusterOption(cluster_id="a", model_id="m"),
        ClusterOption(cluster_id="b", model_id="m"),
        ClusterOption(cluster_id="wrong", model_id="other"),
        ClusterOption(cluster_id="disabled", model_id="m", enabled=False),
    )
    states = {
        "a": ClusterState(last_dispatch_at=now, active_load=1, active_at=now),
        "b": ClusterState(last_dispatch_at=now, active_load=0, active_at=now),
    }
    assert (
        choose_cluster(
            model_id="m",
            candidates=candidates,
            states=states,
            config=LoadConfig(),
            now=now,
        ).cluster_id
        == "b"
    )
    states["b"] = states["b"].model_copy(
        update={"cooldown_until": now + timedelta(seconds=10)}
    )
    assert (
        choose_cluster(
            model_id="m",
            candidates=candidates,
            states=states,
            config=LoadConfig(),
            now=now,
        ).cluster_id
        == "a"
    )
    states["a"] = states["a"].model_copy(
        update={"cooldown_until": now + timedelta(seconds=10)}
    )
    with pytest.raises(NoCandidates):
        choose_cluster(
            model_id="m",
            candidates=candidates,
            states=states,
            config=LoadConfig(),
            now=now,
        )
    assert choose_cluster(
        model_id="m",
        candidates=candidates,
        states=states,
        config=LoadConfig(),
        now=now + timedelta(seconds=11),
    ).cluster_id in {"a", "b"}


@pytest.mark.parametrize("timing", [True, "100", -1, float("nan"), float("inf")])
def test_wire_observations_require_finite_numeric_timings(timing):
    from uuid import uuid4

    with pytest.raises(ValidationError):
        Observation(
            event_id="e",
            decision_id=uuid4(),
            attempt_id="initial",
            cluster_id="east",
            observed_at=datetime.now(timezone.utc),
            outcome="success",
            latency_ms=timing,
        )


def test_recent_nonstream_results_do_not_refresh_an_old_ttft_sample():
    now = datetime.now(timezone.utc)
    states = {
        "a": ClusterState(
            observed_at=now,
            ttft_at=now - timedelta(seconds=600),
            ttft_ms=1,
            last_dispatch_at=now,
        ),
        "b": ClusterState(
            observed_at=now, ttft_at=now, ttft_ms=100, last_dispatch_at=now
        ),
    }
    candidates = tuple(
        ClusterOption(cluster_id=name, model_id="m", prior_ttft_ms=1000)
        for name in states
    )
    assert (
        choose_cluster(
            model_id="m",
            candidates=candidates,
            states=states,
            config=LoadConfig(),
            now=now,
        ).cluster_id
        == "b"
    )
