"""Evaluation must count invalid predictions and keep held-out text disjoint."""

import pytest

from benchmarks.adaptive.evaluate import (
    classification,
    clean_splits,
    threshold,
    wire_labels,
)


def test_dedup_removes_normalized_overlap_and_reports_it():
    data = {
        "train": [("Hello world", "a"), ("hello  world", "a")],
        "dev": [(" HELLO world ", "a"), ("new", "b")],
        "test": [("new", "b"), ("held out", "a")],
    }
    clean, audit = clean_splits(data)
    assert clean == {
        "train": [("Hello world", "a")],
        "dev": [("new", "b")],
        "test": [("held out", "a")],
    }
    assert audit["removed"] == {"train": 1, "dev": 1, "test": 1}


def test_invalid_predictions_count_as_errors_in_accuracy_and_macro_f1():
    rows = [{"truth": "a", "prediction": "a"}, {"truth": "b", "prediction": None}]
    result = classification(rows, ["a", "b"])
    assert result["accuracy"] == 0.5
    assert result["macro_f1"] == 0.5
    assert result["invalid"] == 1


def test_rejection_threshold_uses_balanced_validation_recall():
    rows = [
        {"truth": "a", "score": 0.9},
        {"truth": "a", "score": 0.8},
        {"truth": "oos", "score": 0.7},
    ]
    assert threshold(rows) == pytest.approx(0.75)


def test_dataset_labels_with_punctuation_round_trip_without_dropping_a_class():
    mapping = wire_labels(["card_payment", "reverted_card_payment?"])
    assert mapping["card_payment"] == "card_payment"
    assert "?" not in mapping["reverted_card_payment?"]
    assert len(set(mapping.values())) == 2
    assert {value: key for key, value in mapping.items()}[
        mapping["reverted_card_payment?"]
    ] == "reverted_card_payment?"
