"""Measure real HTTP signal extraction on pinned public intent datasets.

No fine tuning. Five training examples per intent, seed42, max cosine. Keep
whole official test splits, except normalized text overlap removed with a
reported audit. BANKING77 has no official dev: reserve20% of each train class.
"""

import argparse
import asyncio
import collections
import csv
import hashlib
import json
import platform
import random
import re
import subprocess
import time
import unicodedata
from pathlib import Path

import numpy as np

from semantic_router.adaptive.config import PipelineConfig, SignalSpec
from semantic_router.adaptive.models import Message, RoutingRequest
from semantic_router.adaptive.pipeline import SignalPipeline
from semantic_router.adaptive.providers import ProviderConfig, ProviderRegistry

CLINC_REV = "828f8093932c8fe6ca7936c3d2e52903b1c523de"
BANK_REV = "57ec275d8078af65b7731c2a98be812d844a6d6b"


def wire_labels(labels):
    mapping = {
        label: label
        if re.fullmatch(r"[A-Za-z][A-Za-z0-9_-]{0,63}", label)
        else "label_" + hashlib.sha256(label.encode()).hexdigest()[:24]
        for label in labels
    }
    if len(set(mapping.values())) != len(labels):
        raise ValueError("dataset label alias collision")
    return mapping


def normalize(text):
    return " ".join(unicodedata.normalize("NFKC", text).casefold().split())


def clean_splits(data):
    seen, clean, removed = set(), {}, {}
    for split in ("train", "dev", "test"):
        clean[split] = []
        for text, label in data[split]:
            key = normalize(text)
            if key not in seen:
                clean[split].append((text, label))
                seen.add(key)
        removed[split] = len(data[split]) - len(clean[split])
    return clean, {
        "normalization": "NFKC + casefold + collapse whitespace",
        "precedence": ["train", "dev", "test"],
        "removed": removed,
        "original_counts": {k: len(v) for k, v in data.items()},
        "clean_counts": {k: len(v) for k, v in clean.items()},
    }


def load_data(root, dataset):
    if dataset == "massive-zh":
        path = root / "massive/1.0/data/zh-CN.jsonl"
        rows = [json.loads(line) for line in path.read_text().splitlines()]
        return {
            split: [(r["utt"], r["intent"]) for r in rows if r["partition"] == split]
            for split in ("train", "dev", "test")
        }
    if dataset == "clinc150":
        data = json.loads(
            (root / f"clinc--oos-eval/{CLINC_REV}/data/data_full.json").read_text()
        )
        return {
            dest: [
                (text, label)
                for text, label in data[src]
                + (data["oos_" + src] if dest != "train" else [])
            ]
            for dest, src in [("train", "train"), ("dev", "val"), ("test", "test")]
        }
    base = root / f"PolyAI-LDN--task-specific-datasets/{BANK_REV}/banking_data"

    def read(name):
        with (base / f"{name}.csv").open() as stream:
            return [(r["text"], r["category"]) for r in csv.DictReader(stream)]

    # Dedup before splitting, so a repeated utterance cannot straddle dev/train.
    groups = collections.defaultdict(list)
    seen = set()
    for text, label in read("train"):
        if normalize(text) not in seen:
            groups[label].append((text, label))
            seen.add(normalize(text))
    train, dev = [], []
    rng = random.Random(42)
    for label in sorted(groups):
        group = groups[label]
        rng.shuffle(group)
        n = max(1, len(group) // 5)
        dev.extend(group[:n])
        train.extend(group[n:])
    return {"train": train, "dev": dev, "test": read("test")}


def classification(rows, labels):
    n = len(rows)
    correct = sum(r["prediction"] == r["truth"] for r in rows)
    f1, per_label = [], {}
    for label in labels:
        tp = sum(r["prediction"] == label and r["truth"] == label for r in rows)
        fp = sum(r["prediction"] == label and r["truth"] != label for r in rows)
        fn = sum(r["prediction"] != label and r["truth"] == label for r in rows)
        score = 2 * tp / (2 * tp + fp + fn) if 2 * tp + fp + fn else 0
        f1.append(score)
        per_label[label] = {"f1": score, "support": tp + fn}
    confused = collections.Counter(
        (r["truth"], r["prediction"]) for r in rows if r["truth"] != r["prediction"]
    )
    return {
        "n": n,
        "correct": correct,
        "accuracy": correct / n if n else None,
        "macro_f1": sum(f1) / len(f1),
        "invalid": sum(r["prediction"] is None for r in rows),
        "per_label": per_label,
        "top_confusions": [
            {"truth": pair[0], "prediction": pair[1], "n": count}
            for pair, count in confused.most_common(15)
        ],
    }


def threshold(rows):
    """Maximize mean ID acceptance/OOS rejection on validation only.

    Invalid outputs are rejected. Midpoints avoid accidental floating point
    equality; ties choose the lower threshold to preserve more ID coverage.
    """
    scores = sorted({r["score"] for r in rows if r.get("score") is not None})
    candidates = [
        -1.000001,
        *[(a + b) / 2 for a, b in zip(scores, scores[1:])],
        1.000001,
    ]

    def objective(value):
        parts = [
            [r for r in rows if (r["truth"] == "oos") == oos] for oos in [False, True]
        ]
        if any(not part for part in parts):
            raise ValueError("OOS calibration requires both ID and OOS validation")
        return (
            sum(
                sum(
                    (r.get("score") is not None and r["score"] >= value) == (i == 0)
                    for r in part
                )
                / len(part)
                for i, part in enumerate(parts)
            )
            / 2
        )

    return max(candidates, key=objective)


def percentiles(values):
    return {f"p{p}_ms": float(np.percentile(values, p)) for p in [50, 95, 99]}


async def evaluate(args):
    data, audit = clean_splits(load_data(args.data_root, args.dataset))
    labels = sorted({label for _, label in data["train"]})
    aliases = wire_labels(labels)
    original_labels = {value: key for key, value in aliases.items()}
    rng = random.Random(42)
    examples = {
        label: rng.sample(
            [text for text, target in data["train"] if target == label],
            min(5, sum(target == label for _, target in data["train"])),
        )
        for label in labels
    }
    params = {"provider": "local"}
    if args.protocol == "embedding":
        params["examples"] = {
            aliases[label]: group for label, group in examples.items()
        }
    else:
        params.update(
            {
                "max_completion_tokens": 128,
                "questions": {
                    "category": {
                        "instructions": "Identify the intent of the user request.",
                        "options": {
                            aliases[label]: label.replace("_", " ") for label in labels
                        },
                    }
                },
            }
        )
    registry = ProviderRegistry(
        providers={
            "local": ProviderConfig(
                protocol=args.protocol,
                endpoint=args.endpoint,
                model=args.model_id,
                timeout_ms=60000,
                max_response_bytes=16777216,
                max_request_bytes=1048576,
            )
        }
    )
    spec = SignalSpec(
        name="intent", kind=args.protocol, timeout_ms=60000, parameters=params
    )
    registry.validate(signals=(spec,))
    pipeline = SignalPipeline(
        config=PipelineConfig(signals=(spec,), timeout_ms=60000),
        registry=registry.factories,
    )
    predictions, warmups = {}, []

    def request(text, index):
        return RoutingRequest(
            request_id=f"eval-{index}",
            tenant_id="evaluation",
            messages=(Message(role="user", content=text),),
        )

    try:
        # Cold prototype init recorded separately; ten warmups are never scored.
        for i in range(10):
            result = await pipeline.run(
                request=request(data["train"][i][0], f"warm-{i}")
            )
            warmups.append(result.elapsed_ms)
            if result.signals["intent"].status != "ok":
                print("warmup unavailable", result.signals["intent"].error, flush=True)
        for split in ["dev", "test"]:
            rows = data[split]
            if args.per_class:
                sampler = random.Random(43)
                rows = [
                    row
                    for label in sorted({label for _, label in rows})
                    for row in sampler.sample(
                        [row for row in rows if row[1] == label], args.per_class
                    )
                ]
            output = []
            for i, (text, truth) in enumerate(rows):
                result = await pipeline.run(request=request(text, f"{split}-{i}"))
                signal = result.signals["intent"]
                value = signal.value if signal.status == "ok" else {}
                predicted = value.get("category")
                score = (
                    value.get("scores", {}).get(predicted)
                    if args.protocol == "embedding"
                    else value.get("confidence")
                )
                output.append(
                    {
                        "index": i,
                        "text_sha256": hashlib.sha256(text.encode()).hexdigest(),
                        "truth": truth,
                        "prediction": original_labels.get(predicted),
                        "score": score,
                        "error": signal.error,
                        "elapsed_ms": result.elapsed_ms,
                    }
                )
                if (i + 1) % 500 == 0:
                    print(args.dataset, split, i + 1, flush=True)
            predictions[split] = output
    finally:
        await pipeline.aclose()
        await registry.aclose()
    cutoff = threshold(predictions["dev"]) if args.dataset == "clinc150" else None
    metrics = {}
    for split, rows in predictions.items():
        in_domain = [r for r in rows if r["truth"] != "oos"]
        metrics[split] = classification(in_domain, labels)
        metrics[split]["http_signal_latency"] = {
            "n": len(rows),
            **percentiles([r["elapsed_ms"] for r in rows]),
        }
        if cutoff is not None:
            accepted = [
                r for r in in_domain if r["score"] is not None and r["score"] >= cutoff
            ]
            oos = [r for r in rows if r["truth"] == "oos"]
            reject = sum(r["score"] is None or r["score"] < cutoff for r in oos)
            metrics[split]["rejection"] = {
                "threshold_from_dev": cutoff,
                "oos_n": len(oos),
                "oos_recall": reject / len(oos),
                "oos_false_accept_rate": 1 - reject / len(oos),
                "id_coverage": len(accepted) / len(in_domain),
                "accepted_id_accuracy": sum(
                    r["prediction"] == r["truth"] for r in accepted
                )
                / len(accepted)
                if accepted
                else None,
                "balanced_id_accept_oos_reject": (
                    len(accepted) / len(in_domain) + reject / len(oos)
                )
                / 2,
            }
    result = {
        "dataset": args.dataset,
        "model": args.model_id,
        "protocol": args.protocol,
        "source_commit": subprocess.check_output(
            ["git", "rev-parse", "HEAD"], text=True
        ).strip(),
        "evaluator_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
        "python": platform.python_version(),
        "seed": 42,
        "max_examples_per_class": 5 if args.protocol == "embedding" else 0,
        "prototype_counts": {label: len(group) for label, group in examples.items()}
        if args.protocol == "embedding"
        else {},
        "per_class_subset": args.per_class,
        "classes": len(labels),
        "label_aliases": {k: v for k, v in aliases.items() if k != v},
        "concurrency": 1,
        "method": "max cosine of train examples"
        if args.protocol == "embedding"
        else "zero shot strict JSON, greedy, thinking disabled",
        "score_is_calibrated_probability": False,
        "warmup_ms": warmups,
        "audit": audit,
        "metrics": metrics,
        "prototype_text_hashes": {
            label: [hashlib.sha256(text.encode()).hexdigest() for text in group]
            for label, group in examples.items()
        }
        if args.protocol == "embedding"
        else {},
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n")
    args.output.with_suffix(".predictions.json").write_text(
        json.dumps(predictions, ensure_ascii=False) + "\n"
    )
    print(
        json.dumps(
            {
                split: {
                    key: value for key, value in metric.items() if key != "per_label"
                }
                for split, metric in metrics.items()
            },
            ensure_ascii=False,
            indent=2,
        )
    )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data-root", type=Path, required=True)
    parser.add_argument(
        "--dataset", choices=["massive-zh", "clinc150", "banking77"], required=True
    )
    parser.add_argument("--endpoint", required=True)
    parser.add_argument("--model-id", required=True)
    parser.add_argument("--protocol", choices=["embedding", "llm"], default="embedding")
    parser.add_argument(
        "--per-class",
        type=int,
        default=0,
        help="0=full; positive=balanced small LLM probe",
    )
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    start = time.monotonic()
    asyncio.run(evaluate(args))
    print(f"elapsed_seconds={time.monotonic() - start:.3f}")


if __name__ == "__main__":
    main()
