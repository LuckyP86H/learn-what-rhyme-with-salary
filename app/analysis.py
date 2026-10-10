"""The "business logic": fake transcription/diarization and the aggregation of chunk results.

Kept free of Celery and database imports so it is trivially unit-testable. Everything here is a pure,
*deterministic* function of (call_id, chunk index): re-running a chunk after a crash must produce the
same answer, which is what makes at-least-once delivery harmless.
"""
from __future__ import annotations

import random
from dataclasses import dataclass
from typing import Any

_PHRASES = (
    "thanks for calling, how can I help",
    "I was charged twice this month",
    "let me look into that for you",
    "that is really frustrating",
    "I appreciate your patience",
    "can you confirm the account email",
    "perfect, that fixed it",
    "I would like to cancel my plan",
)


@dataclass(frozen=True)
class ChunkSimulation:
    duration_s: float  # how long the fake processing takes (wall clock)
    result: dict[str, Any]


def simulate_chunk(call_id: str, index: int, min_s: float, max_s: float) -> ChunkSimulation:
    # A str seed is hashed deterministically by random.Random, unlike hash(), which is salted per process.
    rng = random.Random(f"{call_id}:{index}")
    duration = rng.uniform(min_s, max_s)
    audio_seconds = 60.0
    agent_share = rng.uniform(0.35, 0.65)
    result = {
        "chunk_index": index,
        "audio_seconds": audio_seconds,
        "word_count": rng.randint(90, 180),
        "sentiment": round(rng.uniform(-1.0, 1.0), 3),
        "speakers": {
            "agent": round(audio_seconds * agent_share, 1),
            "customer": round(audio_seconds * (1 - agent_share), 1),
        },
        "transcript": ". ".join(rng.sample(_PHRASES, 3)),
    }
    return ChunkSimulation(duration_s=round(duration, 2), result=result)


def sentiment_label(score: float) -> str:
    if score > 0.2:
        return "positive"
    if score < -0.2:
        return "negative"
    return "neutral"


def merge_results(chunk_results: list[dict[str, Any]]) -> dict[str, Any]:
    """Aggregate per-chunk results into the call-level summary record."""
    if not chunk_results:
        raise ValueError("cannot merge zero chunk results")
    ordered = sorted(chunk_results, key=lambda r: r["chunk_index"])
    total_audio = sum(r["audio_seconds"] for r in ordered)
    sentiment = sum(r["sentiment"] * r["audio_seconds"] for r in ordered) / total_audio
    agent = sum(r["speakers"]["agent"] for r in ordered)
    customer = sum(r["speakers"]["customer"] for r in ordered)
    label = sentiment_label(sentiment)
    return {
        "chunks": len(ordered),
        "audio_seconds": total_audio,
        "word_count": sum(r["word_count"] for r in ordered),
        "sentiment": round(sentiment, 3),
        "sentiment_label": label,
        "talk_time": {"agent": round(agent, 1), "customer": round(customer, 1)},
        "agent_talk_ratio": round(agent / (agent + customer), 3),
        "text": (
            f"{len(ordered)} chunks / {total_audio:.0f}s of audio; overall sentiment {label} "
            f"({sentiment:+.2f}); agent talked {agent / (agent + customer):.0%} of the time."
        ),
    }
