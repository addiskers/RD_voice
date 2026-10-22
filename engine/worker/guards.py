"""Text guards applied between the LLM and TTS (cascaded mode only).

These are the deterministic replacement for the prompt-only mitigations the EO stack needed:
  - single_closing : keep the first closing sentence of a turn, drop later ones ("fused closings")
  - suppress_repeats: drop a sentence that repeats an earlier sentence of the same turn (or any
                      5-word run already spoken in this turn)
  - max_sentences  : hard cap per turn

``SentenceGate`` is the streaming form: feed LLM text deltas, get back only the sentences that
may be spoken, in order, as soon as each sentence boundary is known.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Iterable, Optional

_SENT_END = re.compile(r"(?<=[.!?।])\s+|(?<=[.!?।])$")
_WORD = re.compile(r"[a-z0-9ऀ-ॿ']+")


def split_sentences(text: str) -> list[str]:
    parts = [p.strip() for p in _SENT_END.split(text or "") if p and p.strip()]
    return parts


def _norm(s: str) -> str:
    return " ".join(_WORD.findall(s.lower()))


@dataclass
class GuardPolicy:
    enabled: bool = True
    max_sentences: int = 4
    single_closing: bool = True
    closing_markers: tuple[str, ...] = (
        "see you", "look forward", "have a great", "have a good", "thank you for your time",
        "thank you for letting", "thanks for letting", "thank you for confirming", "take care",
        "anything else i can help", "count you in", "goodbye", "bye for now",
    )
    suppress_repeats: bool = True

    @classmethod
    def from_config(cls, cfg) -> "GuardPolicy":
        if cfg is None:
            return cls()
        return cls(enabled=cfg.enabled, max_sentences=cfg.max_sentences, single_closing=cfg.single_closing,
                   closing_markers=tuple(m.lower() for m in cfg.closing_markers),
                   suppress_repeats=cfg.suppress_repeats)

    def is_closing(self, sentence: str) -> bool:
        s = sentence.lower()
        return any(m in s for m in self.closing_markers)


@dataclass
class TurnGuardState:
    """Per-turn memory used by both the batch and streaming guard."""
    policy: GuardPolicy
    spoken: list[str] = field(default_factory=list)
    seen_norm: set[str] = field(default_factory=set)
    seen_grams: set[str] = field(default_factory=set)
    closing_said: bool = False
    dropped: list[str] = field(default_factory=list)

    def admit(self, sentence: str) -> Optional[str]:
        """Return the sentence if it may be spoken, else None (and remember why)."""
        p = self.policy
        if not p.enabled:
            self.spoken.append(sentence)
            return sentence
        if len(self.spoken) >= p.max_sentences:
            self.dropped.append(sentence)
            return None
        n = _norm(sentence)
        if p.suppress_repeats and n:
            if n in self.seen_norm:
                self.dropped.append(sentence)
                return None
            words = n.split()
            grams = {" ".join(words[i:i + 5]) for i in range(len(words) - 4)} if len(words) >= 5 else set()
            if grams & self.seen_grams:
                self.dropped.append(sentence)
                return None
        if p.single_closing and p.is_closing(sentence):
            if self.closing_said:
                self.dropped.append(sentence)
                return None
            self.closing_said = True
        self.spoken.append(sentence)
        if n:
            self.seen_norm.add(n)
            words = n.split()
            self.seen_grams |= {" ".join(words[i:i + 5]) for i in range(len(words) - 4)}
        return sentence


def guard_text(text: str, policy: Optional[GuardPolicy] = None) -> str:
    """Batch form: return the guarded text for one agent turn."""
    state = TurnGuardState(policy or GuardPolicy())
    kept = [s for s in (state.admit(sent) for sent in split_sentences(text)) if s]
    return " ".join(kept)


class SentenceGate:
    """Streaming form. ``feed(delta)`` yields sentences cleared for TTS; ``flush()`` yields the tail."""

    def __init__(self, policy: Optional[GuardPolicy] = None):
        self.state = TurnGuardState(policy or GuardPolicy())
        self._buf = ""

    def feed(self, delta: str) -> Iterable[str]:
        self._buf += delta or ""
        out: list[str] = []
        while True:
            m = _SENT_END.search(self._buf)
            if not m or m.end() == 0 or m.end() > len(self._buf):
                break
            # only split when the boundary is followed by more text (or a trailing space) so we
            # do not cut on a period that is still being streamed ("Dr." / "3.5")
            end = m.end()
            if end >= len(self._buf) and not self._buf.endswith((" ", "\n")):
                break
            sentence, self._buf = self._buf[:end].strip(), self._buf[end:]
            if sentence:
                kept = self.state.admit(sentence)
                if kept:
                    out.append(kept)
        return out

    def flush(self) -> Iterable[str]:
        tail, self._buf = self._buf.strip(), ""
        if not tail:
            return []
        kept = self.state.admit(tail)
        return [kept] if kept else []

    @property
    def dropped(self) -> list[str]:
        return self.state.dropped
