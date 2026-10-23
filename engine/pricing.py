"""Per-call cost estimate: measured quantities x vendor list prices, line by line per stage and model.

Quantities come from the worker (``usage_detail`` on the ``ended`` event): seconds streamed to the STT,
tokens per LLM request (uncached, cached, output), characters sent to the TTS (cancelled synthesis
included: vendors bill it), realtime tokens by modality. Prices live in ``engine/pricing.json`` (or the
file ``PRICING_FILE`` points at), each with its source URL and the date it was read. A model with no
price produces a flagged line, never a guess.
"""

from __future__ import annotations

import json
import math
import os
from dataclasses import dataclass, field
from typing import Any

DEFAULT_PRICING_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "pricing.json")


@dataclass
class PriceList:
    as_of: str
    usd_inr: float
    models: dict[str, dict[str, Any]] = field(default_factory=dict)
    carrier_inr_per_minute: dict[str, float] = field(default_factory=dict)
    note: str = ""
    path: str = ""

    def price(self, provider: str, model: str) -> dict[str, Any] | None:
        key = f"{(provider or '').lower()}/{model or ''}"
        if key in self.models:
            return self.models[key]
        for k, v in self.models.items():      # case-insensitive match ("Gemini-2.5-Flash")
            if k.lower() == key.lower():
                return v
        return None


_REQUIRED = {"stt": (("usd_per_unit", "inr_per_unit"),), "tts": (("usd_per_unit", "inr_per_unit"),),
             "llm": (("usd_per_m_input",), ("usd_per_m_output",)),
             "realtime": (("usd_per_m_audio_input",), ("usd_per_m_audio_output",), ("usd_per_m_text_input",),
                          ("usd_per_m_text_output",))}


def validate_prices(pl: PriceList) -> None:
    """Fail at startup, with the offending key named, rather than at the first hangup."""
    if not pl.usd_inr or pl.usd_inr <= 0:
        raise ValueError(f"pricing: usd_inr must be > 0 (got {pl.usd_inr!r}) in {pl.path}")
    for key, price in pl.models.items():
        stage = price.get("stage")
        if stage not in _REQUIRED:
            raise ValueError(f"pricing: model {key!r} has unknown stage {stage!r} (expected one of {sorted(_REQUIRED)})")
        for alternatives in _REQUIRED[stage]:
            if not any(a in price for a in alternatives):
                raise ValueError(f"pricing: model {key!r} ({stage}) is missing {' or '.join(alternatives)}")


def load_prices(path: str | None = None, usd_inr: float | None = None) -> PriceList:
    p = path or DEFAULT_PRICING_FILE
    with open(p, encoding="utf-8") as f:
        raw = json.load(f)
    pl = PriceList(as_of=str(raw.get("as_of", "")), usd_inr=float(usd_inr or raw.get("usd_inr") or 0.0),
                   models=dict(raw.get("models") or {}),
                   carrier_inr_per_minute={k: float(v) for k, v in (raw.get("carrier_inr_per_minute") or {}).items()},
                   note=str(raw.get("note", "")), path=p)
    validate_prices(pl)
    return pl


def load_default_prices() -> PriceList:
    return load_prices()


def _per_unit_usd(price: dict[str, Any], pl: PriceList) -> float | None:
    if "usd_per_unit" in price:
        return float(price["usd_per_unit"])
    if "inr_per_unit" in price:
        return float(price["inr_per_unit"]) / pl.usd_inr
    return None


def _line(provider: str, model: str, quantity: float, unit: str, usd: float | None, pl: PriceList, *,
          unit_price_usd: float | None = None, detail: dict[str, Any] | None = None, note: str = "",
          source: str = "", rates: dict[str, float] | None = None) -> dict[str, Any]:
    return {"provider": provider, "model": model, "quantity": round(quantity, 4), "unit": unit,
            "unit_price_usd": unit_price_usd, "rates": rates or {}, "usd": None if usd is None else round(usd, 6),
            "inr": None if usd is None else round(usd * pl.usd_inr, 4), "detail": detail or {}, "note": note,
            "source": source}


def _rates(price: dict[str, Any], keys: tuple[str, ...]) -> dict[str, float]:
    return {k: float(price[k]) for k in keys if k in price}


def estimate_cost(usage: dict[str, Any], pl: PriceList) -> dict[str, Any]:
    stages: dict[str, dict[str, Any]] = {}
    incomplete = False
    notes: list[str] = []

    def add(stage: str, line: dict[str, Any]) -> None:
        # stage and call totals are sums of the displayed (rounded) lines, so the table always adds up
        stages.setdefault(stage, {"lines": [], "usd": 0.0, "inr": 0.0})
        stages[stage]["lines"].append(line)
        if line["usd"] is not None:
            stages[stage]["usd"] = round(stages[stage]["usd"] + line["usd"], 6)
            stages[stage]["inr"] = round(stages[stage]["inr"] + line["inr"], 4)

    for u in usage.get("stt") or []:
        price = pl.price(u.get("provider", ""), u.get("model", ""))
        minutes = float(u.get("seconds") or 0.0) / 60.0
        if price and price.get("stage") == "stt":
            rate = _per_unit_usd(price, pl)
            add("stt", _line(u["provider"], u["model"], minutes, "minute", minutes * rate, pl, unit_price_usd=rate,
                             detail={"seconds_streamed": u.get("seconds"), "reported_seconds": u.get("reported_seconds")},
                             note=price.get("billing", ""), source=price.get("source", "")))
        else:
            incomplete = True
            add("stt", _line(u.get("provider", "?"), u.get("model", "?"), minutes, "minute", None, pl,
                             note="no price on file for this STT model: add it to pricing.json"))

    for u in usage.get("llm") or []:
        price = pl.price(u.get("provider", ""), u.get("model", ""))
        inp = int(u.get("input_tokens") or 0)
        cached = min(int(u.get("cached_input_tokens") or 0), inp)
        out = int(u.get("output_tokens") or 0)
        detail = {"requests": u.get("requests"), "input_tokens": inp, "uncached_input_tokens": inp - cached,
                  "cached_input_tokens": cached, "output_tokens": out, "thinking_tokens": int(u.get("thinking_tokens") or 0)}
        if price and price.get("stage") == "llm":
            usd = ((inp - cached) * float(price["usd_per_m_input"]) + cached * float(price.get("usd_per_m_cached_input",
                   price["usd_per_m_input"])) + out * float(price["usd_per_m_output"])) / 1e6
            add("llm", _line(u["provider"], u["model"], inp + out, "token", usd, pl, detail=detail,
                             note=price.get("billing", ""), source=price.get("source", ""),
                             rates=_rates(price, ("usd_per_m_input", "usd_per_m_cached_input", "usd_per_m_output"))))
        else:
            incomplete = True
            add("llm", _line(u.get("provider", "?"), u.get("model", "?"), inp + out, "token", None, pl, detail=detail,
                             note="no price on file for this LLM: add it to pricing.json"))

    for u in usage.get("tts") or []:
        price = pl.price(u.get("provider", ""), u.get("model", ""))
        chars = int(u.get("characters") or 0)
        detail = {"requests": u.get("requests"), "characters": chars, "cancelled_characters": u.get("cancelled_characters", 0),
                  "audio_seconds": u.get("audio_seconds")}
        if price and price.get("stage") == "tts":
            rate = _per_unit_usd(price, pl)
            add("tts", _line(u["provider"], u["model"], chars, "character", chars * rate, pl, unit_price_usd=rate,
                             detail=detail, note=price.get("billing", ""), source=price.get("source", "")))
        else:
            incomplete = True
            add("tts", _line(u.get("provider", "?"), u.get("model", "?"), chars, "character", None, pl, detail=detail,
                             note="no price on file for this TTS model: add it to pricing.json"))

    for u in usage.get("realtime") or []:
        price = pl.price(u.get("provider", ""), u.get("model", ""))
        ai, ao = int(u.get("audio_input_tokens") or 0), int(u.get("audio_output_tokens") or 0)
        ti, to = int(u.get("text_input_tokens") or 0), int(u.get("text_output_tokens") or 0)
        detail = {"requests": u.get("requests"), "audio_input_tokens": ai, "audio_output_tokens": ao,
                  "text_input_tokens": ti, "text_output_tokens": to, "cached_input_tokens": u.get("cached_input_tokens", 0)}
        if price and price.get("stage") == "realtime":
            usd = (ai * float(price["usd_per_m_audio_input"]) + ao * float(price["usd_per_m_audio_output"])
                   + ti * float(price["usd_per_m_text_input"]) + to * float(price["usd_per_m_text_output"])) / 1e6
            note = price.get("billing", "") + (" (price marked verify)" if price.get("verify") else "")
            add("realtime", _line(u["provider"], u["model"], ai + ao + ti + to, "token", usd, pl, detail=detail,
                                  note=note, source=price.get("source", ""),
                                  rates=_rates(price, ("usd_per_m_audio_input", "usd_per_m_audio_output",
                                                       "usd_per_m_text_input", "usd_per_m_text_output"))))
        else:
            incomplete = True
            add("realtime", _line(u.get("provider", "?"), u.get("model", "?"), ai + ao + ti + to, "token", None, pl,
                                  detail=detail, note="no price on file for this realtime model: add it to pricing.json"))

    carrier = usage.get("carrier")
    if carrier and carrier.get("seconds") is not None:
        provider = str(carrier.get("provider") or "")
        rate_inr = pl.carrier_inr_per_minute.get(provider.lower())
        seconds = float(carrier["seconds"])
        pulses = math.ceil(seconds / 30.0) if seconds > 0 else 0          # Indian carriers bill in 30 s pulses
        minutes = pulses * 0.5
        if rate_inr is not None:
            usd = minutes * rate_inr / pl.usd_inr
            add("carrier", _line(provider, "pstn", minutes, "minute", usd, pl, unit_price_usd=rate_inr / pl.usd_inr,
                                 detail={"seconds": seconds, "billed_pulses_30s": pulses, "inr_per_minute": rate_inr},
                                 note="30-second billing pulse"))
        else:
            notes.append(f"carrier '{provider}' has no rate in pricing.json; carrier cost not included")

    total_usd = round(sum(s["usd"] for s in stages.values()), 6)
    total_inr = round(sum(s["inr"] for s in stages.values()), 4)
    return {"as_of": pl.as_of, "price_list": os.path.basename(pl.path or DEFAULT_PRICING_FILE), "usd_inr": pl.usd_inr,
            "stages": stages, "total_usd": total_usd, "total_inr": total_inr,
            "models_usd": round(sum(s["usd"] for k, s in stages.items() if k != "carrier"), 6),
            "incomplete": incomplete, "notes": notes,
            "disclaimer": "estimate: measured quantities x public list prices on " + pl.as_of}
