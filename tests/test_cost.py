"""Per-call cost breakdown: list prices x measured quantities, per stage and per vendor/model."""

import json

import pytest

from engine.pricing import PriceList, estimate_cost, load_default_prices, load_prices


def test_default_price_list_has_sources_and_dates():
    pl = load_default_prices()
    assert pl.as_of and pl.usd_inr > 0
    for key in ("deepgram/flux-general-multi", "deepgram/nova-3", "google/gemini-2.5-flash-lite",
                "google/gemini-2.5-flash", "cartesia/sonic-3", "sarvam/bulbul:v3", "sarvam/saaras:v4",
                "elevenlabs/eleven_flash_v2_5", "google/gemini-2.5-flash-native-audio-preview-12-2025"):
        assert key in pl.models, key
        assert pl.models[key]["source"], key


def test_estimate_stt_per_minute_true_seconds():
    pl = PriceList(as_of="2026-10-23", usd_inr=84.0, models={
        "deepgram/flux-general-multi": {"stage": "stt", "unit": "minute", "usd_per_unit": 0.0078, "source": "x"},
    })
    usage = {"stt": [{"provider": "deepgram", "model": "flux-general-multi", "seconds": 90.0}]}
    cost = estimate_cost(usage, pl)
    stt = cost["stages"]["stt"]
    assert stt["lines"][0]["quantity"] == pytest.approx(1.5)             # minutes, no rounding up
    assert stt["lines"][0]["usd"] == pytest.approx(0.0117)
    assert cost["total_usd"] == pytest.approx(0.0117) and cost["total_inr"] == pytest.approx(0.0117 * 84)


def test_estimate_llm_tokens_with_cached_input():
    pl = PriceList(as_of="2026-10-23", usd_inr=84.0, models={
        "google/gemini-2.5-flash-lite": {"stage": "llm", "unit": "token", "usd_per_m_input": 0.10,
                                         "usd_per_m_cached_input": 0.01, "usd_per_m_output": 0.40, "source": "x"},
    })
    usage = {"llm": [{"provider": "google", "model": "gemini-2.5-flash-lite", "input_tokens": 12000,
                      "cached_input_tokens": 2000, "output_tokens": 360, "requests": 6}]}
    line = estimate_cost(usage, pl)["stages"]["llm"]["lines"][0]
    # 10,000 uncached x 0.10/M + 2,000 cached x 0.01/M + 360 out x 0.40/M
    assert line["usd"] == pytest.approx(0.001 + 0.00002 + 0.000144)
    assert line["detail"]["uncached_input_tokens"] == 10000


def test_estimate_tts_characters_including_cancelled():
    pl = PriceList(as_of="2026-10-23", usd_inr=84.0, models={
        "cartesia/sonic-3": {"stage": "tts", "unit": "character", "usd_per_unit": 0.000038, "source": "x"},
    })
    usage = {"tts": [{"provider": "cartesia", "model": "sonic-3", "characters": 450, "cancelled_characters": 50}]}
    line = estimate_cost(usage, pl)["stages"]["tts"]["lines"][0]
    assert line["quantity"] == 450 and line["usd"] == pytest.approx(450 * 0.000038)


def test_estimate_realtime_audio_tokens():
    pl = PriceList(as_of="2026-10-23", usd_inr=84.0, models={
        "google/gemini-2.5-flash-native-audio-preview-12-2025": {
            "stage": "realtime", "unit": "token", "usd_per_m_audio_input": 3.0, "usd_per_m_audio_output": 12.0,
            "usd_per_m_text_input": 0.5, "usd_per_m_text_output": 2.0, "source": "x"},
    })
    usage = {"realtime": [{"provider": "google", "model": "gemini-2.5-flash-native-audio-preview-12-2025",
                           "audio_input_tokens": 1500, "audio_output_tokens": 1000, "text_input_tokens": 2000,
                           "text_output_tokens": 100}]}
    line = estimate_cost(usage, pl)["stages"]["realtime"]["lines"][0]
    assert line["usd"] == pytest.approx(1500 * 3 / 1e6 + 1000 * 12 / 1e6 + 2000 * 0.5 / 1e6 + 100 * 2 / 1e6)


def test_unknown_model_is_flagged_not_guessed():
    pl = PriceList(as_of="2026-10-23", usd_inr=84.0, models={})
    cost = estimate_cost({"tts": [{"provider": "acme", "model": "voice-9", "characters": 100}]}, pl)
    line = cost["stages"]["tts"]["lines"][0]
    assert line["usd"] is None and "no price" in line["note"]
    assert cost["total_usd"] == 0.0 and cost["incomplete"] is True


def test_carrier_minutes_optional():
    pl = PriceList(as_of="2026-10-23", usd_inr=84.0, models={}, carrier_inr_per_minute={"plivo": 0.38})
    cost = estimate_cost({"carrier": {"provider": "plivo", "seconds": 125.0}}, pl)
    # Plivo bills in 30-second pulses in India: 125 s -> 150 s -> 2.5 min
    assert cost["stages"]["carrier"]["lines"][0]["quantity"] == pytest.approx(2.5)
    assert cost["stages"]["carrier"]["lines"][0]["inr"] == pytest.approx(0.95)


def test_price_list_ships_inside_the_installed_package():
    # Docker installs the package non-editable: pricing.json must be declared as package data or the API
    # cannot start there.
    import tomllib
    from pathlib import Path
    pyproject = tomllib.loads((Path(__file__).resolve().parents[1] / "pyproject.toml").read_text(encoding="utf-8"))
    assert "pricing.json" in pyproject["tool"]["setuptools"]["package-data"]["engine"]


def test_price_list_is_validated_at_load(tmp_path):
    bad = {"as_of": "2026-10-23", "usd_inr": 84, "models": {"google/x": {"stage": "llm", "unit": "token", "usd_per_m_input": 1}}}
    p = tmp_path / "p.json"
    p.write_text(json.dumps(bad), encoding="utf-8")
    with pytest.raises(ValueError, match="google/x"):
        load_prices(str(p))
    p.write_text(json.dumps({"as_of": "2026-10-23", "usd_inr": 0, "models": {}}), encoding="utf-8")
    with pytest.raises(ValueError, match="usd_inr"):
        load_prices(str(p))


def test_inr_totals_are_the_sum_of_the_displayed_lines():
    pl = PriceList(as_of="x", usd_inr=84.0, models={"cartesia/sonic-3": {"stage": "tts", "unit": "character",
                                                                           "usd_per_unit": 0.000038, "source": "x"}})
    usage = {"tts": [{"provider": "cartesia", "model": "sonic-3", "characters": 1} for _ in range(7)]}
    out = estimate_cost(usage, pl)
    lines = out["stages"]["tts"]["lines"]
    assert out["stages"]["tts"]["inr"] == pytest.approx(sum(line["inr"] for line in lines))
    assert out["total_inr"] == pytest.approx(sum(s["inr"] for s in out["stages"].values()))


def test_empty_usage_still_prices_the_carrier_leg():
    pl = PriceList(as_of="x", usd_inr=84.0, models={}, carrier_inr_per_minute={"plivo": 0.38})
    out = estimate_cost({"carrier": {"provider": "plivo", "seconds": 12.0}}, pl)
    assert out["stages"]["carrier"]["lines"][0]["inr"] == pytest.approx(0.19)
    assert out["incomplete"] is False and out["models_usd"] == 0
