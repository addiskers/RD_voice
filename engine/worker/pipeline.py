"""Pipeline factory: AgentConfig -> LiveKit AgentSession (the whole "provider adapter" layer).

Provider constructors are called with only the kwargs they accept (signature-filtered), so an
agent config can carry provider-specific ``options`` without the engine knowing every parameter.
"""

from __future__ import annotations

import inspect
import logging
from typing import Any, Callable, Optional

from livekit.agents import AgentSession

from engine.config import Settings
from engine.schemas.agent import AgentConfig, Mode, ProviderRef, TurnConfig, VoiceRef
from engine.worker.breaker import BREAKER, FLUX_KEY

logger = logging.getLogger(__name__)


def _construct(factory: Callable[..., Any], **candidates: Any) -> Any:
    """Call ``factory`` with the subset of ``candidates`` its signature accepts (drops None)."""
    try:
        sig = inspect.signature(factory)
        accepts_var_kw = any(p.kind == inspect.Parameter.VAR_KEYWORD for p in sig.parameters.values())
        allowed = set(sig.parameters)
    except (TypeError, ValueError):
        accepts_var_kw, allowed = True, set()
    kwargs = {k: v for k, v in candidates.items() if v is not None and (accepts_var_kw or k in allowed)}
    dropped = [k for k, v in candidates.items() if v is not None and k not in kwargs]
    if dropped:
        logger.debug("%s: ignoring unsupported options %s", getattr(factory, "__qualname__", factory), dropped)
    return factory(**kwargs)


def _key(ref: ProviderRef, settings: Settings) -> Optional[str]:
    return ref.api_key or settings.provider_key(ref.provider) or None


# ----------------------------------------------------------------------------- failover helpers
def resolve_fallback(kind: str, ref: ProviderRef, settings: Settings) -> Optional[str]:
    """Second vendor for ``kind`` ("stt" | "llm"): the agent's ``fallback`` wins, else the engine default.
    None when disabled, equal to the primary, or not keyed on this engine."""
    fb = ref.fallback if ref.fallback is not None else getattr(settings, f"{kind}_fallback_provider", "")
    fb = (fb or "").strip().lower()
    if not fb or fb == ref.provider.lower() or not settings.provider_key(fb):
        return None
    return fb


def is_flux(ref: ProviderRef, settings: Settings) -> bool:
    return ref.provider.lower() == "deepgram" and (ref.model or settings.default_stt_model).startswith("flux")


_PROVIDER_ALIASES = {"gemini": "google", "eleven": "elevenlabs"}
LLM_ATTEMPT_TIMEOUT_S = 15.0        # per-attempt deadline inside the LLM FallbackAdapter (Gemini needs >= 10 s)
TTS_ATTEMPT_TIMEOUT_S = 6.0         # per-attempt deadline, also each vendor's socket receive timeout (LiveKit: 10 s); 3 s dropped
                                    # Sarvam mid-sentence on 2026-10-23, Cartesia hard failures (402/429) surface in <0.5 s anyway


def _fast_failover_adapter(instances: list[Any]):
    """LiveKit's TTS FallbackAdapter with a 6 s per-attempt deadline and no retries on the failing vendor.
    Measured 2026-10-23: with the primary refusing every connection (402/429) the stock adapter still waited
    its 10 s default before switching, so every call opened with 10 s of silence."""
    import dataclasses

    from livekit.agents import tts as lk_tts
    from livekit.agents.tts.fallback_adapter import DEFAULT_FALLBACK_API_CONNECT_OPTIONS

    def _cap(conn_options):
        return dataclasses.replace(conn_options, timeout=min(conn_options.timeout, TTS_ATTEMPT_TIMEOUT_S))

    class FastFailoverTTS(lk_tts.FallbackAdapter):
        def synthesize(self, text: str, *, conn_options=DEFAULT_FALLBACK_API_CONNECT_OPTIONS):
            return super().synthesize(text, conn_options=_cap(conn_options))

        def stream(self, *, conn_options=DEFAULT_FALLBACK_API_CONNECT_OPTIONS):
            return super().stream(conn_options=_cap(conn_options))

    return FastFailoverTTS(instances, max_retry_per_tts=0)


def provider_id(component: Any) -> str:
    """Engine provider id ("deepgram", "google", ...) of a LiveKit STT/LLM/TTS instance."""
    if type(component).__name__ == "StreamAdapter" and getattr(component, "_stt", None) is not None:
        component = component._stt            # non-streaming STT wrapped by the FallbackAdapter
    label = str(getattr(component, "provider", None) or getattr(component, "label", None) or "").strip().lower()
    known = ("groq", "deepgram", "sarvam", "cartesia", "elevenlabs", "anthropic", "openai", "google", "gemini")
    if "." in label or "/" in label:          # a hostname such as api.openai.com / api.groq.com
        name = next((k for k in known if k in label), "")
    else:
        name = label
    if not name:                               # plugin module path: livekit.plugins.<vendor>.<module>
        mod = type(component).__module__.split(".")
        name = mod[2] if len(mod) > 2 and mod[:2] == ["livekit", "plugins"] else type(component).__name__.lower()
    return _PROVIDER_ALIASES.get(name, name)


_SARVAM_LANGS = ("bn-IN", "en-IN", "gu-IN", "hi-IN", "kn-IN", "ml-IN", "mr-IN", "od-IN", "pa-IN", "ta-IN", "te-IN")


def sarvam_language(code: Optional[str], *, default: str = "en-IN") -> str:
    """Sarvam accepts only regionalised Indian codes ("en-IN", "hi-IN", ...). A bare code such as the "en" a
    Cartesia agent is configured with (and inherits when Sarvam is the fallback voice) is regionalised; anything
    Sarvam does not support falls back to ``default``."""
    c = (code or "").strip().replace("_", "-")
    if not c:
        return default
    if c in _SARVAM_LANGS:
        return c
    base = c.split("-")[0].lower()
    if base == "or":
        base = "od"
    for lang in _SARVAM_LANGS:
        if lang.split("-")[0] == base:
            return lang
    return default


def unwrap_stt(obj: Any) -> Any:
    """The primary STT behind a FallbackAdapter (or ``obj`` itself)."""
    return getattr(obj, "_stt_instances", [obj])[0]


def unwrap_llm(obj: Any) -> Any:
    return getattr(obj, "_llm_instances", [obj])[0]


def model_labels(session: AgentSession) -> dict[str, str]:
    """``{"stt": "deepgram nova-3", "llm": "google gemini-2.5-flash-lite", "tts": "cartesia sonic-3"}`` for the
    primary component of each stage (what the call was configured with)."""
    out: dict[str, str] = {}
    for kind in ("stt", "llm", "tts"):
        comp = getattr(session, kind, None)
        if comp is None:
            continue
        primary = getattr(comp, f"_{kind}_instances", [comp])[0]
        model = getattr(primary, "model", None)
        try:
            model = str(model) if model else ""
        except Exception:
            model = ""
        out[kind] = (provider_id(primary) + (" " + model if model else "")).strip()
    return out


def provider_labels(session: AgentSession) -> dict[str, str]:
    """Vendor currently serving each stage (looks through FallbackAdapters)."""
    out: dict[str, str] = {}
    for kind in ("stt", "llm", "tts"):
        comp = getattr(session, kind, None)
        if comp is None:
            continue
        out[kind] = provider_id(getattr(comp, "_active_instance", comp))
    return out


# ----------------------------------------------------------------------------- STT
def build_stt(ref: ProviderRef, settings: Settings, language: str, *, vad: Any = None, sample_rate: int = 16000):
    """Primary STT, wrapped in LiveKit's FallbackAdapter when a second vendor resolves. Flux is never
    wrapped (its end-of-turn events drive turn detection, which the adapter cannot carry); the circuit
    breaker in ``make_session`` substitutes nova-3 for new calls after a Flux failure instead."""
    primary = _build_stt_single(ref, settings, language, sample_rate=sample_rate)
    fb = resolve_fallback("stt", ref, settings)
    if fb is None or is_flux(ref, settings):
        return primary
    try:
        fallback = _build_stt_single(ProviderRef(provider=fb), settings, language, sample_rate=sample_rate)
        from livekit.agents import stt as lk_stt
        return lk_stt.FallbackAdapter([primary, fallback], vad=vad)
    except Exception as e:
        logger.warning("STT fallback '%s' unavailable: %s", fb, e)
        return primary


def _build_stt_single(ref: ProviderRef, settings: Settings, language: str, *, sample_rate: int = 16000):
    p = ref.provider.lower()
    opts = dict(ref.options)
    if p == "deepgram":
        from livekit.plugins import deepgram
        model = ref.model or settings.default_stt_model
        if model.startswith("flux"):
            # Flux: the STT emits end-of-turn itself (turn_detection="stt"); eager EOT feeds preemptive generation
            hints = opts.pop("language_hint", None) or [h.strip() for h in settings.stt_language_hints.split(",") if h.strip()]
            opts.pop("language", None)
            # 0.6 = commit a little earlier on ambiguous endings; 1800 ms = worst case for a mid-sentence pause
            # (Deepgram's default 3000 ms showed up as 3.0 s turns on live mic tests, 2026-09-08)
            return _construct(deepgram.STTv2, model=model, eot_threshold=opts.pop("eot_threshold", 0.6),
                              eager_eot_threshold=opts.pop("eager_eot_threshold", 0.4),
                              eot_timeout_ms=int(opts.pop("eot_timeout_ms", 1800)),
                              language_hint=hints if model == "flux-general-multi" else None,
                              sample_rate=sample_rate, api_key=_key(ref, settings), **opts)
        return _construct(deepgram.STT, model=model, language=opts.pop("language", settings.deepgram_language),
                          sample_rate=sample_rate, api_key=_key(ref, settings), **opts)
    if p == "sarvam":
        from livekit.plugins import sarvam
        return _construct(sarvam.STT, model=ref.model or "saaras:v4", language=opts.pop("language", language),
                          sample_rate=sample_rate, api_key=_key(ref, settings), **opts)
    if p == "google":
        from livekit.plugins import google
        return _construct(google.STT, model=ref.model, languages=opts.pop("languages", language),
                          credentials_info=opts.pop("credentials_info", None), **opts)
    if p == "openai":
        from livekit.plugins import openai
        return _construct(openai.STT, model=ref.model or "gpt-4o-transcribe", language=opts.pop("language", None),
                          api_key=_key(ref, settings), **opts)
    raise ValueError(f"unsupported STT provider '{ref.provider}'")


# ----------------------------------------------------------------------------- LLM
def build_llm(ref: ProviderRef, settings: Settings):
    """Primary LLM, wrapped in LiveKit's FallbackAdapter when a second vendor resolves."""
    primary = _build_llm_single(ref, settings)
    fb = resolve_fallback("llm", ref, settings)
    if fb is None:
        return primary
    try:
        fallback = _build_llm_single(ProviderRef(provider=fb), settings)
        from livekit.agents import llm as lk_llm
        # the adapter's default 5 s attempt deadline is rejected by Gemini ("minimum allowed deadline is 10s")
        return lk_llm.FallbackAdapter([primary, fallback], attempt_timeout=LLM_ATTEMPT_TIMEOUT_S)
    except Exception as e:
        logger.warning("LLM fallback '%s' unavailable: %s", fb, e)
        return primary


def _build_llm_single(ref: ProviderRef, settings: Settings):
    p = ref.provider.lower()
    opts = dict(ref.options)
    if p in ("google", "gemini"):
        from livekit.plugins import google
        model = ref.model or settings.default_llm_model
        thinking = opts.pop("thinking_config", None)
        budget = opts.pop("thinking_budget", settings.default_llm_thinking_budget)
        level = opts.pop("thinking_level", None)
        if thinking is None:
            if "gemini-3" in model:
                thinking = {"thinking_level": level or "minimal"}      # Gemini 3 takes a level, not a budget
            elif "2.5" in model and budget is not None and int(budget) >= 0:
                thinking = {"thinking_budget": int(budget)}            # 0 = answer without hidden reasoning
        return _construct(google.LLM, model=model, api_key=_key(ref, settings), thinking_config=thinking, **opts)
    if p == "anthropic":
        from livekit.plugins import anthropic
        return _construct(anthropic.LLM, model=ref.model or "claude-haiku-4-5", api_key=_key(ref, settings), **opts)
    if p == "openai":
        from livekit.plugins import openai
        return _construct(openai.LLM, model=ref.model or "gpt-4o-mini", api_key=_key(ref, settings), **opts)
    if p == "groq":
        from livekit.plugins import openai
        # Groq retired its Llama 3.x endpoints (404 on 2026-09-08); gpt-oss-20b is the current fast tier
        return _construct(openai.LLM, model=ref.model or "openai/gpt-oss-20b", api_key=_key(ref, settings),
                          base_url=opts.pop("base_url", "https://api.groq.com/openai/v1"), **opts)
    if p == "sarvam":
        from livekit.plugins import sarvam
        return _construct(sarvam.LLM, model=ref.model or "sarvam-m", api_key=_key(ref, settings), **opts)
    raise ValueError(f"unsupported LLM provider '{ref.provider}'")


# ----------------------------------------------------------------------------- TTS
def build_tts(ref: VoiceRef, settings: Settings, language: str):
    """Primary TTS wrapped in LiveKit's FallbackAdapter when a second vendor is configured and keyed:
    a vendor outage (e.g. Sarvam 402 "credits exhausted", seen 2026-09-08) then switches voices instead of
    leaving the call silent."""
    primary = _build_tts_single(ref, settings, language)
    fb = tts_fallback_vendor(ref.provider, settings)
    if not fb:
        return primary
    try:
        fallback = _build_tts_single(VoiceRef(provider=fb, language=ref.language or None), settings, language)
        # switch on the first failed request, within 3 s: on a phone call silence is worse than a voice change,
        # and a vendor concurrency cap (Cartesia: 2 parallel streams on a starter plan, measured 2026-10-23)
        # fails every request until load drops, so retries only add seconds. The adapter keeps probing the
        # primary in the background and moves back when it recovers.
        return _fast_failover_adapter([primary, fallback])
    except Exception as e:
        logger.warning("TTS fallback '%s' unavailable: %s", fb, e)
        return primary


def tts_fallback_vendor(primary: str, settings: Settings) -> str:
    """The second voice vendor: the configured one, or, when it is the primary itself (the default
    'cartesia' for a cartesia agent, which is no fallback at all), the other keyed Indian-capable vendor."""
    primary = (primary or "").lower()
    fb = (getattr(settings, "tts_fallback_provider", "") or "").lower()
    if fb == primary:
        fb = next((v for v in ("sarvam", "cartesia", "elevenlabs") if v != primary and settings.provider_key(v)), "")
    if not fb or fb == primary or not settings.provider_key(fb):
        return ""
    return fb


def _build_tts_single(ref: VoiceRef, settings: Settings, language: str):
    p = ref.provider.lower()
    opts = dict(ref.options)
    if p == "cartesia":
        from livekit.plugins import cartesia
        return _construct(cartesia.TTS, model=ref.model or settings.default_tts_model, voice=ref.voice or None,
                          language=ref.language or opts.pop("language", settings.default_tts_language), speed=ref.speed,
                          api_key=_key(ref, settings), **opts)
    if p in ("elevenlabs", "eleven"):
        from livekit.plugins import elevenlabs
        return _construct(elevenlabs.TTS, voice_id=ref.voice or None, model=ref.model or "eleven_flash_v2_5",
                          language=ref.language, api_key=_key(ref, settings), **opts)
    if p == "sarvam":
        from livekit.plugins import sarvam
        return _construct(sarvam.TTS,
                          target_language_code=sarvam_language(ref.language or opts.pop("target_language_code", None),
                                                               default=settings.sarvam_tts_language),
                          speaker=ref.voice or settings.sarvam_tts_speaker or None,
                          model=ref.model or settings.sarvam_tts_model, pace=ref.speed,
                          api_key=_key(ref, settings), **opts)
    if p == "google":
        from livekit.plugins import google
        return _construct(google.TTS, voice_name=ref.voice or None, language=ref.language or language,
                          speaking_rate=ref.speed, credentials_info=opts.pop("credentials_info", None), **opts)
    if p == "openai":
        from livekit.plugins import openai
        return _construct(openai.TTS, model=ref.model or "gpt-4o-mini-tts", voice=ref.voice or "alloy",
                          api_key=_key(ref, settings), **opts)
    if p == "deepgram":
        from livekit.plugins import deepgram
        return _construct(deepgram.TTS, model=ref.model or "aura-2-thalia-en", api_key=_key(ref, settings), **opts)
    raise ValueError(f"unsupported TTS provider '{ref.provider}'")


# ----------------------------------------------------------------------------- realtime (S2S)
def build_realtime(ref: VoiceRef, settings: Settings, language: str, instructions: Optional[str] = None):
    p = ref.provider.lower()
    opts = dict(ref.options)
    if p in ("google", "gemini"):
        from google.genai import types as gtypes
        from livekit.plugins import google
        model = ref.model or settings.default_realtime_model
        # gemini-3.1-flash-live accepts a BCP-47 language (EO runs it with en-IN); the 2.5 native-audio
        # models reject one with "Unsupported language code" (API error 1007), so only pass it for 3.1.
        lang = (ref.language or language) if "3.1" in model else None
        return _construct(google.realtime.RealtimeModel, model=model,
                          voice=ref.voice or settings.default_realtime_voice, language=lang,
                          api_key=_key(ref, settings), instructions=instructions, temperature=opts.pop("temperature", None),
                          input_audio_transcription=gtypes.AudioTranscriptionConfig(),
                          output_audio_transcription=gtypes.AudioTranscriptionConfig(),
                          **opts)
    if p == "openai":
        from livekit.plugins import openai
        return _construct(openai.realtime.RealtimeModel, model=ref.model or "gpt-realtime", voice=ref.voice or "alloy",
                          api_key=_key(ref, settings), **opts)
    raise ValueError(f"unsupported realtime provider '{ref.provider}'")


# ----------------------------------------------------------------------------- turn handling
def stt_provides_turns(stt_obj: Any) -> bool:
    """True for STTs with their own end-of-turn model (Deepgram Flux): they drive turn detection."""
    try:
        from livekit.plugins import deepgram
        return isinstance(stt_obj, deepgram.STTv2)
    except Exception:
        return False


def build_turn_handling(turn: TurnConfig, mode: Mode, turn_detector: Any = None,
                        stt_turns: bool = False) -> dict[str, Any]:
    th: dict[str, Any] = {
        "endpointing": {"min_delay": turn.min_endpointing_delay, "max_delay": turn.max_endpointing_delay},
        "interruption": {
            "enabled": turn.allow_interruptions,
            "min_duration": turn.min_interruption_duration,
            "min_words": turn.min_interruption_words,
            "false_interruption_timeout": turn.false_interruption_timeout,
            "resume_false_interruption": turn.resume_false_interruption,
        },
        "preemptive_generation": {"enabled": turn.preemptive_generation, "preemptive_tts": turn.preemptive_tts},
    }
    if turn.interruption_mode == "vad":
        th["interruption"]["mode"] = "vad"          # "adaptive" = let the session auto-select the ML detector
    if mode == Mode.s2s:
        th["turn_detection"] = "realtime_llm"
        return th
    if stt_turns:
        th["turn_detection"] = "stt"                # Flux end-of-turn replaces VAD wait + semantic detector
        return th
    det = (turn.detector or "multilingual").lower()
    if det in ("vad", "stt", "manual"):
        th["turn_detection"] = det
    elif det == "realtime":
        th["turn_detection"] = "realtime_llm"
    else:
        detector = turn_detector
        if detector is None:
            try:
                detector = _load_turn_detector(det)
            except Exception as e:  # model files missing (run `gvox-worker download-files`)
                logger.warning("turn detector '%s' unavailable (%s); falling back to VAD endpointing", det, e)
        th["turn_detection"] = detector if detector is not None else "vad"
    return th


def _load_turn_detector(kind: str):
    if kind == "english":
        from livekit.plugins.turn_detector.english import EnglishModel
        return EnglishModel()
    from livekit.plugins.turn_detector.multilingual import MultilingualModel
    return MultilingualModel()


def prewarm_models(settings: Settings) -> dict[str, Any]:
    """Load the CPU models once per process (called from the worker's setup hook)."""
    from livekit.plugins import silero

    out: dict[str, Any] = {"vad": silero.VAD.load(min_silence_duration=0.4, prefix_padding_duration=0.3)}
    try:
        from livekit.plugins.turn_detector.multilingual import MultilingualModel
        out["turn_detector"] = MultilingualModel()
    except Exception as e:
        logger.warning("turn detector not available (%s); cascaded agents will use VAD endpointing until "
                       "`gvox-worker download-files` has run", e)
    return out


def register_plugins() -> None:
    """Import the plugins we ship so `download-files` sees them and their models get fetched."""
    for mod in ("livekit.plugins.silero", "livekit.plugins.turn_detector", "livekit.plugins.google",
                "livekit.plugins.deepgram", "livekit.plugins.cartesia", "livekit.plugins.elevenlabs",
                "livekit.plugins.sarvam", "livekit.plugins.anthropic", "livekit.plugins.openai"):
        try:
            __import__(mod)
        except Exception as e:  # optional providers
            logger.debug("plugin %s not importable: %s", mod, e)


# ----------------------------------------------------------------------------- session
def make_session(cfg: AgentConfig, settings: Settings, *, prewarmed: Optional[dict[str, Any]] = None,
                 tools: Optional[list[Any]] = None, mcp_servers: Optional[list[Any]] = None,
                 userdata: Any = None, instructions_for_realtime: Optional[str] = None,
                 input_sample_rate: int = 16000) -> AgentSession:
    """``input_sample_rate``: what the transport feeds in (8000 on the carrier path, 16000 for the browser).
    Passing it to the STT stops LiveKit's base class from upsampling phone audio before Deepgram."""
    prewarmed = prewarmed or {}
    language = cfg.language or settings.default_language
    common: dict[str, Any] = {
        "user_away_timeout": cfg.turn.user_away_timeout,
        "max_tool_steps": 4,
    }
    if tools:
        common["tools"] = tools
    if mcp_servers:
        common["mcp_servers"] = mcp_servers
    if userdata is not None:
        common["userdata"] = userdata

    if cfg.mode == Mode.s2s:
        assert cfg.realtime is not None
        return AgentSession(llm=build_realtime(cfg.realtime, settings, language, instructions_for_realtime),
                            vad=prewarmed.get("vad"),
                            turn_handling=build_turn_handling(cfg.turn, cfg.mode, prewarmed.get("turn_detector")),
                            **common)

    assert cfg.llm is not None
    stt_ref = cfg.stt or ProviderRef(provider=settings.default_stt_provider, model=settings.default_stt_model)
    if is_flux(stt_ref, settings) and BREAKER.is_open(FLUX_KEY):
        # a recent Flux failure: start this call on nova-3 (+ fallback adapter) instead of trying Flux again
        logger.warning("Flux breaker open: starting on nova-3 with fallback")
        stt_ref = ProviderRef(provider="deepgram", model="nova-3", api_key=stt_ref.api_key, fallback=stt_ref.fallback,
                              options={k: v for k, v in stt_ref.options.items() if k == "language"})
    tts_ref = cfg.tts or VoiceRef(provider=settings.default_tts_provider, model=settings.default_tts_model,
                                  voice=settings.default_tts_voice or None, language=settings.default_tts_language)
    stt_obj = build_stt(stt_ref, settings, language, vad=prewarmed.get("vad"), sample_rate=input_sample_rate)
    return AgentSession(
        stt=stt_obj,
        llm=build_llm(cfg.llm, settings),
        tts=build_tts(tts_ref, settings, language),
        vad=prewarmed.get("vad"),
        turn_handling=build_turn_handling(cfg.turn, cfg.mode, prewarmed.get("turn_detector"),
                                          stt_turns=stt_provides_turns(stt_obj)),
        **common,
    )
