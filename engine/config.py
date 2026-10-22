"""Engine settings (environment / .env).

One Settings object is shared by the API and the worker. Nothing here is tenant-specific:
per-agent provider keys (BYOK) arrive inside the agent config and override these defaults.
"""

from __future__ import annotations

from functools import lru_cache

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    # --- public reachability -------------------------------------------------------------
    # Carriers fetch answer XML and post ring/hangup callbacks to the API on this base URL.
    public_http_url: str = "http://localhost:8080"
    # Carriers open the bidirectional media WebSocket on the worker at this base URL (wss:// in prod).
    public_ws_url: str = "ws://localhost:8090"

    api_host: str = "0.0.0.0"
    api_port: int = 8080
    worker_host: str = "0.0.0.0"
    worker_port: int = 8090                 # carrier media WebSocket (agent-transport endpoint)
    worker_http_port: int = 8091            # worker control/health/browser-transport HTTP
    # Where the worker reaches the API (internal network) and where the API reaches this worker.
    internal_api_url: str = "http://localhost:8080"
    worker_internal_url: str = "http://localhost:8091"
    worker_agent_name: str = "gvox-caller"
    # Browser transport endpoint as seen from the test UI (worker HTTP port, path /browser).
    browser_ws_url: str = "ws://localhost:8091/browser"
    # False: carriers connect to PUBLIC_WS_URL as-is (agent-transport endpoint). True: append /stream/{call_id}.
    stream_path_per_call: bool = False

    # --- auth ----------------------------------------------------------------------------
    engine_api_key: str = "dev-engine-key"          # Bearer for /v1/* (platform backend -> engine)
    internal_token: str = "dev-internal-token"      # worker <-> api
    webhook_secret: str = "dev-webhook-secret"      # default HMAC secret for outgoing webhooks
    browser_ws_keys: str = ""                       # comma-separated auth keys for the browser transport

    # --- storage -------------------------------------------------------------------------
    database_url: str = "sqlite+aiosqlite:///./data/engine.db"
    recordings_dir: str = "./data/recordings"
    record_calls: bool = True

    # --- limits --------------------------------------------------------------------------
    max_concurrent_calls: int = 20
    call_max_seconds: int = 600
    answer_timeout_seconds: int = 45                # dialing -> no_answer if never answered

    # --- carriers ------------------------------------------------------------------------
    default_provider: str = "plivo"
    plivo_auth_id: str = ""
    plivo_auth_token: str = ""
    plivo_from_number: str = ""
    vobiz_auth_id: str = ""
    vobiz_auth_token: str = ""
    vobiz_from_number: str = ""
    vobiz_api_base: str = "https://api.vobiz.ai/api/v1"
    enablex_app_id: str = ""
    enablex_app_key: str = ""
    enablex_from_number: str = ""
    enablex_api_base: str = "https://api.enablex.io/voice/v1"

    # --- model providers (engine defaults; agent config may carry its own keys) ------------
    deepgram_api_key: str = ""
    google_api_key: str = ""
    anthropic_api_key: str = ""
    openai_api_key: str = ""
    groq_api_key: str = ""
    cartesia_api_key: str = ""
    elevenlabs_api_key: str = ""
    sarvam_api_key: str = ""

    # --- engine defaults ------------------------------------------------------------------
    default_language: str = "en-IN"
    default_stt_provider: str = "deepgram"
    default_stt_model: str = "nova-3"
    deepgram_language: str = "multi"              # nova-3 code-switching (Hindi/English)
    default_llm_provider: str = "google"
    default_llm_model: str = "gemini-2.5-flash"
    default_llm_thinking_budget: int = 0          # Gemini 2.5: 0 = no hidden reasoning before the first token
    stt_language_hints: str = "en,hi"             # Deepgram Flux `flux-general-multi` language hints
    default_tts_provider: str = "cartesia"
    default_tts_model: str = "sonic-3"
    default_tts_voice: str = ""
    default_tts_language: str = "en"
    tts_fallback_provider: str = "cartesia"   # second voice vendor used automatically when the primary fails; "" = off
    sarvam_tts_model: str = "bulbul:v3"
    sarvam_tts_speaker: str = "rahul"
    sarvam_tts_language: str = "en-IN"
    default_realtime_model: str = "gemini-2.5-flash-native-audio-preview-12-2025"
    default_realtime_voice: str = "Aoede"

    # --- workers ---------------------------------------------------------------------------
    worker_id: str = ""                             # default: hostname
    worker_max_sessions: int = 20                   # concurrent calls one worker process accepts
    worker_heartbeat_seconds: float = 5.0
    worker_dead_after_seconds: float = 20.0          # API marks a silent worker dead after this
    drain_timeout_seconds: int = 900                # draining worker hangs up what is left after this
    drain_accept_grace_seconds: float = 12.0        # keep answering streams this long after drain starts (proxy health interval x2)

    # --- carrier limits ---------------------------------------------------------------------
    carrier_max_concurrent: str = "plivo=50,vobiz=50,enablex=50,fake=1000"   # per-provider concurrent calls
    carrier_cps: str = "plivo=2,vobiz=2,enablex=2,fake=100"                 # per-provider dials per second

    # --- failover ---------------------------------------------------------------------------
    stt_fallback_provider: str = "sarvam"           # used when keyed and different from the primary; "" = off
    llm_fallback_provider: str = "groq"

    # --- recordings -------------------------------------------------------------------------
    recording_storage: str = "local"                # local | s3
    recording_s3_bucket: str = ""
    recording_s3_endpoint: str = ""
    recording_s3_region: str = "ap-south-1"
    recording_s3_access_key: str = ""
    recording_s3_secret_key: str = ""
    recording_retention_days: int = 90
    recording_url_mode: str = "stream"              # stream | signed
    spool_dir: str = "./data/spool"

    # --- security ---------------------------------------------------------------------------
    engine_encryption_key: str = ""                 # Fernet key; empty = plaintext at rest (dev only)
    carrier_verify_signatures: bool = True
    carrier_verify_log_only: bool = False           # verify but only log failures (first day on a new carrier account)
    vobiz_callback_ips: str = ""                    # comma-separated IPs/CIDRs allowed to call /carrier/vobiz/*

    # --- test -------------------------------------------------------------------------------
    enable_test_endpoints: bool = False             # fake carrier + /v1/test/* endpoints
    worker_media_url: str = "ws://127.0.0.1:8090"   # where the fake carrier opens media streams (never the proxy)

    log_level: str = "INFO"

    @staticmethod
    def _parse_limits(spec: str) -> dict[str, int]:
        out: dict[str, int] = {}
        for part in spec.split(","):
            if "=" in part:
                k, v = part.split("=", 1)
                try:
                    out[k.strip().lower()] = int(v.strip())
                except ValueError:
                    continue
        return out

    def carrier_limit(self, provider: str) -> int:
        return self._parse_limits(self.carrier_max_concurrent).get(provider.lower(), 50)

    def carrier_cps_limit(self, provider: str) -> int:
        return self._parse_limits(self.carrier_cps).get(provider.lower(), 2)

    def provider_key(self, provider: str) -> str:
        """Engine-level API key for a model provider ('' when unset)."""
        return {
            "deepgram": self.deepgram_api_key,
            "google": self.google_api_key,
            "gemini": self.google_api_key,
            "anthropic": self.anthropic_api_key,
            "openai": self.openai_api_key,
            "groq": self.groq_api_key,
            "cartesia": self.cartesia_api_key,
            "elevenlabs": self.elevenlabs_api_key,
            "sarvam": self.sarvam_api_key,
        }.get(provider.lower(), "")

    @property
    def browser_keys(self) -> set[str]:
        return {k.strip() for k in self.browser_ws_keys.split(",") if k.strip()}


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()


__all__ = ["Settings", "get_settings", "Field"]
