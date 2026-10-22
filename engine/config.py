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

    log_level: str = "INFO"

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
