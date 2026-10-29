"""Secrets pasted into a Kubernetes Secret with `echo | base64` carry a trailing newline; the engine must not
reject every API call because of it."""

import logging

from engine.config import Settings


def test_settings_strip_surrounding_whitespace_from_env_values(caplog):
    with caplog.at_level(logging.WARNING, logger="engine.config"):
        s = Settings(_env_file=None, engine_api_key="yAz1-key\n", internal_token=" tok\r\n", public_http_url="https://x\n",
                     worker_max_sessions="10\n", carrier_media_ips=" 1.2.3.4/32 5.6.7.8/32 ")
    assert s.engine_api_key == "yAz1-key" and s.internal_token == "tok" and s.public_http_url == "https://x"
    assert s.worker_max_sessions == 10
    assert s.carrier_max_concurrent == "plivo=5, vobiz=5"           # inner characters are the format; only the edges go
    msg = " ".join(r.getMessage() for r in caplog.records)
    assert "engine_api_key" in msg and "internal_token" in msg and "whitespace" in msg
    assert "yAz1-key" not in msg                                     # never log the secret itself


def test_clean_settings_log_nothing(caplog):
    with caplog.at_level(logging.WARNING, logger="engine.config"):
        Settings(_env_file=None, engine_api_key="clean-key")
    assert not [r for r in caplog.records if "whitespace" in r.getMessage()]
