"""agent-transport 0.2.2 hands LiveKit a stub job/context; LiveKit Agents 1.8 reads attributes they lack."""

from types import SimpleNamespace

from engine.worker.main import patch_stub_job


def test_patch_stub_job_adds_attributes_livekit_reads():
    ctx = SimpleNamespace(job=SimpleNamespace(id="stub", enable_recording=True))
    patch_stub_job(ctx)
    assert ctx.job.enable_redaction is False and ctx.job.id == "stub" and ctx.job.agent_name == ""
    assert ctx.job.enable_recording is False            # LiveKit's recorder stays off: the engine records itself
    assert ctx._redaction_enabled is False and ctx._primary_agent_session is None
    assert ctx._telemetry_state is None and ctx.inference_headers == {}
    assert ctx.is_fake_job() is False


def test_patch_stub_job_keeps_existing_values_and_tolerates_missing_job():
    ctx = SimpleNamespace(job=SimpleNamespace(enable_redaction=True), _redaction_enabled=True)
    patch_stub_job(ctx)
    assert ctx.job.enable_redaction is True and ctx._redaction_enabled is True
    patch_stub_job(SimpleNamespace())          # no job attribute at all: no exception
