/* gvox-voice test console: talks to the engine API (same origin) and the worker's /browser WebSocket. */
(() => {
  const $ = (id) => document.getElementById(id);
  const state = { config: null, example: null, apiBase: "", apiKey: "", talk: null, phoneCallId: null, phonePoll: null };

  // ------------------------------------------------------------------ settings / helpers
  function loadSettings() {
    state.apiBase = localStorage.getItem("gvox.apiBase") || window.location.origin;
    state.apiKey = localStorage.getItem("gvox.apiKey") || "dev-engine-key";
    $("api-base").value = state.apiBase;
    $("api-key").value = state.apiKey;
  }
  $("save-settings").onclick = () => {
    state.apiBase = $("api-base").value.trim() || window.location.origin;
    state.apiKey = $("api-key").value.trim();
    localStorage.setItem("gvox.apiBase", state.apiBase);
    localStorage.setItem("gvox.apiKey", state.apiKey);
    checkHealth();
  };
  async function api(path, opts = {}) {
    const headers = Object.assign({ Authorization: `Bearer ${state.apiKey}` }, opts.headers || {});
    if (opts.json !== undefined) { headers["Content-Type"] = "application/json"; opts.body = JSON.stringify(opts.json); }
    const r = await fetch(state.apiBase + path, Object.assign({}, opts, { headers }));
    const text = await r.text();
    let data = null; try { data = text ? JSON.parse(text) : null; } catch { data = text; }
    if (!r.ok) throw new Error(`${r.status} ${typeof data === "string" ? data : JSON.stringify(data && data.detail || data)}`);
    return data;
  }
  const esc = (s) => String(s ?? "").replace(/[&<>"]/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;" }[c]));
  const fmt = (n, d = 2) => (n === null || n === undefined ? "—" : Number(n).toFixed(d));

  async function checkHealth() {
    try {
      const h = await fetch(state.apiBase + "/health").then((r) => r.json());
      $("health-dot").classList.add("ok");
      $("health-text").textContent = `engine v${h.version} · active calls ${h.active_calls}`;
    } catch (e) {
      $("health-dot").classList.remove("ok");
      $("health-text").textContent = "engine API unreachable";
    }
  }

  async function loadConfig() {
    try {
      state.config = await api("/ui/config.json");          // needs the engine API key (it returns the browser key)
      state.example = await api("/ui/example-agent");
      const badges = Object.entries(state.config.providers).map(([k, v]) => `<span class="badge ${v ? "on" : ""}">${k}${v ? " ✓" : ""}</span>`).join("");
      $("provider-badges").innerHTML = badges + (state.config.carriers.length ? state.config.carriers.map((c) => `<span class="badge on">carrier: ${c}</span>`).join("") : "");
      $("talk-mode").value = state.example.mode || "s2s";
      const ttsSel = $("talk-tts");
      ttsSel.innerHTML = Object.entries(state.config.tts_options || {}).map(([k, v]) =>
        `<option value="${k}">${k} · ${v.model}${v.voice ? " · " + v.voice : ""}${v.language ? " (" + v.language + ")" : ""}</option>`).join("")
        || `<option value="">no TTS key configured</option>`;
      if (state.config.default_tts_provider) ttsSel.value = state.config.default_tts_provider;
      const fill = (sel, opts, label, def) => {
        sel.innerHTML = Object.entries(opts || {}).map(([k, v]) => `<option value="${k}">${label(v)}</option>`).join("")
          || `<option value="">none configured</option>`;
        if (def && opts && opts[def]) sel.value = def;
      };
      fill($("talk-stt"), state.config.stt_options, (v) => `${v.provider} ${v.model}${v.options && v.options.language ? " (" + v.options.language + ")" : ""}`, state.config.default_stt);
      fill($("talk-llm"), state.config.llm_options, (v) => `${v.provider} ${v.model}${v.options && v.options.thinking_budget === 0 ? " · no thinking" : ""}`, state.config.default_llm);
      const summarize = () => {
        const mode = $("talk-mode").value;
        const t = (state.config.tts_options || {})[ttsSel.value];
        const s = (state.config.stt_options || {})[$("talk-stt").value] || state.config.stt_default;
        const l = (state.config.llm_options || {})[$("talk-llm").value] || state.config.llm_default;
        $("pipeline-summary").textContent = mode === "s2s"
          ? `pipeline: ${state.config.default_realtime_model} (${state.config.default_realtime_voice})`
          : `pipeline: ${s.provider} ${s.model} → ${l.provider} ${l.model} → ${t ? t.provider + " " + t.model + (t.voice ? " " + t.voice : "") : "no TTS"}`;
      };
      $("talk-mode").onchange = summarize; ttsSel.onchange = summarize; $("talk-stt").onchange = summarize; $("talk-llm").onchange = summarize; summarize();
      if (!$("agent-json").value) $("agent-json").value = JSON.stringify(state.example, null, 2);
      const pub = state.config.public_http_url || "";
      if (/localhost|127\.0\.0\.1/.test(pub)) {
        $("phone-warning").textContent = `PUBLIC_HTTP_URL is ${pub}: the carrier cannot reach this machine, so phone calls will ring and then drop. Put the API and the worker behind public HTTPS/WSS URLs (or a tunnel) before dialing.`;
      }
    } catch (e) {
      $("talk-status").textContent = "Could not load /ui/config.json: " + e.message;
    }
  }

  // ------------------------------------------------------------------ tabs
  document.querySelectorAll("nav button").forEach((b) => {
    b.onclick = () => {
      document.querySelectorAll("nav button").forEach((x) => x.classList.remove("active"));
      document.querySelectorAll("section.tab").forEach((x) => x.classList.remove("active"));
      b.classList.add("active");
      $("tab-" + b.dataset.tab).classList.add("active");
      if (b.dataset.tab === "calls") refreshCalls();
      if (b.dataset.tab === "ops") startOps(); else stopOps();
    };
  });

  // ------------------------------------------------------------------ agent config assembly
  function variables() {
    const v = {};
    ["first_name", "event_name", "event_date", "event_time", "event_venue"].forEach((k) => (v[k] = $("var-" + k).value));
    return v;
  }
  function currentAgent(sourceSel) {
    const source = $(sourceSel).value;
    let agent;
    if (source === "editor") {
      agent = JSON.parse($("agent-json").value);
    } else if (source === "prompt") {
      agent = { name: "ui-prompt", mode: "s2s", language: state.config.default_language,
        instructions: $("talk-prompt").value, greeting: $("talk-greeting").value || null,
        llm: { provider: "google", model: state.config.default_llm_model },
        realtime: { provider: "google", model: state.config.default_realtime_model, voice: state.config.default_realtime_voice, language: state.config.default_language },
        tools: [{ id: "end_call", type: "builtin", builtin: "end_call", name: "end_call", description: "End the call when the user says goodbye." }],
        max_duration_s: 300, record: true };
    } else {
      agent = JSON.parse(JSON.stringify(state.example));
    }
    if (source !== "editor") {
      agent.mode = $("talk-mode").value;
      agent.workflow_mode = $("talk-flat").checked ? "flat" : "nodes";
      if (agent.mode === "cascaded") {
        const s = (state.config.stt_options || {})[$("talk-stt").value];
        const l = (state.config.llm_options || {})[$("talk-llm").value];
        agent.stt = s || agent.stt || state.config.stt_default;
        agent.llm = l || agent.llm || state.config.llm_default;
        const t = (state.config.tts_options || {})[$("talk-tts").value];
        if (t) agent.tts = t;
      }
    }
    return agent;
  }

  // ------------------------------------------------------------------ audio (browser mic <-> worker)
  class Audio {
    constructor() { this.ctx = null; this.stream = null; this.node = null; this.next = 0; this.sources = []; this.muted = false; this.inA = null; this.outA = null; }
    async start(onPcm) {
      this.ctx = new (window.AudioContext || window.webkitAudioContext)();
      await this.ctx.audioWorklet.addModule("pcm-processor.js");
      this.outA = this.ctx.createAnalyser(); this.outA.fftSize = 256; this.outA.connect(this.ctx.destination);
      this.stream = await navigator.mediaDevices.getUserMedia({ audio: { echoCancellation: true, noiseSuppression: true, autoGainControl: true } });
      const src = this.ctx.createMediaStreamSource(this.stream);
      this.inA = this.ctx.createAnalyser(); this.inA.fftSize = 256; src.connect(this.inA);
      this.node = new AudioWorkletNode(this.ctx, "pcm-processor");
      this.node.port.onmessage = (e) => onPcm(this.toInt16(this.downsample(e.data, this.ctx.sampleRate, 16000)));
      this.inA.connect(this.node);
      const mute = this.ctx.createGain(); mute.gain.value = 0; this.node.connect(mute); mute.connect(this.ctx.destination);
      if (this.ctx.state === "suspended") await this.ctx.resume();
    }
    play(int16, rate) {
      if (!this.ctx || this.muted) return;
      const f = new Float32Array(int16.length);
      for (let i = 0; i < int16.length; i++) f[i] = int16[i] / 32768;
      const buf = this.ctx.createBuffer(1, f.length, rate); buf.getChannelData(0).set(f);
      const s = this.ctx.createBufferSource(); s.buffer = buf; s.connect(this.outA);
      const now = this.ctx.currentTime; this.next = Math.max(now, this.next); s.start(this.next); this.next += buf.duration;
      this.sources.push(s); s.onended = () => { const i = this.sources.indexOf(s); if (i > -1) this.sources.splice(i, 1); };
    }
    interrupt() {
      this.muted = true; this.sources.forEach((s) => { try { s.stop(); s.disconnect(); } catch {} }); this.sources = [];
      if (this.ctx) this.next = this.ctx.currentTime; setTimeout(() => (this.muted = false), 250);
    }
    level(an) { if (!an) return 0; const d = new Uint8Array(an.fftSize); an.getByteTimeDomainData(d); let m = 0; for (const v of d) m = Math.max(m, Math.abs(v - 128)); return Math.min(1, m / 60); }
    playing() { return !!this.ctx && this.next > this.ctx.currentTime + 0.02; }
    stop() {
      if (this.stream) this.stream.getTracks().forEach((t) => t.stop());
      this.sources.forEach((s) => { try { s.stop(); } catch {} });
      if (this.ctx && this.ctx.state !== "closed") this.ctx.close();
      this.ctx = null; this.stream = null; this.node = null; this.sources = [];
    }
    downsample(buf, from, to) {
      if (from === to) return buf;
      const ratio = from / to, n = Math.round(buf.length / ratio), out = new Float32Array(n);
      let o = 0, i = 0;
      while (o < n) { const nextI = Math.round((o + 1) * ratio); let acc = 0, c = 0; for (; i < nextI && i < buf.length; i++) { acc += buf[i]; c++; } out[o++] = c ? acc / c : 0; }
      return out;
    }
    toInt16(f) { const b = new Int16Array(f.length); for (let i = 0; i < f.length; i++) b[i] = Math.max(-1, Math.min(1, f[i])) * 0x7fff; return b; }
  }
  const b64 = (i16) => { let s = ""; const u8 = new Uint8Array(i16.buffer); for (let i = 0; i < u8.length; i += 0x8000) s += String.fromCharCode.apply(null, u8.subarray(i, i + 0x8000)); return btoa(s); };
  const unb64 = (s) => { const bin = atob(s); const u8 = new Uint8Array(bin.length); for (let i = 0; i < bin.length; i++) u8[i] = bin.charCodeAt(i); return new Int16Array(u8.buffer); };

  function bubble(role, text) {
    const t = $("talk-transcript"); const d = document.createElement("div");
    d.className = "bubble " + role; d.textContent = text;
    if (role === "agent" && state.talk && state.talk.pendingTotal != null) {
      const b = document.createElement("span"); b.className = "badge-lat " + latClass(state.talk.pendingTotal);
      b.textContent = `⏱ ${state.talk.pendingTotal.toFixed(2)} s`; d.appendChild(b); state.talk.pendingTotal = null;
    }
    t.appendChild(d); t.scrollTop = t.scrollHeight;
    return d;
  }
  const latClass = (s) => (s == null ? "" : s <= 0.9 ? "lat-good" : s <= 1.3 ? "lat-ok" : "lat-bad");
  function renderLatency(talk) {
    const m = talk.metrics;
    if (m) {
      const turns = (m.turns || []).filter((t) => t.total != null);
      const last = turns[turns.length - 1];
      const big = $("lat-total");
      big.textContent = last ? `${last.total.toFixed(2)} s` : "—";
      big.className = "lat-big " + latClass(last ? last.total : null);
      const forced = last && last.eou_delay != null && last.eou_delay >= 1.7;
      $("lat-stages").textContent = last
        ? `turn ${last.turn}: endpointing ${fmt(last.eou_delay)}${forced ? " (max wait — your sentence sounded unfinished)" : ""} · stt ${fmt(last.stt_delay)} · llm first token ${fmt(last.llm_ttft)} · tts first byte ${fmt(last.tts_ttfb)} (s)`
        : "measuring…";
      $("lat-session").textContent = turns.length ? `${turns.length} turn${turns.length > 1 ? "s" : ""} · p50 ${fmt(m.total_p50)} s · p95 ${fmt(m.total_p95)} s` : "—";
      $("lat-first").textContent = m.pickup_to_first_word != null ? `${fmt(m.pickup_to_first_word)} s after connect` : "—";
    }
    $("lat-client").textContent = talk.clientTotal != null ? `${talk.clientTotal.toFixed(2)} s (mic silence → audio in browser)` : "—";
  }
  function resetLatency() {
    $("lat-total").textContent = "—"; $("lat-total").className = "lat-big";
    $("lat-stages").textContent = "waiting for the first reply…";
    $("lat-session").textContent = "—"; $("lat-first").textContent = "—"; $("lat-client").textContent = "—";
  }

  // ------------------------------------------------------------------ talk session
  async function startTalk() {
    let agent;
    try { agent = currentAgent("talk-agent-source"); } catch (e) { $("talk-status").textContent = "Agent JSON invalid: " + e.message; return; }
    $("talk-start").disabled = true; $("talk-transcript").innerHTML = ""; $("talk-callid").textContent = "";
    $("talk-status").className = "status"; $("talk-status").textContent = "Requesting microphone…";
    resetLatency();
    const audio = new Audio();
    const talk = { ws: null, audio, callId: null, meter: null, metrics: null, pendingTotal: null,
      lastVoiceAt: null, awaitingReply: false, clientTotal: null, voicedRun: 0, playbackEndedAt: 0 };
    state.talk = talk;
    // Microphone FIRST: the agent greets as soon as the session starts, so the audio context must exist
    // before we connect (otherwise the greeting plays into nothing while the permission dialog is open).
    let sendPcm = null;
    try {
      await audio.start((pcm) => { if (sendPcm) sendPcm(pcm); });
    } catch (e) { $("talk-status").className = "status bad"; $("talk-status").textContent = "Microphone error: " + e.message; $("talk-start").disabled = false; state.talk = null; return; }
    $("talk-status").textContent = "Microphone ready, connecting…";
    const ws = new WebSocket(state.config.browser_ws_url);
    talk.ws = ws;
    ws.onopen = () => {
      ws.send(JSON.stringify({ type: "setup", auth_key: state.config.browser_key, agent, variables: variables(), sample_rate: 16000 }));
      sendPcm = (pcm) => { if (ws.readyState === 1) ws.send(JSON.stringify({ type: "audio", data: b64(pcm) })); };
      talk.meter = setInterval(() => {
        const lin = audio.level(audio.inA);
        $("meter-in").style.width = lin * 100 + "%"; $("meter-out").style.width = audio.level(audio.outA) * 100 + "%";
        // client-side stopwatch: sustained mic energy (>= 3 samples ≈ 240 ms) while nothing is playing and at least
        // 500 ms after playback ended, so speaker echo cannot restart the clock
        const playing = audio.playing();
        if (playing) talk.playbackEndedAt = performance.now();
        const quiet = !playing && performance.now() - talk.playbackEndedAt > 500;
        talk.voicedRun = (lin > 0.08 && quiet) ? talk.voicedRun + 1 : 0;
        if (talk.voicedRun >= 3) { talk.lastVoiceAt = performance.now(); talk.awaitingReply = true; }
      }, 80);
      $("talk-status").textContent = "Connected. The agent should greet you; speak naturally, interrupt freely.";
      $("talk-stop").disabled = false;
    };
    ws.onmessage = (ev) => {
      const m = JSON.parse(ev.data);
      if (m.type === "audio") {
        audio.play(unb64(m.data), 16000);
        if (talk.awaitingReply && talk.lastVoiceAt != null) {
          talk.clientTotal = (performance.now() - talk.lastVoiceAt) / 1000; talk.awaitingReply = false; renderLatency(talk);
        }
      }
      else if (m.type === "ready") { talk.callId = m.call_id; $("talk-callid").textContent = m.call_id; bubble("sys", "session " + m.call_id); }
      else if (m.type === "transcript") bubble(m.role, m.text);
      else if (m.type === "metrics") {
        talk.metrics = m;
        const turns = (m.turns || []).filter((t) => t.total != null);
        if (turns.length) talk.pendingTotal = turns[turns.length - 1].total;
        renderLatency(talk);
      }
      else if (m.type === "node") bubble("sys", `→ step: ${m.to}${m.via ? " (" + m.via + ")" : ""}`);
      else if (m.type === "turn_complete") { /* playback state is derived from the audio schedule */ }
      else if (m.type === "interrupted") { audio.interrupt(); bubble("sys", "— interrupted —"); }
      else if (m.type === "error") { $("talk-status").className = "status bad"; $("talk-status").textContent = m.code + ": " + m.message; }
      else if (m.type === "session_end") bubble("sys", "session ended (" + m.reason + ")");
    };
    ws.onerror = () => { $("talk-status").className = "status bad"; $("talk-status").textContent = "WebSocket error (is the worker running on " + state.config.browser_ws_url + "?)"; };
    ws.onclose = () => finishTalk();
  }
  async function finishTalk() {
    const talk = state.talk; if (!talk) return; state.talk = null;
    clearInterval(talk.meter); talk.audio.stop();
    $("talk-start").disabled = false; $("talk-stop").disabled = true;
    $("meter-in").style.width = "0"; $("meter-out").style.width = "0";
    $("talk-status").textContent = "Call ended. Fetching the engine's record…";
    if (talk.callId) {
      for (let i = 0; i < 6; i++) {
        await new Promise((r) => setTimeout(r, 800));
        try {
          const c = await api("/v1/calls/" + talk.callId);
          renderSummary(c);
          if (["completed", "failed", "cancelled"].includes(c.status)) break;
        } catch (e) { $("talk-status").textContent = "Record fetch failed: " + e.message; break; }
      }
      $("talk-status").textContent = "Idle.";
    }
  }
  function renderSummary(c) {
    const m = c.metrics || {}; const turns = (m.turns || []).length;
    $("talk-summary").innerHTML = [
      ["status", `<span class="pill ${c.status}">${c.status}</span> ${esc(c.end_reason || "")}`],
      ["duration", fmt(c.duration_s, 1) + " s"],
      ["turns", `${turns} measured · total p50 ${fmt(m.total_p50)} s · p95 ${fmt(m.total_p95)} s`],
      ["first word", fmt(m.pickup_to_first_word) + " s after connect"],
      ["outcome", `<code>${esc(JSON.stringify(c.outcome || {}))}</code>`],
      ["recording", c.recording_url ? `<a href="#" data-rec="${c.id}">play</a>` : "none (browser sessions are not recorded)"],
    ].map(([k, v]) => `<div>${k}</div><div>${v}</div>`).join("");
    bindRecordingLinks($("talk-summary"));
  }
  $("talk-start").onclick = startTalk;
  $("talk-stop").onclick = () => { if (state.talk && state.talk.ws) { try { state.talk.ws.send(JSON.stringify({ type: "stop" })); } catch {} state.talk.ws.close(); } };

  // ------------------------------------------------------------------ phone
  $("phone-dial").onclick = async () => {
    let agent; try { agent = currentAgent("phone-agent-source"); } catch (e) { $("phone-status").textContent = "Agent JSON invalid: " + e.message; return; }
    const body = { to: $("phone-to").value.trim(), agent, variables: variables(), provider: $("phone-provider").value || null,
      webhook_url: $("phone-webhook").value.trim() || null, metadata: { source: "ui" } };
    $("phone-status").textContent = "Dialing…";
    try {
      const r = await api("/v1/calls", { method: "POST", json: body });
      state.phoneCallId = r.call_id; $("phone-hangup").disabled = false; $("phone-say-btn").disabled = false;
      $("phone-status").textContent = `Call ${r.call_id} ${r.status} via ${r.provider}`;
      clearInterval(state.phonePoll);
      state.phonePoll = setInterval(pollPhone, 2000);
    } catch (e) { $("phone-status").textContent = "Dial failed: " + e.message; }
  };
  async function pollPhone() {
    if (!state.phoneCallId) return;
    try {
      const c = await api("/v1/calls/" + state.phoneCallId);
      const last = (c.transcript || []).slice(-3).map((t) => `${t.role}: ${t.text}`).join("<br>");
      $("phone-live").innerHTML = [["call", c.id], ["status", `<span class="pill ${c.status}">${c.status}</span> ${esc(c.end_reason || "")}`],
        ["duration", fmt(c.duration_s, 1) + " s"], ["turns", (c.transcript || []).length], ["latest", last || "—"],
        ["outcome", `<code>${esc(JSON.stringify(c.outcome || {}))}</code>`]].map(([k, v]) => `<div>${k}</div><div>${v}</div>`).join("");
      if (["completed", "failed", "cancelled", "busy", "no_answer"].includes(c.status)) {
        clearInterval(state.phonePoll); $("phone-hangup").disabled = true; $("phone-say-btn").disabled = true;
      }
    } catch (e) { $("phone-status").textContent = "Poll failed: " + e.message; }
  }
  $("phone-hangup").onclick = async () => { try { await api(`/v1/calls/${state.phoneCallId}/hangup`, { method: "POST" }); } catch (e) { $("phone-status").textContent = e.message; } };
  $("phone-say-btn").onclick = async () => { try { await api(`/v1/calls/${state.phoneCallId}/say`, { method: "POST", json: { text: $("phone-say").value } }); } catch (e) { $("phone-status").textContent = e.message; } };

  // ------------------------------------------------------------------ calls
  async function refreshCalls() {
    try {
      const rows = await api("/v1/calls?limit=50");
      $("calls-body").innerHTML = rows.map((c) => `<tr class="click" data-id="${c.id}"><td><code>${c.id.slice(5)}</code></td><td>${c.direction}</td>
        <td><span class="pill ${c.status}">${c.status}</span></td><td>${fmt(c.duration_s, 0)}</td><td>${inr((c.metrics || {}).cost)}</td><td><small>${esc(JSON.stringify(c.outcome || {})).slice(0, 60)}</small></td></tr>`).join("") || "<tr><td colspan=6><small>no calls yet</small></td></tr>";
      document.querySelectorAll("#calls-body tr.click").forEach((tr) => (tr.onclick = () => showCall(tr.dataset.id)));
    } catch (e) { $("calls-body").innerHTML = `<tr><td colspan=6>${esc(e.message)}</td></tr>`; }
  }
  function inr(cost) {
    if (!cost || cost.total_inr == null) return "—";
    return `₹${cost.total_inr.toFixed(2)}${cost.incomplete ? "<span class=\"warn\" title=\"a model has no price on file: lower bound\">*</span>" : ""}`;
  }
  const STAGE_NAMES = { stt: "STT · speech → text", llm: "LLM · reasoning", tts: "TTS · text → speech", realtime: "speech-to-speech", carrier: "telephony · carrier" };
  function costQuantity(l) {
    const d = l.detail || {};
    if (l.unit === "minute" && d.billed_pulses_30s != null) return `${l.quantity.toFixed(1)} min <small>(${fmt(d.seconds, 0)} s → ${d.billed_pulses_30s} × 30 s pulses)</small>`;
    if (l.unit === "minute") return `${l.quantity.toFixed(3)} min <small>(${fmt(d.seconds_streamed, 1)} s streamed, ${fmt(d.recognized_seconds, 1)} s of speech)</small>`;
    if (l.unit === "character") return `${d.characters != null ? d.characters : l.quantity} chars <small>(${d.requests || 0} req${d.cancelled_characters ? `, ${d.cancelled_characters} cancelled` : ""}${d.audio_seconds ? `, ${fmt(d.audio_seconds, 1)} s audio` : ""})</small>`;
    if (d.audio_input_tokens != null) return `<small>audio ${d.audio_input_tokens} in / ${d.audio_output_tokens} out · text ${d.text_input_tokens} in / ${d.text_output_tokens} out</small>`;
    return `<small>${d.uncached_input_tokens || 0} in${d.cached_input_tokens ? ` + ${d.cached_input_tokens} cached` : ""} + ${d.output_tokens || 0} out${d.thinking_tokens ? ` (${d.thinking_tokens} thinking)` : ""} · ${d.requests || 0} req</small>`;
  }
  function costUnitPrice(l) {
    const r = l.rates || {};
    if (l.unit === "minute" && l.unit_price_usd != null) return (l.detail || {}).inr_per_minute != null ? `₹${l.detail.inr_per_minute}/min` : `$${l.unit_price_usd.toFixed(4)}/min`;
    if (l.unit === "character" && l.unit_price_usd != null) return `$${(l.unit_price_usd * 1000).toFixed(3)}/1k chars`;
    if (r.usd_per_m_audio_input != null) return `$${r.usd_per_m_audio_input}/M audio in · $${r.usd_per_m_audio_output}/M audio out · $${r.usd_per_m_text_input}/M text in · $${r.usd_per_m_text_output}/M text out`;
    if (r.usd_per_m_input != null) return `$${r.usd_per_m_input}/M in · $${r.usd_per_m_cached_input != null ? r.usd_per_m_cached_input : r.usd_per_m_input}/M cached · $${r.usd_per_m_output}/M out`;
    return "no price on file";
  }
  function costSection(c, cost) {
    if (!cost || !cost.stages) return `<h2 style="margin-top:12px">Cost</h2><small>no usage recorded (call still running, or it ran on an older worker)</small>`;
    const rows = Object.entries(cost.stages).flatMap(([stage, s]) => s.lines.map((l) => `<tr class="${l.usd == null ? "cost-missing" : ""}"><td>${STAGE_NAMES[stage] || stage}</td><td><b>${esc(l.provider)}</b> <small>${esc(l.model)}</small></td>
        <td>${costQuantity(l)}</td><td><small>${costUnitPrice(l)}</small></td><td>${l.usd == null ? "?" : "$" + l.usd.toFixed(5)}</td><td>${l.inr == null ? "?" : "₹" + l.inr.toFixed(3)}</td><td><small title="${esc(l.source || "")}">${esc(l.note || "")}</small></td></tr>`)).join("");
    const mins = c.duration_s ? c.duration_s / 60 : 0;
    const perMin = mins ? ` · <b>₹${(cost.total_inr / mins).toFixed(2)} per minute</b>` : "";
    const carrierUsd = cost.total_usd - (cost.models_usd != null ? cost.models_usd : cost.total_usd);
    return `<h2 style="margin-top:12px">Cost · measured usage × list price</h2>
      <table><thead><tr><th>stage</th><th>vendor · model</th><th>billed quantity</th><th>unit price</th><th>USD</th><th>INR</th><th>billing note</th></tr></thead><tbody>${rows}</tbody></table>
      <div class="status">models <b>$${(cost.models_usd != null ? cost.models_usd : cost.total_usd).toFixed(5)}</b> (₹${((cost.models_usd != null ? cost.models_usd : cost.total_usd) * cost.usd_inr).toFixed(3)})${carrierUsd > 0 ? ` + carrier $${carrierUsd.toFixed(5)}` : ""} = <b>$${cost.total_usd.toFixed(5)} · ₹${cost.total_inr.toFixed(3)}</b>${perMin}
      ${cost.incomplete ? '<br><span class="warn">* a model has no price on file: this total is a lower bound (add it to pricing.json)</span>' : ""}
      ${(cost.notes || []).map((n) => `<br><span class="warn">${esc(n)}</span>`).join("")}
      <br><small>${esc(cost.disclaimer || "")} · USD/INR ${cost.usd_inr} · ${esc(cost.price_list || "pricing.json")}</small></div>`;
  }
  const providers = (t) => [t.stt_provider, t.llm_provider, t.tts_provider].map((p) => p || "·").join(" / ");
  async function showCall(id) {
    state.selectedCallId = id;
    $("ops-deliveries-call").textContent = id;
    try {
      const c = await api("/v1/calls/" + id);
      const m = c.metrics || {};
      // stage breakdown per turn: end-of-turn wait, LLM first token, TTS first byte, and whatever is left
      // (network hops, transition round-trips, audio pipeline). A node change means an extra LLM call.
      const stageBar = (t) => {
        const total = t.total || 0; if (!total) return "";
        const parts = [["eou", t.eou_delay || 0, "#0ea5e9"], ["llm", t.llm_ttft || 0, "#f59e0b"], ["tts", t.tts_ttfb || 0, "#10b981"]];
        const other = Math.max(0, total - parts.reduce((s, p) => s + p[1], 0));
        parts.push(["other", other, "#64748b"]);
        return `<div class="stagebar">${parts.map(([k, v, col]) => `<span title="${k} ${fmt(v)} s" style="width:${(100 * v / total).toFixed(1)}%;background:${col}"></span>`).join("")}</div>`;
      };
      const turns = (m.turns || []).map((t) => {
        const other = t.total != null ? Math.max(0, t.total - (t.eou_delay || 0) - (t.llm_ttft || 0) - (t.tts_ttfb || 0)) : null;
        return `<tr><td>${t.turn}</td><td><small>${esc(t.node || "")}${t.transition ? ' <span class="badge-lat" title="the workflow moved to a new node before this reply: one extra LLM round-trip">+handoff</span>' : ""}</small></td>
          <td>${fmt(t.eou_delay)}</td><td>${fmt(t.llm_ttft)}</td><td>${fmt(t.tts_ttfb)}</td><td>${fmt(other)}</td>
          <td class="${latClass(t.total)}"><b>${fmt(t.total)}</b>${stageBar(t)}</td><td><small>${esc(providers(t))}</small></td></tr>`;
      }).join("");
      const models = m.models ? ["stt", "llm", "tts"].map((k) => `${k}: <b>${esc(m.models[k] || "?")}</b>`).join(" · ") : "—";
      const provEvents = (m.provider_events || []).map((e) => `${e.kind}:${e.provider} ${e.available ? "up" : "DOWN"} @${fmt(e.at, 1)}s`).join(", ");
      $("call-detail").innerHTML = `
        <div class="kv"><div>id</div><div>${c.id}</div><div>status</div><div><span class="pill ${c.status}">${c.status}</span> ${esc(c.end_reason || "")} ${esc(c.hangup_cause || "")}</div>
        <div>to / from</div><div>${esc(c.to)} / ${esc(c.from || "")} (${esc(c.provider)})</div><div>agent</div><div>${esc(c.agent_id || "inline")} v${c.agent_version || "-"}</div>
        <div>duration</div><div>${fmt(c.duration_s, 1)} s · first word ${fmt(m.pickup_to_first_word)} s</div>
        <div>latency</div><div>total p50 ${fmt(m.total_p50)} s · p95 ${fmt(m.total_p95)} s · llm ttft p50 ${fmt(m.llm_ttft_p50)} · tts ttfb p50 ${fmt(m.tts_ttfb_p50)}</div>
        <div>outcome</div><div><code>${esc(JSON.stringify(c.outcome || {}))}</code></div>
        <div>recording</div><div>${c.recording_url ? `<a href="#" data-rec="${c.id}">play</a> <span id="rec-holder"></span>` : "none"}</div>
        <div>error</div><div>${esc(c.error || "—")}</div>
        <div>models</div><div>${models}</div>
        <div>failover</div><div>${esc(provEvents || "none")}</div>
        <div>cost</div><div>${m.cost && m.cost.total_inr != null ? `<b>₹${m.cost.total_inr.toFixed(3)}</b> ($${m.cost.total_usd.toFixed(5)})${m.cost.incomplete ? ' <span class="warn">*incomplete</span>' : ""}` : "—"}</div></div>
        ${costSection(c, m.cost)}
        <h2 style="margin-top:12px">Per-turn latency (s) · end of caller speech → agent audio</h2>
        <table><thead><tr><th>#</th><th>node</th><th title="caller stopped speaking → turn committed (VAD / turn detector / Flux)">end of turn</th><th title="LLM time to first token">llm 1st token</th><th title="TTS time to first byte">tts 1st byte</th><th title="total minus the three stages: network hops, handoff round-trips, audio pipeline">other</th><th>total</th><th>served by</th></tr></thead><tbody>${turns || "<tr><td colspan=8><small>none</small></td></tr>"}</tbody></table>
        <div class="status">bar: <span style="color:#0ea5e9">■</span> end of turn <span style="color:#f59e0b">■</span> llm <span style="color:#10b981">■</span> tts <span style="color:#64748b">■</span> other</div>
        <h2 style="margin-top:12px">Transcript</h2>
        <div class="transcript" style="height:260px">${(c.transcript || []).map((t) => `<div class="bubble ${t.role === "user" ? "user" : t.role === "agent" ? "agent" : "sys"}">${esc(t.text)}${t.interrupted ? " ⏸" : ""}</div>`).join("") || "<small>empty</small>"}</div>`;
      bindRecordingLinks($("call-detail"));
    } catch (e) { $("call-detail").innerHTML = `<small>${esc(e.message)}</small>`; }
  }
  function bindRecordingLinks(root) {
    root.querySelectorAll("a[data-rec]").forEach((a) => (a.onclick = async (ev) => {
      ev.preventDefault();
      try {
        const r = await fetch(state.apiBase + `/v1/calls/${a.dataset.rec}/recording`, { headers: { Authorization: `Bearer ${state.apiKey}` } });
        if (!r.ok) throw new Error(r.status);
        const url = URL.createObjectURL(await r.blob());
        const el = document.createElement("audio"); el.controls = true; el.src = url; a.replaceWith(el);
      } catch (e) { a.textContent = "recording unavailable (" + e.message + ")"; }
    }));
  }
  $("calls-refresh").onclick = refreshCalls;

  // ------------------------------------------------------------------ ops (workers, capacity, deliveries, smoke)
  let opsTimer = null;
  function startOps() { refreshOps(); if (!opsTimer) opsTimer = setInterval(refreshOps, 5000); }
  function stopOps() { if (opsTimer) { clearInterval(opsTimer); opsTimer = null; } }
  $("ops-internal-token").value = localStorage.getItem("gvox.internalToken") || "";
  $("ops-internal-token").onchange = () => localStorage.setItem("gvox.internalToken", $("ops-internal-token").value.trim());
  const internalHeaders = () => ({ "X-Internal-Token": $("ops-internal-token").value.trim() });

  async function refreshOps() {
    try {
      const [workers, cap] = await Promise.all([api("/v1/workers"), api("/v1/capacity")]);
      $("ops-workers").innerHTML = workers.map((w) => `<tr><td><code>${esc(w.worker_id)}</code></td><td><small>${esc(w.worker_url)}</small></td>
        <td>${w.active_sessions} / ${w.max_sessions}</td><td><span class="pill ${w.status === "live" ? (w.draining ? "ringing" : "completed") : "failed"}">${w.status}${w.draining ? " · draining" : ""}</span></td>
        <td>${fmt(w.seconds_since_seen, 0)} s</td><td>${w.status === "live" && !w.draining ? `<button class="btn secondary" style="padding:3px 8px" data-drain="${esc(w.worker_id)}">Drain</button>` : ""}</td></tr>`).join("")
        || "<tr><td colspan=6><small>no worker has registered (static MAX_CONCURRENT_CALLS pool in use)</small></td></tr>";
      document.querySelectorAll("button[data-drain]").forEach((b) => (b.onclick = async () => {
        if (!confirm(`Drain ${b.dataset.drain}? It stops taking new streams, finishes its calls, then exits.`)) return;
        try { await api(`/v1/workers/${encodeURIComponent(b.dataset.drain)}/drain`, { method: "POST" }); $("ops-workers-status").textContent = `drain requested for ${b.dataset.drain}`; refreshOps(); }
        catch (e) { $("ops-workers-status").textContent = e.message; }
      }));
      const carriers = Object.entries(cap.carriers || {}).map(([k, p]) => `<div>${esc(k)}</div><div>${p.free} / ${p.total} free · cps ${p.cps_free}</div>`).join("");
      $("ops-capacity").innerHTML = `<div>engine</div><div><b>${cap.engine.free}</b> / ${cap.engine.total} free (active ${cap.active_calls}, accepting ${cap.accepting})</div>${carriers}`;
      $("ops-updated").textContent = "updated " + new Date().toLocaleTimeString();
    } catch (e) { $("ops-workers-status").textContent = "ops refresh failed: " + e.message; }
    if (state.selectedCallId) refreshDeliveries();
  }

  async function refreshDeliveries() {
    try {
      const rows = await api(`/v1/calls/${state.selectedCallId}/deliveries`);
      $("ops-deliveries").innerHTML = rows.map((r) => `<tr><td>${r.seq}</td><td>${esc(r.event)}</td><td><span class="pill ${r.status === "delivered" ? "completed" : r.status === "dead" ? "failed" : "ringing"}">${r.status}</span></td>
        <td>${r.attempts}</td><td><small>${r.last_status_code ?? ""} ${esc((r.last_error || "").slice(0, 60))}</small></td>
        <td><button class="btn secondary" style="padding:3px 8px" data-replay="${r.seq}">Replay</button></td></tr>`).join("") || "<tr><td colspan=6><small>no webhook deliveries (call has no webhook_url)</small></td></tr>";
      document.querySelectorAll("button[data-replay]").forEach((b) => (b.onclick = async () => {
        try { await api(`/v1/calls/${state.selectedCallId}/deliveries/${b.dataset.replay}/replay`, { method: "POST" }); $("ops-deliveries-status").textContent = `seq ${b.dataset.replay} queued again`; setTimeout(refreshDeliveries, 800); }
        catch (e) { $("ops-deliveries-status").textContent = e.message; }
      }));
    } catch (e) { $("ops-deliveries-status").textContent = e.message; }
  }

  $("ops-smoke-run").onclick = async () => {
    const btn = $("ops-smoke-run");
    btn.disabled = true;
    $("ops-smoke-body").innerHTML = "";
    try {
      const start = await api("/v1/test/telephony-smoke", { method: "POST", headers: internalHeaders(),
        json: { calls: parseInt($("ops-smoke-calls").value, 10) || 3, script: $("ops-smoke-script").value.trim() || "yes" } });
      $("ops-smoke-status").textContent = `running job ${start.job_id}…`;
      const poll = setInterval(async () => {
        try {
          const job = await api(`/v1/test/telephony-smoke/${start.job_id}`, { headers: internalHeaders() });
          if (job.status === "running") return;
          clearInterval(poll); btn.disabled = false;
          const r = job.result || {};
          $("ops-smoke-status").textContent = job.status === "done"
            ? `done: ${r.completed}/${r.calls} completed, dropped ${r.dropped}, client p50 ${fmt(r.client_p50)} s / p95 ${fmt(r.client_p95)} s, engine total p50 ${fmt(r.engine_total_p50)} s`
            : `failed: ${esc(JSON.stringify(r).slice(0, 300))}`;
          $("ops-smoke-body").innerHTML = (r.per_call || []).map((c) => `<tr><td>${c.index ?? ""}</td><td><code>${esc((c.call_id || "").slice(5))}</code></td><td>${esc(c.status || c.error || "")}</td>
            <td>${c.user_turns ?? 0}↑ ${c.agent_turns ?? 0}↓</td><td>${fmt((c.client_latency_s || [])[0])}</td><td>${fmt((c.engine_total_s || [])[0])}</td><td>${c.ok ? "✓" : "✗"}</td></tr>`).join("");
          refreshCalls();
        } catch (e) { clearInterval(poll); btn.disabled = false; $("ops-smoke-status").textContent = e.message; }
      }, 2000);
    } catch (e) { btn.disabled = false; $("ops-smoke-status").textContent = "could not start: " + e.message + " (ENABLE_TEST_ENDPOINTS=true? internal token?)"; }
  };

  // ------------------------------------------------------------------ agent JSON
  $("agent-load-example").onclick = () => { $("agent-json").value = JSON.stringify(state.example, null, 2); $("agent-status").textContent = "example loaded"; };
  $("agent-validate").onclick = async () => {
    try { const v = await api("/v1/agents/validate", { method: "POST", json: JSON.parse($("agent-json").value) });
      $("agent-status").textContent = `valid: ${v.nodes} nodes, ${v.edges} edges; variables: ${v.required_variables.join(", ") || "none"}`;
    } catch (e) { $("agent-status").textContent = "invalid: " + e.message; }
  };
  $("agent-save").onclick = async () => {
    try { const r = await api("/v1/agents/" + encodeURIComponent($("agent-id").value.trim()), { method: "PUT", json: JSON.parse($("agent-json").value) });
      $("agent-status").textContent = `saved ${r.agent_id} version ${r.version}`;
    } catch (e) { $("agent-status").textContent = "save failed: " + e.message; }
  };

  // ------------------------------------------------------------------ boot
  loadSettings();
  checkHealth();
  loadConfig();
  setInterval(checkHealth, 15000);
})();
