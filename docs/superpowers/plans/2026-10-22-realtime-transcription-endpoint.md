# Realtime Transcription Endpoint Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a WebSocket endpoint `/v1/realtime` that fronts realtime speech-to-text backends (`type = "realtime"`): CGC API-key auth, optional `?model=` routing, per-user rate limits, usage metering — with billing events deliberately deferred.

**Architecture:** One new aiohttp WS route and module (`llmproxy/realtime.py`) plus a new backend type. The proxy authenticates the client (Bearer header, or the `bearer, <key>` subprotocol for browsers), opens a second WS to the backend using the backend's config token, and pumps text frames 1:1 both ways. Downstream frames are sniffed for `conversation.item.input_audio_transcription.completed` events to meter `usage.seconds` (Prometheus `llmproxy_audio_seconds_total`); the same hook is where billing will plug in once the per-segment vs per-session decision lands. The client's CGC key never reaches the backend.

**Tech Stack:** aiohttp ~= 3.10.9 (WS server + client, no new dependency), prometheus_client, stdlib unittest with aiohttp.test_utils.

**Spec:** Self-contained here; design verified against `Comtegra/llmproxy` master and the OpenAI Realtime transcription subset contract spoken by realtime STT backends (session.update / input_audio_buffer.* events, `usage.seconds` in completed events, close 1008 on bad key, 1013 on session limits).

## Global Constraints

- No new dependencies; aiohttp's WebSocket support on both sides.
- All tests unittest-discoverable (`python -m unittest`); follow the `LLMProxyAppTestCase` harness in `tests/test_proxy.py`.
- **No billing writes in this feature.** Billing (per-segment vs per-session events) is a deferred owner decision; the usage-sniffing hook must be one call site so `billing.record` can be added later without restructuring.
- The client's API key must never be forwarded to the backend: the backend sees `Authorization: Bearer <config token>` and no client subprotocols.
- `auth.require_auth` behavior for existing endpoints is unchanged (same errors, same cache).
- Conventional commits with scope (`feat(realtime): …`, `test(realtime): …`, `docs: …`); English.
- Existing auth/backend error semantics: 401 plain, 404/400/429 OpenAI JSON bodies via `errors.json_error` / `proxy._model_error`.

## Review Focus

1. A model name that exists but is of another backend type must get 404 `model_not_supported` at the WS handshake, never a backend connection. (Tested in Task 4)
2. The client's key must never appear in the backend handshake headers. (Task 5)
3. A session idle longer than the global `timeout_read` must NOT be killed by the proxy's HTTP client timeout. (Task 7)
4. A backend auth rejection (close 1008) must surface as close 1011 plus a CRITICAL log, never leaking the backend's reason. (Task 5)
5. The rate-limit concurrency slot must be released when a client disconnects mid-session, so the next session is not 429-locked. (Task 6)

---

### Task 1: Auth token extraction (`require_token` + `extract_token`)

**Files:**
- Modify: `llmproxy/auth.py`
- Test: `tests/test_realtime.py` (new)

**Interfaces:**
- Produces: `extract_token(req) -> str | None` (Authorization Bearer, else the `bearer, <key>` subprotocol pair, else None); `async require_token(req, token) -> dict` (digest → cache → DB; `token is None` → 401 "Incorrect API key", no cache write, no DB hit); `require_auth(req)` keeps its behavior, delegating to `require_token`.

- [ ] **Step 1: Write failing tests** in `tests/test_realtime.py`: `extract_token` from Bearer header; from `Sec-WebSocket-Protocol: bearer, tok`; None when absent; `require_token(req, None)` → `HTTPUnauthorized` with no DB lookup (patch `get_db` to assert not called); a valid subprotocol token authenticates against the test key.
- [ ] **Step 2: Run** `python -m unittest tests.test_realtime` → FAIL (`extract_token` undefined).
- [ ] **Step 3: Implement** in `llmproxy/auth.py`: split `require_auth` into header parsing + `require_token` (shared digest/cache/DB block, unchanged messages); add `extract_token` reading both sources (header first, then subprotocol exactly `"bearer", <key>`).
- [ ] **Step 4: Run** `python -m unittest tests.test_realtime tests.test_cache` → PASS (new + existing auth tests).
- [ ] **Step 5: Commit** `feat(realtime): auth token extraction for WebSocket handshakes`

### Task 2: `realtime` backend type (config)

**Files:**
- Modify: `llmproxy/config.py` (`BACKEND_TYPES`, `validate`)
- Test: `tests/test_realtime.py`

**Interfaces:**
- Produces: `"realtime"` accepted in `BACKEND_TYPES`; validation: `token` and `device` required (non-empty strings) for `type = "realtime"`; optional `heartbeat` (positive number, seconds).

- [ ] **Step 1: Write failing tests**: `validate` accepts a realtime backend; rejects missing `token`; rejects missing `device`; rejects `heartbeat = 0` / `"x"`.
- [ ] **Step 2: Run** → FAIL.
- [ ] **Step 3: Implement**: extend `BACKEND_TYPES`; in `validate`, for `meta["type"] == "realtime"` require `token` and `device` like conversion does; validate `heartbeat` when present (int/float, > 0, finite).
- [ ] **Step 4: Run** → PASS.
- [ ] **Step 5: Commit** `feat(realtime): realtime backend type and config validation`

### Task 3: Mock realtime backend (WS)

**Files:**
- Modify: `tests/mockbackend.py`

**Interfaces:**
- Produces: mock WS route `GET /v1/realtime`; `app["ws_control"]` dict (keys: `token` expected backend token — mismatch → error event + close 1008; `reject_auth` → 1008 unconditionally; `max_sessions` → close 1013; `close_code` → close with that code after echo; `usage_seconds` sent in the completed event, default 3.7; `echo` bool default True); `app["ws_records"]` list appending `{"headers": dict(req.headers), "model": req.query.get("model"), "frames": []}` per session. On `input_audio_buffer.commit` it sends `{"type": "conversation.item.input_audio_transcription.completed", "usage": {"type": "duration", "seconds": usage_seconds}}`; other client text frames are echoed back when `echo`.

- [ ] **Step 1: Write failing tests** (in `tests/test_realtime.py`): direct against `mockbackend.create_app()` — subprotocol-free client with correct Bearer gets 101 and receives the scripted completed event after commit; wrong token → close 1008; `max_sessions` → close 1013.
- [ ] **Step 2: Run** → FAIL.
- [ ] **Step 3: Implement** the `ws_realtime` handler + route in `create_app` (initialize `ws_control`/`ws_records` there).
- [ ] **Step 4: Run** → PASS.
- [ ] **Step 5: Commit** `test(realtime): mock realtime backend for proxy tests`

### Task 4: Route, backend selection, handshake auth

**Files:**
- Create: `llmproxy/realtime.py`
- Modify: `llmproxy/app.py` (import `realtime`, add `aiohttp.web.get("/v1/realtime", realtime.realtime)`)
- Test: `tests/test_realtime.py`

**Interfaces:**
- Consumes: Task 1 (`extract_token`, `require_token`), Task 2 (type accepted by `validate` — note: `create_app` calls `config.validate`, so test app configs must include valid realtime backends), Task 3 (mock).
- Produces: `async def realtime(req) -> aiohttp.web.WebSocketResponse` — the WS handler; `_select_backend(req) -> (b_name, b_cfg)`: `?model=` given → must exist (404 `model_not_found`) and be type `realtime` (404 `model_not_supported`, via `proxy._model_error`); absent → the single realtime backend, 400 `bad_request` (param `model`) when more than one is configured, 404 `model_not_found` ("No realtime model is configured.") when none. Auth: `auth.require_token(req, auth.extract_token(req))`; `HTTPUnauthorized` re-raised as `errors.json_error(... HTTPUnauthorized, "Invalid or missing API key.", openai_type="authentication_error", anthropic_type="authentication_error")` (mirrors `chat.chat`). Success: client `WebSocketResponse(protocols=("bearer",) if negotiated)`, prepared after the backend WS connects, provenance headers set before `prepare` via `provenance.headers(app["config"])`.

- [ ] **Step 1: Write failing tests** (proxy-level, `LLMProxyAppTestCase` subclass with one `realtime` backend pointing at the mock): no credentials → 401 with `authentication_error`; wrong key → 401; `?model=doesnotexist` → 404 `model_not_found`; `?model=mymodel` (a chat backend) → 404 `model_not_supported`; valid key + subprotocol auth → handshake 101 and the response's `Sec-WebSocket-Protocol` is `bearer`; Bearer-header auth → 101; two realtime backends, no `?model=` → 400 (test config needs a second realtime alias to the same mock).
- [ ] **Step 2: Run** → FAIL.
- [ ] **Step 3: Implement** `realtime.py`: selection + auth + rate-limit slot (`ratelimit.slot`) + `metrics.track_user_request` + backend connect (`app["client"].ws_connect`, headers `Authorization: Bearer <b_cfg["token"]>`, `heartbeat=b_cfg.get("heartbeat", 30)`, url `yarl.URL(b_cfg["url"]) / "v1/realtime"` with `model=` set to `b_cfg.get("model") or b_name`) + prepare + pump shell (frames forwarded both ways — the pump body is Task 5; for Task 4 a minimal echo-through pump suffices to prove the 101 path).
- [ ] **Step 4: Run** `python -m unittest tests.test_realtime` → PASS.
- [ ] **Step 5: Run the whole suite** `python -m unittest` → PASS (known pre-existing Windows failure `test_backend_unreachable_on_poll_is_502` excepted).
- [ ] **Step 6: Commit** `feat(realtime): /v1/realtime WebSocket endpoint with auth and model routing`

### Task 5: Session pump, close mapping, usage metering

**Files:**
- Modify: `llmproxy/realtime.py`
- Test: `tests/test_realtime.py`

**Interfaces:**
- Consumes: Task 4 handler; Task 3 mock (`ws_records` to assert headers/model).
- Produces: `_forward_up(ws_client, ws_backend)` and `_forward_down(ws_backend, ws_client, on_completed)` pumps (TEXT and BINARY forwarded; CLOSED handled by the caller); main loop: `asyncio.wait(..., FIRST_COMPLETED)`, then close both sides, drain the backend side bounded (30 s timeout, cancel on overrun); close-code mapping: backend 1008 → client 1011 + `app.logger.critical("realtime backend rejected the session...")` (no reason leaked), 1013 → 1013, 1000/1001 → as-is, anything else → 1011; client close → close the backend with the same code. `_on_completed(b_name, evt)`: `usage.seconds` numeric > 0 → `metrics.AUDIO_SECONDS_TOTAL.labels(b_name).inc(seconds)` — single hook site marked `# billing deferred`.

- [ ] **Step 1: Write failing tests**: client event echoed back (pump up→down); `input_audio_buffer.commit` → mock completed event arrives at client and `llmproxy_audio_seconds_total{model=...}` grows by 3.7; a non-completed frame and unparseable-but-text frames pass through untouched; `ws_records` shows `Authorization: Bearer mybackendtoken` and NO `Sec-WebSocket-Protocol` header and `model` rewritten per backend `model` config; backend `reject_auth` (1008) → client close 1011; `max_sessions` (1013) → 1013; backend normal close → client sees that close; client closes mid-session → handler returns (test server closes cleanly).
- [ ] **Step 2: Run** → FAIL.
- [ ] **Step 3: Implement** the pumps + mapping + `_on_completed` as specified.
- [ ] **Step 4: Run** → PASS.
- [ ] **Step 5: Commit** `feat(realtime): bidirectional frame pump, close-code mapping, usage metering`

### Task 6: Rate limits across the session

**Files:**
- Modify: `llmproxy/realtime.py` (only if wiring is off; slot was added in Task 4)
- Test: `tests/test_realtime.py`

- [ ] **Step 1: Write failing tests**: backend `rate_limit = {concurrency: 1}`: hold one session open, second concurrent handshake → 429 JSON body pre-upgrade; close the first → a new handshake succeeds; `rpm = 1`: second handshake within the window → 429 with `Retry-After`.
- [ ] **Step 2: Run** → FAIL (or PASS if Task 4 already wired it — then the test is the contract, keep it).
- [ ] **Step 3: Fix/adjust** the slot scope to cover the whole session.
- [ ] **Step 4: Run** → PASS.
- [ ] **Step 5: Commit** `test(realtime): session-scoped rate limiting`

### Task 7: Session liveness under the HTTP client timeout

**Files:**
- Test: `tests/test_realtime.py`

- [ ] **Step 1: Write the failing-or-proving test**: with the harness's `timeout_read: 1`, open a session, stay silent 1.5 s, then send an event and expect the echo. If it FAILS, the session ClientTimeout leaks into WS reads — fix by passing an explicit `ClientTimeout(sock_read=None)` (or the aiohttp-3.10 equivalent for `ws_connect`) when connecting.
- [ ] **Step 2: Run** → PASS (possibly after the fix above).
- [ ] **Step 3: Commit** `test(realtime): idle sessions survive the global read timeout`

### Task 8: Docs

**Files:**
- Modify: `README.md` (backend `type` table row; a `## Realtime transcription (/v1/realtime)` section: OpenAI-Realtime-subset proxying, optional `?model=`, Bearer + `bearer, <key>` subprotocol auth, no client key forwarded, `llmproxy_audio_seconds_total` metering, and the explicit note that billing events are not yet written — pending the per-segment vs per-session decision; `verify_ssl`/`heartbeat`/`health_path` options)
- Modify: `llmproxy/config.toml` (commented example realtime backend)

- [ ] **Step 1: Write** both docs sections.
- [ ] **Step 2: Verify** by reading them back; run `python -m unittest` → PASS.
- [ ] **Step 3: Commit** `docs(realtime): document /v1/realtime and the realtime backend type`
