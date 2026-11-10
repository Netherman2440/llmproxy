"""Phase 1 rate limiting: in-memory ``rpm`` + ``concurrency``.

Limits resolve by precedence from two levels — per-model
(``[backends.X.rate_limit]``) then global (``[rate_limit]``). The resolver
loops over a dimension tuple and walks a level list, so phase 2 extends it
additively: prepend a per-user level and append ``prompt_tpd`` /
``completion_tpd``. ``slot`` enforces whatever dimensions the resolver returns
and needs no change when a dimension or level is added.

0 means unlimited (exempt -> not enforced); absent/None means fall through to
the next level. The counter key ("bucket") is per ``(user, model)`` when the
limit came from a per-model config and per ``(user,)`` when it came from the
global config, so a global ``rpm`` is N-per-user (not one hard cap shared by
all users) and a per-model limit is scoped to that model.

Counters are process-local and in-memory; a restart resets the windows.
"""

import asyncio
import collections
import contextlib
import math
import time

import aiohttp.web

from . import errors, leases, metrics

# Dimensions enforced in phase 1. Phase 2 appends the two ``*_tpd`` entries.
_DIMENSIONS = ("rpm", "concurrency")

# Sliding-window length for rpm, in seconds.
RPM_WINDOW = 60

# In-memory state. Keyed by the bucket tuple returned from resolve().
# _rpm: bucket -> deque of monotonic timestamps (one entry per admitted request;
#   evicted lazily when older than RPM_WINDOW). len() == count, never sum().
# _concurrency: bucket -> in-flight count; released in slot's finally.
_rpm = {}
_concurrency = {}

# User-id component for the bucket when no auth row is available.
_ANON = "__anon__"


def flush():
    """Reset rate windows without forgetting ownership of active slots.

    SIGHUP may change admission policy, but an older request still owns its
    concurrency slot until its finally runs. Clearing that counter would both
    admit too much work and let the old release decrement a newer request.
    """
    _rpm.clear()


def resolve(user, model, cfg):
    """Return ``{dimension: (limit, bucket_key)}`` for dimensions with a limit.

    Precedence (phase 1): per-model then global. ``0`` exempts (omitted);
    absent falls through. Bucket key is ``(uid, model)`` for a per-model limit
    and ``(uid,)`` for a global one, so the counter is per-user in both cases.
    """
    uid = user["id"] if user else _ANON
    model_rl = cfg.get("backends", {}).get(model, {}) \
        .get("rate_limit", {})
    global_rl = cfg.get("rate_limit", {})

    resolved = {}
    for dim in _DIMENSIONS:
        v = model_rl.get(dim)
        if v is not None:
            if v != 0:
                resolved[dim] = (v, (uid, model))
            continue  # 0 => unlimited; per-model present => stop here
        v = global_rl.get(dim)
        if v is not None and v != 0:
            resolved[dim] = (v, (uid,))
        # else: nothing configured => no enforcement for this dimension
    return resolved


def _reject(f_req, dimension, limit, b_name, oldest=None):
    """Build (and return, not raise) the 429 for one dimension.

    Body flavour follows the client's API (errors.json_error). Retry-After
    is deterministic only for rpm (seconds to the oldest in-window entry
    expiring); concurrency omits it. The rejection metric is incremented here.
    """
    metrics.RATE_LIMIT_REJECTIONS_TOTAL.labels(b_name, dimension).inc()

    if dimension == "rpm":
        msg = "Rate limit exceeded: %d rpm (model '%s')." % (limit, b_name)
        # Shared windows use server expiry, not this process's monotonic
        # timestamps: a conservative full window is safe without clock skew.
        oldest = time.monotonic() if oldest is None else oldest
        retry_after = max(1, math.ceil(
            RPM_WINDOW - (time.monotonic() - oldest)))
    else:
        msg = "Concurrency limit exceeded: %d." % limit
        retry_after = None

    headers = {}
    if retry_after is not None:
        headers["Retry-After"] = str(retry_after)
    return errors.json_error(f_req, aiohttp.web.HTTPTooManyRequests, msg,
        openai_type="rate_limit_exceeded", anthropic_type="rate_limit_error",
        code="rate_limit_exceeded", headers=headers)


class ParserAdmission:
    """Application-owned counters; reload never replaces an active owner."""

    def __init__(self):
        self.active = 0
        self.users = {}


@contextlib.asynccontextmanager
async def parser_slot(f_req, user):
    cfg = f_req.app["config"].get("parsing", {})
    state = f_req.app["parser_admission"]
    uid = user["id"] if user else _ANON
    global_limit = cfg.get("concurrency", 4)
    user_limit = cfg.get("per_user", 2)
    timeout = cfg.get("timeout", 30.0)
    local_owned = False
    shared_context = contextlib.nullcontext()
    if leases.shared(f_req):
        shared_context = leases.hold(f_req, [
            (leases.bucket(f_req, "parser-global"), global_limit),
            (leases.bucket(f_req, "parser-user", uid), user_limit)])
    else:
        if state.active >= global_limit:
            raise _reject(f_req, "parsing", global_limit, "__parsing__")
        if state.users.get(uid, 0) >= user_limit:
            raise _reject(f_req, "parsing", user_limit, "__parsing__")
        state.active += 1
        state.users[uid] = state.users.get(uid, 0) + 1
        local_owned = True
    try:
        try:
            async with shared_context:
                async with asyncio.timeout(timeout):
                    yield
        except leases.LeaseCapacityError as error:
            raise _reject(f_req, "parsing", error.limit, "__parsing__") from None
        except TimeoutError:
            raise errors.json_error(f_req, aiohttp.web.HTTPRequestTimeout,
                "Request body parsing timed out.", openai_type="timeout_error",
                anthropic_type="api_error", code="request_timeout") from None
    finally:
        if local_owned:
            state.active -= 1
            state.users[uid] -= 1
            if state.users[uid] == 0:
                del state.users[uid]


@contextlib.asynccontextmanager
async def slot(f_req, user, b_name, b_cfg):
    """Enforce rate limits around one backend request.

    Ordering (§6.2): rpm then concurrency — both free in-memory checks. A
    rejected request never opens the backend connection (the 429 is raised
    before ``yield``). Concurrency is released in ``finally`` so it covers
    normal completion, handler exceptions, backend timeouts and mid-stream
    client disconnect (the streaming drain keeps the handler alive).
    """
    resolved = resolve(user, b_name, f_req.app["config"])

    if leases.shared(f_req):
        if "rpm" in resolved:
            limit, key = resolved["rpm"]
            admitted = await leases.take_window(f_req,
                leases.bucket(f_req, "rpm", *key), limit, RPM_WINDOW)
            if not admitted:
                raise _reject(f_req, "rpm", limit, b_name)
        dimensions = []
        if "concurrency" in resolved:
            limit, key = resolved["concurrency"]
            dimensions.append((leases.bucket(f_req, "model-concurrency", *key), limit))
        try:
            async with leases.hold(f_req, dimensions):
                yield
        except leases.LeaseCapacityError as error:
            raise _reject(f_req, "concurrency", error.limit, b_name) from None
        return

    # 1. rpm — sliding window of admitted timestamps.
    if "rpm" in resolved:
        limit, key = resolved["rpm"]
        now = time.monotonic()
        dq = _rpm.get(key)
        if dq is not None:
            while dq and now - dq[0] >= RPM_WINDOW:
                dq.popleft()
            if not dq:
                del _rpm[key]
                dq = None
        if dq is not None and len(dq) >= limit:
            raise _reject(f_req, "rpm", limit, b_name, oldest=dq[0])
        if dq is None:
            dq = _rpm[key] = collections.deque()
        dq.append(now)

    # 2. concurrency — in-flight counter. Never hold a slot for a rejection.
    acquired = None
    if "concurrency" in resolved:
        limit, key = resolved["concurrency"]
        cur = _concurrency.get(key, 0)
        if cur >= limit:
            raise _reject(f_req, "concurrency", limit, b_name)
        _concurrency[key] = cur + 1
        acquired = key

    try:
        yield
    finally:
        if acquired is not None:
            n = _concurrency.get(acquired, 0) - 1
            if n <= 0:
                _concurrency.pop(acquired, None)
            else:
                _concurrency[acquired] = n
