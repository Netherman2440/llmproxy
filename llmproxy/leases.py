"""Bounded shared lease ownership; never fall back to process-local capacity."""

import asyncio
import contextlib
import hashlib
import json
import uuid

import aiohttp.web

from . import errors
from .db import get_db


class LeaseCapacityError(ValueError):
    def __init__(self, limit):
        super().__init__("Shared admission capacity exceeded")
        self.limit = limit


def shared(request):
    return request.app["config"].get("admission", {}).get("mode", "local") == "shared"


def bucket(request, kind, *identity):
    scope = request.app["config"]["admission"]["scope"]
    raw = json.dumps([scope, kind, *map(str, identity)], separators=(",", ":")).encode()
    return "llmproxy:" + hashlib.sha256(raw).hexdigest()


def _unavailable(request):
    return errors.json_error(request, aiohttp.web.HTTPServiceUnavailable,
        "Shared admission is unavailable.", openai_type="server_error",
        anthropic_type="api_error", code="admission_unavailable")


async def _call(request, awaitable, timeout):
    try:
        async with asyncio.timeout(timeout):
            return await awaitable
    except Exception:
        raise _unavailable(request) from None


async def take_window(request, name, limit, seconds):
    """Keep one admitted RPM event until expiry, not until request completion."""
    io_timeout = request.app["config"].get("admission", {}).get("io_timeout", 5.0)
    database = await _call(request, get_db(request.app["config"]["db"]["uri"]), io_timeout)
    try:
        return await _call(request,
            database.admission_acquire(name, uuid.uuid4().hex, limit, seconds), io_timeout)
    finally:
        with contextlib.suppress(Exception):
            await _call(request, database.close(), io_timeout)


@contextlib.asynccontextmanager
async def hold(request, limits):
    """Acquire all dimensions conservatively, renew, and release only this owner.

    A dedicated SQLite connection prevents a renew task from committing an
    unrelated financial transaction on the request's connection. A watchdog
    stops the handler BEFORE the earliest server lease can expire, including
    when renewal or the database fails. Backend debt needs its own durable
    binding/reconciliation; this context never invents a successful completion.
    """
    if not limits:
        yield
        return
    cfg = request.app["config"].get("admission", {})
    ttl, io_timeout = cfg.get("lease_ttl", 60.0), cfg.get("io_timeout", 5.0)
    database = await _call(request, get_db(request.app["config"]["db"]["uri"]), io_timeout)
    owner, held = uuid.uuid4().hex, {}
    loop = asyncio.get_running_loop()
    watchdog, monitor = None, None
    try:
        for name, limit in limits:
            started = loop.time()
            acquired = await _call(request, database.admission_acquire(name, owner, limit, ttl), io_timeout)
            if not acquired:
                raise LeaseCapacityError(limit)
            held[name] = started + ttl * 0.9
        watchdog = asyncio.timeout_at(min(held.values()))

        async def renew():
            delay = ttl / 4
            while True:
                await asyncio.sleep(delay)
                delay = ttl / 4
                for name in held:
                    started = loop.time()
                    try:
                        renewed = await _call(request, database.admission_renew(name, owner, ttl), io_timeout)
                    except Exception:
                        # Existing confirmed ownership remains valid only up to
                        # the unchanged earliest deadline. Retry inside that
                        # window; uncertain I/O NEVER extends the watchdog.
                        delay = min(io_timeout, ttl / 16)
                        break
                    if not renewed:
                        watchdog.reschedule(loop.time())
                        return
                    held[name] = started + ttl * 0.9
                watchdog.reschedule(min(held.values()))

        try:
            async with watchdog:
                monitor = asyncio.create_task(renew())
                try:
                    yield
                finally:
                    monitor.cancel()
                    await asyncio.gather(monitor, return_exceptions=True)
        except TimeoutError:
            if watchdog.expired():
                raise _unavailable(request) from None
            raise
    finally:
        if monitor is not None and not monitor.done():
            monitor.cancel()
            await asyncio.gather(monitor, return_exceptions=True)
        try:
            for name in held:
                try:
                    await _call(request, database.admission_release(name, owner), io_timeout)
                except Exception as error:
                    # Expiry remains the bounded fallback; never mask completed
                    # billing or retry it because admission cleanup failed.
                    request.app.logger.error("Lease cleanup failed (%s)", type(error).__name__)
        finally:
            with contextlib.suppress(Exception):
                await _call(request, database.close(), io_timeout)
