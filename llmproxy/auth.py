import collections
import datetime
import hashlib
import hmac
import time

import aiohttp.web

from . import metrics
from .db import DatabaseError, get_db
from .expiry import normalize_expires as _normalize_expires, valid_expiry

# Default TTL (seconds) for the in-process auth cache. 0 disables it.
DEFAULT_CACHE_TTL = 5

# Negative results (unknown key) are cached too, otherwise a flood of invalid
# keys still costs one database round trip each. Their TTL is capped
# independently of auth_cache_ttl so that a key created moments after someone
# probed for it does not stay rejected for a long configured TTL.
NEGATIVE_CACHE_TTL = 5

# The cache is an LRU because digests of *unknown* keys are attacker-controlled:
# without a cap, negative caching hands over unbounded memory growth.
CACHE_MAX_ENTRIES = 8192

# digest -> (row | None, expires_at | None, monotonic deadline).
# row is None for a negative entry; expires_at is kept alongside the row rather
# than inside it so the dict handed to callers is identical on hit and miss.
_cache = collections.OrderedDict()


def flush_cache():
    """Drop every cached lookup.

    Wired to SIGHUP via reload_config: the TTL bounds how long a REVOKED key
    keeps working, and this is the operator's lever to make that immediate
    without restarting the proxy."""
    _cache.clear()


def _cache_get(digest, now):
    entry = _cache.get(digest)
    if entry is None:
        return None

    if entry[2] <= now:
        del _cache[digest]
        return None

    _cache.move_to_end(digest)
    return entry


def _cache_put(digest, row, expires_at, ttl, now):
    _cache[digest] = (row, expires_at, now + ttl)
    _cache.move_to_end(digest)
    while len(_cache) > CACHE_MAX_ENTRIES:
        _cache.popitem(last=False)


async def require_auth(req, *, fresh=False):
    scheme, _, token = req.headers.get("Authorization", "").partition(" ")
    if scheme != "Bearer" or not token:
        raise aiohttp.web.HTTPUnauthorized(text="Unsupported authorization scheme")

    digest = hashlib.sha256(token.encode()).hexdigest()

    ttl = req.app["config"].get("auth_cache_ttl", DEFAULT_CACHE_TTL)
    now = time.monotonic()

    if not fresh and ttl > 0 and (entry := _cache_get(digest, now)) is not None:
        metrics.AUTH_CACHE_HITS_TOTAL.inc()
        row, expires_at, _ = entry
        if row is None:
            raise aiohttp.web.HTTPUnauthorized(text="Incorrect API key")
        # Expiry remains authoritative on both cache paths; corrupt dates
        # never become unlimited access even if an adapter returned the row.
        if not valid_expiry(expires_at, datetime.datetime.now(datetime.UTC)):
            del _cache[digest]
            raise aiohttp.web.HTTPUnauthorized(text="Incorrect API key")
        # A copy, so a handler mutating the user dict cannot poison the cache.
        return dict(row)

    metrics.AUTH_CACHE_MISSES_TOTAL.inc()

    # Only on a miss do we need a database connection at all; on the hit path
    # above the request never opens one.
    db = await get_db(req.app["config"]["db"]["uri"], req)

    try:
        rows = await db.user_list(digest)
    except DatabaseError as e:
        req.app.logger.critical(e)
        raise aiohttp.web.GracefulExit() from e

    if not rows:
        if ttl > 0:
            _cache_put(digest, None, None, min(ttl, NEGATIVE_CACHE_TTL), now)
        raise aiohttp.web.HTTPUnauthorized(text="Incorrect API key")

    # user_list deliberately supports prefix searches for the operator CLI.
    # Authentication must independently require the full digest, regardless
    # of adapter filtering or row order.
    row = next((candidate for candidate in rows
        if isinstance(candidate.get("secret"), str)
        and hmac.compare_digest(candidate["secret"].encode(), digest.encode())), None)
    if row is None or not valid_expiry(row.get("expires"),
            datetime.datetime.now(datetime.UTC)):
        raise aiohttp.web.HTTPUnauthorized(text="Incorrect API key")

    if ttl > 0:
        expires_at = _normalize_expires(row.get("expires"))
        _cache_put(digest, dict(row), expires_at, ttl, now)

    return row
