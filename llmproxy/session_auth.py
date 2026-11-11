"""Authorization lifetime shared by legacy and receipt-v1 WebSockets."""
import asyncio
import datetime
import time

from aiohttp import web

from . import auth


OWNER_FIELDS = ('id', '_user_id', '_namespace', '_org_id', '_tier')


def owner_identity(user):
    # Comments and credential material are neither ownership nor price tier.
    return {name: user.get(name) for name in OWNER_FIELDS}


async def fresh_user(request):
    clone = request.clone()
    clone.pop('db', None)
    try:
        return await auth.require_auth(clone, fresh=True)
    finally:
        db = clone.pop('db', None)
        if db is not None:
            await db.close()


async def watch(request, user, started):
    maximum = request.app['config'].get('realtime', {}).get('max_session_seconds', 3600)
    next_check = time.monotonic()
    identity = owner_identity(user)
    while True:
        if not auth.valid_expiry(user.get('expires'), datetime.datetime.now(datetime.UTC)):
            return 1008
        if time.monotonic() - started >= maximum:
            return 1000
        if time.monotonic() >= next_check:
            try:
                async with asyncio.timeout(0.5):
                    fresh = await fresh_user(request)
                if owner_identity(fresh) != identity:
                    return 1008
            except web.HTTPUnauthorized:
                return 1008
            except (Exception, web.GracefulExit):
                # Unavailable authorization must stop NEW work, not historical billing.
                return 1011
            next_check = time.monotonic() + 1
        await asyncio.sleep(0.2)
