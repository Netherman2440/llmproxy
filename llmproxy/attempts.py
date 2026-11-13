"""Durable HTTP usage obligations. Unknown usage never authorizes a guessed bill."""
import asyncio
import contextlib
import datetime
import hashlib
import uuid
from decimal import Decimal
from urllib.parse import urlsplit

from .accounting import AccountingConflict, _call, _text, clean_owner, encode
from .products import product
from .usage import timestamp

ENDPOINTS = frozenset(('/v1/chat/completions', '/v1/completions', '/v1/messages', '/v1/responses'))


def normalize(binding):
    if type(binding) is not dict:
        raise ValueError('Invalid HTTP attempt')
    value = {'owner': clean_owner(binding.get('owner'))}
    for name in ('request_id', 'backend_alias', 'backend_url', 'device', 'endpoint'):
        value[name] = _text(binding.get(name), name, maximum=512)
    url = urlsplit(value['backend_url'])
    try:
        request_uuid = uuid.UUID(value['request_id'])
    except ValueError as error:
        raise ValueError('Invalid HTTP request identity') from error
    if (len(value['request_id']) not in (32, 36)
            or request_uuid.hex != value['request_id'].replace('-', '') or value['endpoint'] not in ENDPOINTS
            or url.scheme not in ('http', 'https') or not url.hostname
            or url.username is not None or url.password is not None or url.query or url.fragment):
        raise ValueError('Invalid HTTP attempt source')
    value['created_at'] = timestamp(binding.get('created_at')).isoformat(timespec='microseconds')
    return value


def fingerprint(value):
    return hashlib.sha256(encode(value).encode()).hexdigest()


def proof(binding, resources):
    expected = {product(binding['backend_alias'], binding['device'], 'prompt'),
        product(binding['backend_alias'], binding['device'], 'completion')}
    if type(resources) is not dict or set(resources) != expected:
        raise ValueError('Invalid HTTP usage products')
    result = {}
    for key, value in resources.items():
        if (type(value) not in (int, Decimal) or not Decimal(value).is_finite()
                or not 0 <= value < 2**53 or int(value) != value):
            raise ValueError('Invalid HTTP token quantity')
        result[key] = int(value)
    return result


def validate(intent):
    if type(intent) is not dict:
        raise AccountingConflict('Invalid HTTP attempt identity')
    value = normalize(intent.get('binding'))
    if intent.get('key') != value['request_id']:
        raise AccountingConflict('HTTP attempt source conflict')
    return value


async def prepare(db, binding):
    value = normalize(binding)
    return await _call(db.attempt_prepare(value['request_id'], value, fingerprint(value)))


async def confirm(db, intent, resources, *, observed_at=None):
    value = validate(intent)
    resources = proof(value, resources)
    observed_at = timestamp(observed_at or datetime.datetime.now(datetime.UTC).isoformat()).isoformat(timespec='microseconds')
    await _call(db.attempt_confirm(intent['key'], fingerprint(value), resources, observed_at))


async def settle(db, intent):
    value = validate(intent)
    return await _call(db.attempt_settle(intent['key'], fingerprint(value)))


async def abandon(db, intent, *, rejected=False):
    if type(rejected) is not bool:
        raise ValueError('Invalid HTTP rejection proof')
    value = validate(intent)
    await _call(db.attempt_abandon(intent['key'], fingerprint(value), rejected))


async def pending(db, limit=100, after=None):
    if type(limit) is not int or not 1 <= limit <= 100:
        raise ValueError('Invalid HTTP attempt page')
    return await _call(db.attempt_pending(limit, after))


@contextlib.asynccontextmanager
async def track_request(request, user, backend_alias, backend_cfg):
    if request.path not in ENDPOINTS:
        yield
        return
    from .db import get_db
    db = await get_db(request.app['config']['db']['uri'], request)
    intent = await prepare(db, {'request_id': str(request['request_id']), 'owner': user,
        'backend_alias': backend_alias, 'backend_url': backend_cfg['url'], 'device': backend_cfg['device'],
        'endpoint': request.path, 'created_at': datetime.datetime.now(datetime.UTC).isoformat()})
    request['http_attempt'] = intent
    try:
        yield
    finally:
        if not request.get('http_attempt_settled'):
            status = request.get('http_attempt_backend_status')
            rejected = status is not None and 400 <= status < 500
            if not rejected:
                request.app['http_attempt_recovery_healthy'] = False
                request.app.logger.error('HTTP usage unresolved: request_id=%s backend=%s',
                    intent['key'], backend_alias)
            await abandon(db, intent, rejected=rejected)


async def recover_once(app):
    from . import metrics
    from .db import get_db
    db = await get_db(app['config']['db']['uri'])
    try:
        intents = await pending(db, after=app.get('http_attempt_after'))
        if not intents and app.get('http_attempt_after'):
            app['http_attempt_after'] = None
            intents = await pending(db)
        count = 0
        async with asyncio.timeout(15):
            for intent in intents:
                if intent['state'] == 'confirmed':
                    count += int(await settle(db, intent))
                app['http_attempt_after'] = intent['key']
        # Global bounded existence query, not just this fair page: unresolved
        # attempts cannot disappear from health merely by moving the cursor.
        healthy = not bool(await pending(db, 1))
        if not healthy and app.get('http_attempt_recovery_healthy') is not False and hasattr(app, 'logger'):
            app.logger.error('Retained HTTP usage obligations require recovery or operator reconciliation')
        app['http_attempt_recovery_healthy'] = healthy
        metrics.RECOVERY_HEALTHY.labels(journal='http_attempt').set(1 if healthy else 0)
        return count
    finally:
        await db.close()


def main():
    import argparse
    import json
    import os
    import sys
    parser = argparse.ArgumentParser(description='Inspect retained HTTP obligations using a DB environment reference')
    parser.add_argument('--db-env', default='LLMPROXY_DB_URI')
    parser.add_argument('--after')
    parser.add_argument('--recover-confirmed', action='store_true', help='Explicitly settle only retained authoritative usage')
    args = parser.parse_args()

    async def run():
        from .db import get_db, shutdown_all
        uri = os.environ.get(args.db_env)
        if not uri:
            raise ValueError('Database reference is not set')
        db = await get_db(uri)
        try:
            rows = await pending(db, after=args.after)
            for intent in rows:
                billed = False
                if args.recover_confirmed and intent['state'] == 'confirmed':
                    billed = await settle(db, intent)
                print(json.dumps({'attempt_id': intent['key'], 'backend_alias': intent['binding']['backend_alias'],
                    'endpoint': intent['binding']['endpoint'], 'state': intent['state'], 'recovered': billed,
                    'resources': intent['resources'], 'observed_at': intent['observed_at']}))
        finally:
            await db.close()
            await shutdown_all()
    try:
        asyncio.run(run())
    except Exception as error:
        print(json.dumps({'status': 'error', 'category': type(error).__name__}), file=sys.stderr)
        raise SystemExit(1) from None


if __name__ == '__main__':
    main()
