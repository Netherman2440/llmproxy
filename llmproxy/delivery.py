"""Durable conversion obligations; a remote boolean is not a financial ledger."""
import asyncio
import hashlib
from urllib.parse import urlsplit

from .accounting import AccountingUnavailable as AccountingUnavailable
from .accounting import AccountingConflict, _call, _text, clean_owner, encode
from .usage import ID, timestamp


class DeliveryUnresolved(AccountingConflict):
    pass


def normalize(binding, pages):
    if type(binding) is not dict or type(pages) is not int or not 1 <= pages < 2**31:
        raise ValueError('Invalid conversion obligation')
    value = {'owner': clean_owner(binding.get('owner')), 'pages': pages}
    for name in ('backend_alias', 'backend_url', 'device', 'request_id', 'job_id'):
        value[name] = _text(binding.get(name), name, maximum=512)
    if not ID.fullmatch(value['job_id']) or not ID.fullmatch(value['request_id']):
        raise ValueError('Invalid conversion source identity')
    url = urlsplit(value['backend_url'])
    if (url.scheme not in ('http', 'https') or not url.hostname or url.username is not None
            or url.password is not None or url.query or url.fragment):
        raise ValueError('Invalid conversion backend URL')
    value['created_at'] = timestamp(binding.get('created_at')).isoformat(timespec='microseconds')
    return value


def delivery_key(binding):
    identity = {key: binding[key] for key in ('backend_alias', 'backend_url', 'job_id')}
    identity['owner_id'] = binding['owner']['id']
    return hashlib.sha256(encode(identity).encode()).hexdigest()


def fingerprint(binding):
    immutable = {key: value for key, value in binding.items() if key != 'created_at'}
    return hashlib.sha256(encode(immutable).encode()).hexdigest()


def product(binding):
    return (binding['backend_alias'] + '/' + binding['device'] + '/conversion').lower()


async def prepare_delivery(db, binding, pages):
    value = normalize(binding, pages)
    return await _call(db.delivery_prepare(delivery_key(value), value, fingerprint(value)))


async def settle_delivery(db, intent, outcome=None):
    if type(intent) is not dict or type(intent.get('binding')) is not dict:
        raise AccountingConflict('Invalid delivery intent')
    value = normalize(intent['binding'], intent['binding'].get('pages'))
    if intent.get('key') != delivery_key(value):
        raise AccountingConflict('Delivery owner or source conflict')
    if outcome is not None:
        if type(outcome) is not dict or set(outcome) != {'delivery_claimed'} or type(outcome['delivery_claimed']) is not bool:
            raise ValueError('Invalid delivery claim outcome')
        await _call(db.delivery_outcome(intent['key'], fingerprint(value), outcome['delivery_claimed']))
    return await _call(db.delivery_settle(intent['key'], fingerprint(value)))


async def pending_deliveries(db, limit=100, after=None):
    if type(limit) is not int or not 1 <= limit <= 100:
        raise ValueError('Invalid delivery page limit')
    return await _call(db.delivery_pending(limit, after))


async def recover_once(app, limit=100):
    from .db import get_db
    db = await get_db(app['config']['db']['uri'])
    try:
        intents = await pending_deliveries(db, limit, app.get('delivery_after'))
        if not intents and app.get('delivery_after'):
            app['delivery_after'] = None
            intents = await pending_deliveries(db, limit)
        count = 0
        async with asyncio.timeout(15):
            for intent in intents:
                # An unknown/legacy claim is not evidence of successful local
                # ownership. Only durable positive proof authorizes a bill.
                if intent['state'] == 'confirmed':
                    count += int(await settle_delivery(db, intent))
                app['delivery_after'] = intent['key']
        return count
    finally:
        await db.close()


def setup(app):
    async def loop():
        while True:
            try:
                await recover_once(app)
                app['delivery_recovery_healthy'] = True
            except Exception as error:
                app['delivery_recovery_healthy'] = False
                app.logger.error('Delivery recovery failed: category=%s', type(error).__name__)
            await asyncio.sleep(5)

    async def start(app):
        app['delivery_recovery_task'] = asyncio.create_task(loop())

    async def stop(app):
        task = app.get('delivery_recovery_task')
        if task is not None:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)

    app.on_startup.append(start)
    app.on_shutdown.append(stop)
