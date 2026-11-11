"""Bounded, service-authenticated receipt recovery, independent of user keys."""
import asyncio
import json
import time

import aiohttp
import yarl

from . import accounting
from .db import get_db
from .proxy import read_limited
from .usage import ID, parse_receipt


class BackendUnavailable(RuntimeError):
    pass


class RetentionGap(ValueError):
    pass


class RequestPacer:
    def __init__(self):
        self.lock = asyncio.Lock()
        self.next_request = 0

    async def take(self):
        async with self.lock:
            now = time.monotonic()
            delay = max(0, self.next_request - now)
            self.next_request = max(now, self.next_request) + 0.2
        if delay:
            await asyncio.sleep(delay)


def _route(app, binding):
    config = app['config'].get('backends', {}).get(binding['backend_alias'])
    if (config is None or config.get('type') != 'realtime'
            or config['url'].rstrip('/') != binding['backend_url'].rstrip('/')
            or (config.get('model') or binding['backend_alias']) != binding['model_id']):
        raise LookupError('Historical backend route is unavailable')
    return config


async def _request(app, binding, method, path, params=None):
    config = _route(app, binding)
    pacer = app.setdefault('accounting_pacer', RequestPacer())
    await pacer.take()
    try:
        async with asyncio.timeout(5):
            async with app['reconcile_client'].request(method,
                    yarl.URL(binding['backend_url']) / path, params=params,
                    headers={'Authorization': 'Bearer ' + config['token']},
                    ssl=config.get('verify_ssl', True), allow_redirects=False) as response:
                if response.status != 200:
                    raise BackendUnavailable('Private receipt endpoint unavailable')
                data = await read_limited(response, 1024 * 1024 if method == 'GET' else 65536)
                return json.loads(data)
    except (aiohttp.ClientError, TimeoutError):
        raise BackendUnavailable('Private receipt endpoint unavailable') from None


def _integer(value):
    return type(value) is int and 0 <= value < 2**63


def validate_page(page, binding, after):
    if (type(page) is not dict or page.get('session_id') != binding['session_id']
            or page.get('protocol_version') != 'receipt-v1'
            or type(page.get('backend_instance_id')) is not str
            or not ID.fullmatch(page['backend_instance_id'])
            or binding['backend_instance_id'] not in (None, page['backend_instance_id'])):
        raise ValueError('Invalid receipt page identity')
    if (any(not _integer(page.get(key)) for key in ('next_cursor', 'last_cursor', 'pruned_cursor', 'pending'))
            or type(page.get('has_more')) is not bool or type(page.get('retention_gap')) is not bool
            or page.get('state') not in ('active', 'finished', 'interrupted')
            or type(page.get('items')) is not list or len(page['items']) > 100
            or page['pruned_cursor'] > page['last_cursor']):
        raise ValueError('Invalid receipt page metadata')
    if page['retention_gap'] or after < page['pruned_cursor']:
        raise RetentionGap('Missing retained receipt history')
    effective = binding | {'backend_instance_id': page['backend_instance_id']}
    previous = after
    for event in page['items']:
        parsed = parse_receipt(event, effective)
        cursor = parsed['receipt']['cursor']
        if cursor <= previous:
            raise ValueError('Receipt page made no progress')
        previous = cursor
    if (page['next_cursor'] != previous or previous > page['last_cursor']
            or (page['has_more'] and (not page['items'] or previous >= page['last_cursor']))
            or (not page['has_more'] and previous != page['last_cursor'])):
        raise ValueError('Invalid receipt page cursor')
    return effective


async def ack_receipt(app, db, binding, receipt):
    result = await _request(app, binding, 'POST', 'internal/v1/receipts/' + receipt['event_id'] + '/ack')
    if type(result) is not dict or result.get('status') != 'acknowledged':
        raise ValueError('Invalid receipt acknowledgment')
    await accounting.mark_acked(db, receipt['event_id'])


async def _session(app, db, binding):
    if binding['status'] == 'bound':
        # No frame can be forwarded until activation is durable. CAS must not
        # overwrite a live worker that activated after this snapshot was read.
        await accounting.set_session_status(db, binding['session_id'], 'settled', expected_status='bound')
        return
    # Resolve Mongo's partial write before requiring a healthy STT endpoint.
    # This list starts with the persistent active-event fence, not cursor order.
    for receipt in await accounting.incomplete_receipts(db, binding['session_id']):
        await accounting.record_receipt(db, binding, receipt)
    after = binding['reconcile_cursor']
    for _ in range(4):
        page = await _request(app, binding, 'GET',
            'internal/v1/sessions/' + binding['session_id'] + '/receipts', {'after': after, 'limit': 100})
        effective = validate_page(page, binding, after)
        if binding['backend_instance_id'] is None:
            await accounting.attach_instance(db, binding['session_id'], effective['backend_instance_id'])
        binding = binding | {'backend_instance_id': effective['backend_instance_id']}
        for receipt in page['items']:
            await accounting.record_receipt(db, binding, receipt)
        # Includes receipts billed live or before a prior lost ACK response.
        for receipt in await accounting.unacked_receipts(db, binding['session_id']):
            await ack_receipt(app, db, binding, receipt)
        after = page['next_cursor']
        await accounting.advance_cursor(db, binding['session_id'], after)
        if not page['has_more']:
            terminal = page['state'] in ('finished', 'interrupted') and page['pending'] == 0
            await accounting.set_session_status(db, binding['session_id'], 'settled' if terminal else 'pending')
            return
    # Page budget exhausted: cursor is durable, next cycle continues fairly.
    await accounting.set_session_status(db, binding['session_id'], 'pending')


async def reconcile_once(app, limit=100):
    if type(limit) is not int or not 1 <= limit <= 100:
        raise ValueError('Invalid reconciliation limit')
    db = await get_db(app['config']['db']['uri'])
    try:
        bindings = await accounting.pending_sessions(db, limit, app.get('accounting_after'))
        if not bindings and app.get('accounting_after'):
            app['accounting_after'] = None
            bindings = await accounting.pending_sessions(db, limit)
        visited = 0
        async with asyncio.timeout(30):
            for binding in bindings:
                reason, status = None, 'retry'
                try:
                    await _session(app, db, binding)
                except RetentionGap:
                    reason, status = 'retention_gap', 'blocked'
                except ValueError:
                    reason, status = 'contract', 'blocked'
                except LookupError:
                    reason = 'route_missing'
                except BackendUnavailable:
                    reason = 'backend_unavailable'
                except accounting.AccountingUnavailable:
                    reason = 'storage_unavailable'
                if reason is not None:
                    app['accounting_healthy'] = False
                    await accounting.set_session_status(db, binding['session_id'], status, reason)
                app['accounting_after'] = binding['session_id']
                visited += 1
        app['accounting_healthy'] = await accounting.healthy(db)
        return visited
    finally:
        await db.close()
