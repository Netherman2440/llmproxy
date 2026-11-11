"""Receipt-v1 WS lifecycle. Legacy transparent forwarding lives separately."""
import asyncio
import datetime
import json
import time
import uuid

import aiohttp
from aiohttp import web
import yarl

from . import accounting, auth, metrics, provenance, reconciliation, streaming
from .db import get_db
from .usage import COMPLETED_EVENT, ID


async def setup(app):
    db = await get_db(app['config']['db']['uri'])
    try:
        app['accounting_healthy'] = await accounting.healthy(db)
    finally:
        await db.close()
    capacity = app['config']['realtime'].get('max_sessions', 4)
    app['realtime_capacity'] = {'active': 0}
    app['ws_client'] = aiohttp.ClientSession(connector=aiohttp.TCPConnector(limit=capacity),
        timeout=aiohttp.ClientTimeout(total=None, connect=5, sock_read=None))
    app['reconcile_client'] = aiohttp.ClientSession(connector=aiohttp.TCPConnector(limit=4),
        timeout=aiohttp.ClientTimeout(total=5))

    async def loop():
        while True:
            try:
                await reconciliation.reconcile_once(app)
            except Exception as error:
                app['accounting_healthy'] = False
                app.logger.error('Receipt recovery failed: category=%s', type(error).__name__)
            await asyncio.sleep(5)

    async def start(app):
        # First cycle starts on startup, not on the next browser connection.
        app['reconciliation_task'] = asyncio.create_task(loop())

    async def stop(app):
        task = app.get('reconciliation_task')
        if task is not None:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)

    async def close(app):
        await app['ws_client'].close()
        await app['reconcile_client'].close()

    app.on_startup.append(start)
    app.on_shutdown.append(stop)
    app.on_cleanup.append(close)


async def _fresh_user(request):
    clone = request.clone()
    clone.pop('db', None)
    try:
        return await auth.require_auth(clone, fresh=True)
    finally:
        db = clone.pop('db', None)
        if db is not None:
            await db.close()


async def _watch(request, user, binding, started):
    maximum = request.app['config']['realtime'].get('max_session_seconds', 3600)
    next_check = time.monotonic()
    while True:
        if not auth.valid_expiry(user.get('expires'), datetime.datetime.now(datetime.UTC)):
            return 1008
        if time.monotonic() - started >= maximum:
            return 1000
        if time.monotonic() >= next_check:
            try:
                async with asyncio.timeout(0.5):
                    fresh = await _fresh_user(request)
                if accounting.binding_hash(binding | {'owner': fresh}) != accounting.binding_hash(binding):
                    return 1008
            except web.HTTPUnauthorized:
                return 1008
            except (Exception, web.GracefulExit):
                # Fail closed on an unavailable authorization database.
                return 1011
            next_check = time.monotonic() + 1
        await asyncio.sleep(0.2)


async def _connect(app, config, name, sid):
    async with asyncio.timeout(5):
        return await app['ws_client'].ws_connect(
            (yarl.URL(config['url']) / 'v1/realtime').with_query({'model': config.get('model') or name}),
            headers={'Authorization': 'Bearer ' + config['token'], 'X-STT-Session-ID': sid},
            heartbeat=config.get('heartbeat', 30), ssl=config.get('verify_ssl', True))


async def _up(client, backend, user, write_timeout):
    async for message in client:
        if not auth.valid_expiry(user.get('expires'), datetime.datetime.now(datetime.UTC)):
            return 1008
        async with asyncio.timeout(write_timeout):
            if message.type == aiohttp.WSMsgType.TEXT:
                await backend.send_str(message.data)
            elif message.type == aiohttp.WSMsgType.BINARY:
                await backend.send_bytes(message.data)
    return 1000


async def _send_client(request, client, data):
    if client.closed:
        return
    try:
        async with asyncio.timeout(request.app['config'].get('write_timeout', 10)):
            await client.send_str(data)
    except (ConnectionError, aiohttp.ClientError, TimeoutError):
        # Continue consuming durable receipts; client delivery is not billing.
        if request.transport is not None:
            request.transport.abort()


async def _down(request, db, binding, backend, client):
    async for message in backend:
        if message.type != aiohttp.WSMsgType.TEXT:
            if message.type == aiohttp.WSMsgType.BINARY:
                raise ValueError('Unexpected binary receipt-v1 response')
            continue
        event = json.loads(message.data)
        if type(event) is not dict:
            raise ValueError('Invalid receipt-v1 event')
        completed = event.get('type') == COMPLETED_EVENT
        if completed:
            first_bill = await accounting.record_receipt(db, binding, event)
            if first_bill:
                try:
                    metrics.AUDIO_SECONDS_TOTAL.labels(binding['backend_alias']).inc(
                        event['receipt']['sample_count'] / 16000)
                except Exception:
                    request.app.logger.error('Receipt telemetry failed after durable billing')
        terminal = event.get('type') == 'session.finished'
        if terminal and (event.get('session_id') != binding['session_id']
                or type(event.get('last_cursor')) is not int or event['last_cursor'] < 0
                or event.get('state') not in ('finished', 'interrupted')
                or type(event.get('pending')) is not int or event['pending'] != 0):
            raise ValueError('Invalid terminal session event')
        await _send_client(request, client, message.data)
        if completed:
            try:
                await reconciliation.ack_receipt(request.app, db, binding, event['receipt'])
            except (reconciliation.BackendUnavailable, LookupError):
                # Persistent ACK outbox is retried without another bill.
                request.app.logger.warning('Receipt ACK deferred')
        if terminal:
            return


async def _close(socket, code=1000):
    if socket is not None:
        try:
            async with asyncio.timeout(2):
                await socket.close(code=code)
        except (ConnectionError, aiohttp.ClientError, TimeoutError):
            pass


async def run(request, user, name, config):
    app = request.app
    capacity = app['realtime_capacity']
    if capacity['active'] >= app['config']['realtime'].get('max_sessions', 4):
        raise web.HTTPTooManyRequests()
    capacity['active'] += 1
    client = backend = down = None
    tasks = []
    failure, code = None, 1000
    binding, db = None, None
    started = time.monotonic()
    try:
        db = await get_db(app['config']['db']['uri'], request)
        if not await accounting.healthy(db):
            raise web.HTTPServiceUnavailable()
        binding = accounting.clean_binding({'session_id': uuid.uuid4().hex,
            'request_id': str(request['request_id']), 'owner': user,
            'backend_alias': name, 'backend_url': config['url'], 'model_id': config.get('model') or name,
            'device': config['device'], 'backend_instance_id': None,
            'created_at': datetime.datetime.now(datetime.UTC).isoformat()})
        await accounting.bind_session(db, binding)
        try:
            backend = await _connect(app, config, name, binding['session_id'])
            async with asyncio.timeout(5):
                message = await backend.receive()
            created = json.loads(message.data) if message.type == aiohttp.WSMsgType.TEXT else None
            if (type(created) is not dict or created.get('type') != 'session.created'
                    or created.get('protocol_version') != 'receipt-v1'
                    or created.get('session_id') != binding['session_id']
                    or type(created.get('backend_instance_id')) is not str
                    or not ID.fullmatch(created['backend_instance_id'])):
                raise ValueError('Backend does not implement receipt-v1')
        except ValueError:
            failure = 'contract'
            raise web.HTTPBadGateway() from None
        except (aiohttp.ClientError, TimeoutError):
            # No client frame was forwarded; retain bound, not a fabricated
            # debt/contract incident for a backend that was merely offline.
            raise web.HTTPBadGateway() from None
        await accounting.attach_instance(db, binding['session_id'], created['backend_instance_id'])
        binding['backend_instance_id'] = created['backend_instance_id']
        await accounting.set_session_status(db, binding['session_id'], 'pending')
        client = web.WebSocketResponse()
        streaming.set_prepared_headers(request, client)
        client.headers.update(provenance.headers(app['config']))
        await client.prepare(request)
        await _send_client(request, client, message.data)
        up = asyncio.create_task(_up(client, backend, user, app['config'].get('write_timeout', 10)))
        down = asyncio.create_task(_down(request, db, binding, backend, client))
        watch = asyncio.create_task(_watch(request, user, binding, started))
        tasks = [up, down, watch]
        done, _ = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
        if watch in done:
            code = watch.result()
        elif up in done:
            code = up.result()
        if down in done:
            down.result()
    except accounting.AccountingUnavailable:
        failure, code = 'storage_unavailable', 1011
        if client is None:
            raise web.HTTPServiceUnavailable() from None
    except ValueError:
        failure, code = 'contract', 1011
        if client is None:
            raise web.HTTPBadGateway() from None
    except (ConnectionError, aiohttp.ClientError, TimeoutError):
        code = 1011
        if client is None:
            raise web.HTTPBadGateway() from None
    finally:
        for task in tasks:
            if task is not down:
                task.cancel()
        await asyncio.gather(*(task for task in tasks if task is not down), return_exceptions=True)
        if down is not None and not down.done():
            try:
                async with asyncio.timeout(app['config'].get('write_timeout', 10)):
                    await backend.send_json({'type': 'session.finish'})
                await asyncio.wait_for(asyncio.shield(down), app['config']['realtime'].get('drain_timeout', 30))
            except (ConnectionError, aiohttp.ClientError, TimeoutError):
                pass
            except (ValueError, accounting.AccountingUnavailable) as error:
                failure = 'contract' if isinstance(error, ValueError) else 'storage_unavailable'
                code = 1011
        if down is not None:
            if not down.done():
                down.cancel()
            outcomes = await asyncio.gather(down, return_exceptions=True)
            if isinstance(outcomes[0], Exception):
                failure = 'contract' if isinstance(outcomes[0], ValueError) else 'storage_unavailable'
                code = 1011
        if failure is not None and db is not None and binding is not None:
            app['accounting_healthy'] = False
            try:
                await accounting.set_session_status(db, binding['session_id'],
                    'blocked' if failure == 'contract' else 'retry', failure)
            except accounting.AccountingUnavailable:
                app.logger.error('Cannot persist receipt session failure')
        await _close(backend)
        await _close(client, code)
        capacity['active'] -= 1
    return client
