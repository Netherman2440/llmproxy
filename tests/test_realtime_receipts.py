import asyncio
import datetime
import hashlib
import os
import tempfile
import unittest
from unittest.mock import patch

import aiohttp
from aiohttp.test_utils import TestServer

from llmproxy import auth, config, metrics, receipt_realtime, reconciliation
from llmproxy.app import create_app
from llmproxy.db import SqliteDatabase
from .accounting_fixtures import MODEL
from .receipt_backend import ReceiptBackend
from .test_realtime import _NoCancellationTestServer


class TestReceiptProfileConfig(unittest.TestCase):
    def test_receipt_profile_rejects_unknown_mode_and_invalid_capacity(self):
        for value in (None, True, [], 'implicit'):
            with self.subTest(mode=value), self.assertRaises(config.ConfigError):
                config.validate({'realtime': {'mode': value}})
        for key, maximum in (('max_sessions', 64), ('max_session_seconds', 3600), ('drain_timeout', 30)):
            for value in (True, 0, -1, '4', float('nan'), maximum + 1):
                with self.subTest(key=key, value=value), self.assertRaises(config.ConfigError):
                    config.validate({'realtime': {'mode': 'receipt-v1', key: value}})
        for value in (None, False, []):
            with self.subTest(table=value), self.assertRaises(config.ConfigError):
                config.validate({'realtime': value})


class TestReceiptRealtime(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        auth.flush_cache()
        fd, self.path = tempfile.mkstemp()
        os.close(fd)
        self.addCleanup(os.unlink, self.path)
        self.db = await SqliteDatabase.create('sqlite://' + self.path)
        self.addAsyncCleanup(self.db.close)
        await self.db.user_create(hashlib.sha256(b'synthetic-client-auth').hexdigest())
        self.backend = ReceiptBackend(self.db)
        self.backend_server = TestServer(self.backend.app)
        await self.backend_server.start_server()
        self.addAsyncCleanup(self.backend_server.close)
        self.app = await create_app({'db': {'uri': 'sqlite://' + self.path},
            'timeout_connect': 2, 'timeout_read': 2, 'auth_cache_ttl': 60,
            'realtime': {'mode': 'receipt-v1', 'max_sessions': 1},
            'backends': {'parakeet': {'type': 'realtime',
                'url': str(self.backend_server.make_url('')).rstrip('/'),
                'token': 'synthetic-service-auth', 'model': MODEL, 'device': 'a5000'}}})
        self.server = _NoCancellationTestServer(self.app)
        await self.server.start_server()
        self.addAsyncCleanup(self.server.close)
        self.client = aiohttp.ClientSession()
        self.addAsyncCleanup(self.client.close)
        self.headers = {'Authorization': 'Bearer synthetic-client-auth', 'X-STT-Session-ID': 'f' * 32}

    async def connect(self):
        return await self.client.ws_connect(self.server.make_url('/v1/realtime?model=parakeet'),
            headers=self.headers)

    async def count(self):
        row = await (await self.db.db.execute('SELECT COUNT(*) AS n FROM event_oneoff')).fetchone()
        return row['n']

    async def wait_for_count(self, count):
        async with asyncio.timeout(8):
            while await self.count() != count:
                await asyncio.sleep(0.02)

    async def test_binding_precedes_backend_connect_and_final_precedes_ack_only_after_bill(self):
        ws = await self.connect()
        created = await ws.receive_json()
        self.assertTrue(self.backend.bound_before_connect)
        self.assertNotEqual(created['session_id'], 'f' * 32)
        self.assertIn('X-Request-ID', ws._response.headers)
        await ws.send_json({'type': 'input_audio_buffer.commit'})
        final = await ws.receive_json()
        self.assertIn('receipt', final)
        self.assertEqual(await self.count(), 1)
        await ws.close()
        self.assertFalse(self.backend.ack_before_bill)

    async def test_wrong_protocol_is_rejected_before_client_upgrade(self):
        self.backend.protocol = 'legacy'
        with self.assertRaises(aiohttp.WSServerHandshakeError) as caught:
            await self.connect()
        self.assertEqual(caught.exception.status, 502)
        self.assertEqual(await self.count(), 0)

    async def test_three_completions_delayed_beyond_two_seconds_survive_client_exit(self):
        self.backend.defer, self.backend.delay = True, 2.3
        ws = await self.connect()
        await ws.receive_json()
        for _ in range(3):
            await ws.send_json({'type': 'input_audio_buffer.commit'})
        await ws.close()
        await self.wait_for_count(3)
        self.assertFalse(self.backend.ack_before_bill)
        self.assertEqual(len(set(item['event_id'] for item in self.backend.receipts)), 3)

    async def test_telemetry_failure_does_not_drop_final_or_repeat_financial_write(self):
        ws = await self.connect()
        await ws.receive_json()
        with patch.object(metrics.AUDIO_SECONDS_TOTAL, 'labels', side_effect=RuntimeError('injected telemetry')):
            await ws.send_json({'type': 'input_audio_buffer.commit'})
            final = await ws.receive_json()
        self.assertIn('receipt', final)
        self.assertEqual(await self.count(), 1)
        await ws.close()

    async def test_expiry_stops_new_audio_and_still_bills_pending_completion(self):
        expires = datetime.datetime.now(datetime.UTC) + datetime.timedelta(seconds=0.7)
        await self.db.db.execute('UPDATE api_key SET expires=?', (expires,))
        await self.db.db.commit()
        self.backend.defer = True
        ws = await self.connect()
        await ws.receive_json()
        await ws.send_json({'type': 'input_audio_buffer.commit'})
        async with asyncio.timeout(2):
            messages = []
            async for message in ws:
                messages.append(message)
        await self.wait_for_count(1)
        self.assertEqual(ws.close_code, 1008)
        self.assertFalse(self.backend.ack_before_bill)

    async def test_revoked_cached_key_stops_session_without_losing_historical_owner(self):
        self.backend.defer = True
        ws = await self.connect()
        await ws.receive_json()
        await ws.send_json({'type': 'input_audio_buffer.commit'})
        await self.db.db.execute('DELETE FROM api_key')
        await self.db.db.commit()
        async with asyncio.timeout(5):
            async for _ in ws:
                pass
        await self.wait_for_count(1)
        self.assertEqual(ws.close_code, 1008)

    async def test_failed_financial_write_never_forwards_final_and_recovery_bills_it_once(self):
        await self.db.db.execute('''CREATE TRIGGER fail_finance BEFORE INSERT ON event_oneoff
            BEGIN SELECT RAISE(ABORT, 'injected storage failure'); END''')
        await self.db.db.commit()
        ws = await self.connect()
        await ws.receive_json()
        await ws.send_json({'type': 'input_audio_buffer.commit'})
        async with asyncio.timeout(3):
            message = await ws.receive()
        self.assertEqual(message.type, aiohttp.WSMsgType.CLOSE)
        self.assertEqual(await self.count(), 0)
        self.assertEqual(self.backend.acks, [])
        await self.db.db.execute('DROP TRIGGER fail_finance')
        await self.db.db.commit()
        await reconciliation.reconcile_once(self.app)
        self.assertEqual(await self.count(), 1)
        self.assertFalse(self.backend.ack_before_bill)

    async def test_malformed_live_usage_is_not_transparently_forwarded(self):
        self.backend.malformed = True
        ws = await self.connect()
        await ws.receive_json()
        await ws.send_json({'type': 'input_audio_buffer.commit'})
        async with asyncio.timeout(3):
            message = await ws.receive()
        self.assertEqual(message.type, aiohttp.WSMsgType.CLOSE)
        self.assertEqual(await self.count(), 0)
        self.assertEqual(self.backend.acks, [])
        # The separately fetched durable receipt is valid, unlike the outer
        # live frame. Its legitimate obligation must still be recovered.
        await reconciliation.reconcile_once(self.app)
        self.assertEqual(await self.count(), 1)

    async def test_malformed_terminal_control_is_not_reported_as_a_successful_finish(self):
        self.backend.terminal_overrides = {'last_cursor': True}
        ws = await self.connect()
        await ws.receive_json()
        await ws.send_json({'type': 'session.finish'})
        async with asyncio.timeout(3):
            message = await ws.receive()
        self.assertEqual(message.type, aiohttp.WSMsgType.CLOSE)
        self.assertEqual(ws.close_code, 1011)

    async def test_cancel_during_connect_releases_capacity_and_leaves_no_audio_debt(self):
        with patch.object(receipt_realtime, '_connect', side_effect=asyncio.CancelledError()):
            with self.assertRaises(aiohttp.ClientError):
                async with asyncio.timeout(2):
                    await self.connect()
        self.assertEqual(self.app['realtime_capacity']['active'], 0)
        await reconciliation.reconcile_once(self.app)
        self.assertEqual(await self.count(), 0)
        rows = await (await self.db.db.execute("SELECT status FROM accounting_session")).fetchall()
        self.assertTrue(rows)
        self.assertTrue(all(row['status'] == 'settled' for row in rows))

    async def test_temporary_preconnect_network_failure_does_not_create_permanent_debt(self):
        with patch.object(receipt_realtime, '_connect', side_effect=aiohttp.ClientConnectionError()):
            with self.assertRaises(aiohttp.WSServerHandshakeError) as caught:
                await self.connect()
        self.assertEqual(caught.exception.status, 502)
        await reconciliation.reconcile_once(self.app)
        rows = await (await self.db.db.execute('SELECT status FROM accounting_session')).fetchall()
        self.assertTrue(all(row['status'] == 'settled' for row in rows))
        ws = await self.connect()
        await ws.receive_json()
        await ws.close()

    async def test_external_cancel_after_upgrade_drains_already_accepted_work(self):
        self.backend.defer, self.backend.delay = True, 0.2
        handler = None
        original = aiohttp.web.WebSocketResponse.prepare
        async def observe_prepare(socket, request):
            nonlocal handler
            result = await original(socket, request)
            if request.app is self.app:
                handler = asyncio.current_task()
            return result
        with patch.object(aiohttp.web.WebSocketResponse, 'prepare', observe_prepare):
            ws = await self.connect()
            await ws.receive_json()
            await ws.send_json({'type': 'input_audio_buffer.commit'})
            async with asyncio.timeout(2):
                while not any(value['pending'] == 1 for value in self.backend.sessions.values()):
                    await asyncio.sleep(0.01)
            self.assertIsNotNone(handler)
            handler.cancel()
            await self.wait_for_count(1)
            async with asyncio.timeout(3):
                while self.app['realtime_capacity']['active']:
                    await asyncio.sleep(0.01)
        self.assertFalse(self.backend.ack_before_bill)
        await ws.close()

    async def test_saturated_ws_capacity_does_not_queue_http_or_a_second_session(self):
        ws = await self.connect()
        await ws.receive_json()
        try:
            async with asyncio.timeout(1):
                with self.assertRaises(aiohttp.WSServerHandshakeError) as caught:
                    await self.connect()
                self.assertEqual(caught.exception.status, 429)
                async with self.client.get(self.server.make_url('/v1/models'), headers=self.headers) as response:
                    self.assertEqual(response.status, 200)
        finally:
            await ws.close()
