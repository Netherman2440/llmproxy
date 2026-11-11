"""Actual HTTP/SQLite and SIGKILL must retain usage obligations, not free success."""
import asyncio
import copy
import multiprocessing
from unittest.mock import patch

import aiohttp
import aiohttp.web

from llmproxy import attempts
from llmproxy.db import SqliteDatabase
from . import mockbackend
from .test_proxy import LLMProxyAppTestCase
from .test_shared_admission import _peer_process


class TestHttpAttemptRuntime(LLMProxyAppTestCase):
    AUTH = {'Authorization': 'Bearer mytoken'}

    async def journal(self):
        db = await SqliteDatabase.create('sqlite://' + self.db_path)
        return db

    async def test_all_three_frontends_settle_durable_attempts_before_success(self):
        for path in ('/v1/chat/completions', '/v1/messages', '/v1/responses'):
            response = await self.client.post(path, headers=self.AUTH,
                json={'model': 'mymodel', 'stream': True, 'messages': [{'role': 'user', 'content': 'hello'}]})
            self.assertEqual(response.status, 200)
            await response.read()
        db = await self.journal()
        try:
            rows = await db.db.execute_fetchall('SELECT state FROM http_attempt')
            self.assertEqual([row['state'] for row in rows], ['billed'] * 3)
        finally:
            await db.close()
        self.assertEqual(len(await self.get_events()), 6)

    async def test_confirmed_finance_failure_recovers_without_browser_or_second_backend_call(self):
        with patch.object(SqliteDatabase, 'attempt_settle', side_effect=RuntimeError('injected finance fault')):
            response = await self.client.post('/v1/chat/completions', headers=self.AUTH,
                json={'model': 'mymodel', 'stream': True, 'messages': [{'role': 'user', 'content': 'hello'}]})
            try:
                with self.assertRaises(aiohttp.ClientPayloadError):
                    await response.read()
            finally:
                response.close()
        self.assertEqual(await self.get_events(), [])
        db = await self.journal()
        try:
            [stored] = await attempts.pending(db)
            self.assertEqual(stored['state'], 'confirmed')
        finally:
            await db.close()
        self.assertEqual(await attempts.recover_once(self.app), 1)
        self.assertEqual(len(await self.get_events()), 2)
        self.assertEqual(await attempts.recover_once(self.app), 0)

    async def test_malformed_usage_is_unresolved_without_logging_backend_payload(self):
        marker = 'SYNTHETIC_PRIVATE_USAGE_DOCUMENT'
        async def malformed(request, message, mode):
            response = aiohttp.web.StreamResponse(headers={'Content-Type': 'text/event-stream'})
            await response.prepare(request)
            await response.write(('data: {"private":"' + marker + '",BROKEN}\n\n').encode())
            await response.write(b'data: [DONE]\n\n')
            return response
        with patch.object(mockbackend, '_chat_stream', malformed), self.assertLogs(level='ERROR') as captured:
            response = await self.client.post('/v1/chat/completions', headers=self.AUTH,
                json={'model': 'mymodel', 'stream': True, 'messages': [{'role': 'user', 'content': 'hello'}]})
            try:
                with self.assertRaises(aiohttp.ClientPayloadError):
                    await response.read()
            finally:
                response.close()
        self.assertNotIn(marker, '\n'.join(captured.output))
        db = await self.journal()
        try:
            [intent] = await attempts.pending(db)
            self.assertEqual(intent['state'], 'unknown')
            self.assertIsNone(intent['resources'])
        finally:
            await db.close()
        self.assertEqual(await self.get_events(), [])

    async def test_upstream_400_is_rejected_not_unexplained_financial_debt(self):
        response = await self.client.post('/v1/chat/completions', headers=self.AUTH,
            json={'model': 'mymodel', 'messages': [], '_trigger_error': 400})
        await response.read()
        self.assertGreaterEqual(response.status, 400)
        db = await self.journal()
        try:
            rows = await db.db.execute_fetchall('SELECT state FROM http_attempt')
            self.assertEqual([row['state'] for row in rows], ['rejected'])
        finally:
            await db.close()
        self.assertEqual(await self.get_events(), [])

    async def test_actual_sigkill_after_live_partial_retains_unresolved_attempt_without_guessed_bill(self):
        release = asyncio.Event()
        async def partial_only(request, message, mode):
            response = aiohttp.web.StreamResponse(headers={'Content-Type': 'text/event-stream'})
            await response.prepare(request)
            await response.write(b'data: {"choices":[{"delta":{"content":"partial"}}]}\n\n')
            await release.wait()
            return response
        ctx = multiprocessing.get_context('spawn')
        ready, stop = ctx.Queue(), ctx.Event()
        process = ctx.Process(target=_peer_process, args=(copy.deepcopy(self.app['config']), ready, stop))
        response = None
        try:
            process.start()
            port = await asyncio.to_thread(ready.get, True, 5)
            self.assertIsInstance(port, int)
            with patch.object(mockbackend, '_chat_stream', partial_only):
                async with aiohttp.ClientSession(base_url=f'http://127.0.0.1:{port}') as client:
                    response = await client.post('/v1/chat/completions', headers=self.AUTH,
                        json={'model': 'mymodel', 'stream': True, 'messages': [{'role': 'user', 'content': 'hello'}]})
                    self.assertIn(b'partial', await asyncio.wait_for(response.content.readuntil(b'\n\n'), 3))
                    db = await self.journal()
                    try:
                        [stored] = await attempts.pending(db)
                        self.assertEqual(stored['state'], 'prepared')
                    finally:
                        await db.close()
                    process.kill()
                    await asyncio.to_thread(process.join, 3)
                    self.assertEqual(process.exitcode, -9)
                    response.close()
            self.assertEqual(await attempts.recover_once(self.app), 0)
            self.assertFalse(self.app['http_attempt_recovery_healthy'])
            self.assertEqual(await self.get_events(), [])
        finally:
            release.set()
            if response is not None:
                response.close()
            if process.is_alive():
                process.kill()
                await asyncio.to_thread(process.join, 3)
            ready.close()
