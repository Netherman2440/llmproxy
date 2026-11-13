"""A complete SSE success must not escape before durable accounting."""
import asyncio
from unittest.mock import patch

import aiohttp

from llmproxy import billing
from . import mockbackend
from .test_proxy import LLMProxyAppTestCase


class TestStreamSettlement(LLMProxyAppTestCase):
    AUTH = {'Authorization': 'Bearer mytoken'}

    async def check_barrier(self, path, terminal):
        started, release = asyncio.Event(), asyncio.Event()
        original = billing.record
        async def paused_record(*args, **kwargs):
            started.set()
            await release.wait()
            await original(*args, **kwargs)
        response = reader = None
        chunks = []
        async def collect():
            async for chunk in response.content.iter_any():
                chunks.append(chunk)
        try:
            with patch.object(billing, 'record', paused_record):
                response = await self.client.post(path, headers=self.AUTH,
                    json={'model': 'mymodel', 'stream': True, 'messages': [{'role': 'user', 'content': 'hello'}]})
                reader = asyncio.create_task(collect())
                await asyncio.wait_for(started.wait(), 3)
                done, _ = await asyncio.wait({reader}, timeout=0.1)
                self.assertFalse(done, 'HTTP EOF escaped while the financial commit was blocked')
                self.assertNotIn(terminal, b''.join(chunks), 'Terminal SSE escaped before accounting')
                self.assertEqual(await self.get_events(), [])
                release.set()
                await asyncio.wait_for(reader, 3)
            self.assertIn(terminal, b''.join(chunks))
            self.assertEqual(len(await self.get_events()), 2)
        finally:
            release.set()
            if response is not None:
                response.close()
            if reader is not None:
                await asyncio.gather(reader, return_exceptions=True)

    async def check_storage_failure(self, path, terminal):
        response = None
        chunks = []
        try:
            with patch.object(billing, 'record', side_effect=RuntimeError('injected financial failure')):
                response = await self.client.post(path, headers=self.AUTH,
                    json={'model': 'mymodel', 'stream': True, 'messages': [{'role': 'user', 'content': 'hello'}]})
                with self.assertRaises(aiohttp.ClientPayloadError):
                    async for chunk in response.content.iter_any():
                        chunks.append(chunk)
            self.assertNotIn(terminal, b''.join(chunks))
            self.assertEqual(await self.get_events(), [])
        finally:
            if response is not None:
                response.close()

    async def test_disconnect_during_settlement_preserves_already_confirmed_usage(self):
        started, release = asyncio.Event(), asyncio.Event()
        original = billing.record
        async def paused_record(*args, **kwargs):
            started.set()
            await release.wait()
            await original(*args, **kwargs)
        try:
            with patch.object(billing, 'record', paused_record):
                response = await self.client.post('/v1/responses', headers=self.AUTH,
                    json={'model': 'mymodel', 'stream': True, 'input': 'hello'})
                await asyncio.wait_for(started.wait(), 3)
                response.close()
                # Give the real transport disconnect an event-loop turn while
                # settlement is deliberately paused, not a fake write callback.
                await asyncio.sleep(0.05)
                release.set()
                self.assertEqual(len(await self.get_events(expect=2)), 2)
        finally:
            release.set()

    async def test_repeated_terminal_frames_exceeding_hold_budget_abort_unbilled(self):
        async def excessive_tail(request, *args):
            response = aiohttp.web.StreamResponse(headers={'Content-Type': 'text/event-stream'})
            await response.prepare(request)
            await response.write(b'data: {"choices":[],"usage":{"prompt_tokens":1,"completion_tokens":2}}\n\n')
            for _ in range(65):
                await response.write(b'data: [DONE]\n\n')
            await response.write_eof()
            return response
        with patch.object(mockbackend, '_chat_stream', excessive_tail):
            response = await self.client.post('/v1/chat/completions', headers=self.AUTH,
                json={'model': 'mymodel', 'stream': True, 'messages': [{'role': 'user', 'content': 'hello'}]})
            try:
                with self.assertRaises(aiohttp.ClientPayloadError):
                    await response.read()
            finally:
                response.close()
        self.assertEqual(await self.get_events(), [])

    async def test_chat_terminal_and_eof_wait_for_financial_commit(self):
        await self.check_barrier('/v1/chat/completions', b'[DONE]')

    async def test_messages_terminal_and_eof_wait_for_financial_commit(self):
        await self.check_barrier('/v1/messages', b'message_stop')

    async def test_responses_terminal_and_eof_wait_for_financial_commit(self):
        await self.check_barrier('/v1/responses', b'response.completed')

    async def test_chat_storage_failure_is_not_a_successful_stream(self):
        await self.check_storage_failure('/v1/chat/completions', b'[DONE]')

    async def test_messages_storage_failure_is_not_a_successful_stream(self):
        await self.check_storage_failure('/v1/messages', b'message_stop')

    async def test_responses_storage_failure_is_not_a_successful_stream(self):
        await self.check_storage_failure('/v1/responses', b'response.completed')
