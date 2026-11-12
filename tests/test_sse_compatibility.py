"""Protocol-valid partials must stream while financial completion stays gated."""
import asyncio
import json
from unittest.mock import patch

from aiohttp import web

from llmproxy import billing
from . import mockbackend
from .test_proxy import LLMProxyAppTestCase


def event(choices, usage=None):
    return b'data: ' + json.dumps({'choices': choices, 'usage': usage}).encode() + b'\n\n'


def delta(index=0):
    return {'index': index, 'delta': {'content': 'synthetic-token'}, 'finish_reason': None}


class TestChatNullableUsage(LLMProxyAppTestCase):
    AUTH = {'Authorization': 'Bearer mytoken'}

    async def request(self, choices=1):
        return await self.client.post('/v1/chat/completions', headers=self.AUTH,
            json={'model': 'mymodel', 'stream': True, 'n': choices,
                'messages': [{'role': 'user', 'content': 'synthetic'}]})

    async def test_more_than64_nullable_usage_partials_are_not_terminal_tail(self):
        async def upstream(request, *args):
            response = web.StreamResponse(headers={'Content-Type': 'text/event-stream'})
            await response.prepare(request)
            for _ in range(65):
                await response.write(event([delta()]))
            await response.write(event([], {'prompt_tokens': 1, 'completion_tokens': 65}))
            await response.write(b'data: [DONE]\n\n')
            return response
        with patch.object(mockbackend, '_chat_stream', upstream):
            response = await self.request()
            try:
                self.assertEqual(response.status, 200)
                body = await response.read()
            finally:
                response.close()
        self.assertEqual(body.count(b'synthetic-token'), 65)
        self.assertIn(b'[DONE]', body)
        self.assertEqual([row['quantity'] for row in await self.get_events()], [1, 65])

    async def test_nullable_usage_partial_arrives_before_backend_can_finish(self):
        release = asyncio.Event()

        async def upstream(request, *args):
            response = web.StreamResponse(headers={'Content-Type': 'text/event-stream'})
            await response.prepare(request)
            await response.write(event([delta()]))
            await release.wait()
            await response.write(event([], {'prompt_tokens': 1, 'completion_tokens': 1}))
            await response.write(b'data: [DONE]\n\n')
            return response

        response = None
        try:
            with patch.object(mockbackend, '_chat_stream', upstream):
                response = await self.request()
                chunk = await asyncio.wait_for(response.content.readuntil(b'\n\n'), 3)
                self.assertIn(b'synthetic-token', chunk)
                self.assertNotIn(b'[DONE]', chunk)
                self.assertEqual(await self.get_events(), [])
                release.set()
                self.assertIn(b'[DONE]', await response.read())
            self.assertEqual(len(await self.get_events()), 2)
        finally:
            release.set()
            if response is not None:
                response.close()

    async def test_finished_choice_does_not_buffer_other_choices_partials(self):
        started, settle = asyncio.Event(), asyncio.Event()
        original = billing.record

        async def paused(*args, **kwargs):
            started.set()
            await settle.wait()
            return await original(*args, **kwargs)

        async def upstream(request, *args):
            response = web.StreamResponse(headers={'Content-Type': 'text/event-stream'})
            await response.prepare(request)
            await response.write(event([{'index': 0, 'delta': {}, 'finish_reason': 'stop'}]))
            for _ in range(65):
                await response.write(event([delta(1)]))
            await response.write(event([{'index': 1, 'delta': {}, 'finish_reason': 'stop'}]))
            await response.write(event([], {'prompt_tokens': 2, 'completion_tokens': 65}))
            await response.write(b'data: [DONE]\n\n')
            return response

        response, reader, chunks = None, None, []

        async def collect():
            async for chunk in response.content.iter_any():
                chunks.append(chunk)

        try:
            with patch.object(mockbackend, '_chat_stream', upstream), patch.object(billing, 'record', paused):
                response = await self.request(choices=2)
                reader = asyncio.create_task(collect())
                await asyncio.wait_for(started.wait(), 3)
                done, _ = await asyncio.wait({reader}, timeout=0.1)
                self.assertFalse(done, 'Completion EOF escaped before settlement')
                body = b''.join(chunks)
                self.assertEqual(body.count(b'synthetic-token'), 65)
                self.assertNotIn(b'"finish_reason": "stop"', body)
                self.assertNotIn(b'[DONE]', body)
                self.assertEqual(await self.get_events(), [])
                settle.set()
                await asyncio.wait_for(reader, 3)
            body = b''.join(chunks)
            self.assertEqual(body.count(b'"finish_reason": "stop"'), 2)
            self.assertIn(b'[DONE]', body)
            self.assertEqual([row['quantity'] for row in await self.get_events()], [2, 65])
        finally:
            settle.set()
            if response is not None:
                response.close()
            if reader is not None:
                await asyncio.gather(reader, return_exceptions=True)
