from unittest.mock import patch

from aiohttp import web

from . import mockbackend
from .test_proxy import LLMProxyAppTestCase


class TestChatSseLineEndings(LLMProxyAppTestCase):
    async def check_stream(self, ending, *, trailing=False):
        partial = b'data: {"choices":[{"delta":{"content":"synthetic"}}]}' + ending * 2
        usage = b'data: {"choices":[],"usage":{"prompt_tokens":1,"completion_tokens":2}}' + ending * 2
        done = b'data: [DONE]' + ending * 2
        tail = b': keepalive' + ending * 2 if trailing else b''

        async def upstream(request, *args):
            response = web.StreamResponse(headers={'Content-Type': 'text/event-stream'})
            await response.prepare(request)
            await response.write(partial + usage + done + tail)
            return response

        with patch.object(mockbackend, '_chat_stream', upstream):
            response = await self.client.post('/v1/chat/completions', headers={'Authorization': 'Bearer mytoken'},
                json={'model': 'mymodel', 'stream': True, 'messages': [{'role': 'user', 'content': 'synthetic'}]})
            try:
                self.assertEqual(response.status, 200)
                result = await response.read()
            finally:
                response.close()
        for frame in (partial, usage, done):
            self.assertIn(frame, result, 'Framing normalized or lost original event bytes')
        if trailing:
            self.assertIn(tail, result)
        self.assertEqual([row['quantity'] for row in await self.get_events()], [1, 2])

    async def test_standard_crlf_stream_is_billed_and_completed(self):
        await self.check_stream(b'\r\n')

    async def test_standard_cr_only_stream_is_billed_and_completed(self):
        await self.check_stream(b'\r')

    async def test_trailing_comment_does_not_replace_authoritative_usage(self):
        await self.check_stream(b'\n', trailing=True)
