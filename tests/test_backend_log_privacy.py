import asyncio
import contextlib
import json

from aiohttp import web
from aiohttp.test_utils import TestServer

from .test_conversion import ConversionTestCase

MARKER = 'SYNTHETIC_DOCUMENT_FRAGMENT_not_for_routine_logs'


class TestBackendLogPrivacy(ConversionTestCase):
    @contextlib.asynccontextmanager
    async def upstream(self, body, status=500, alias='mymodel', content_type='application/json'):
        async def handle(request):
            await request.read()
            return web.Response(body=body, status=status, content_type=content_type)
        app = web.Application()
        app.router.add_route('*', '/{path:.*}', handle)
        async with TestServer(app) as server:
            old = self.app['config']['backends'][alias]['url']
            self.app['config']['backends'][alias]['url'] = str(server.make_url('')).rstrip('/')
            try:
                yield
            finally:
                self.app['config']['backends'][alias]['url'] = old

    async def chat(self):
        response = await self.client.post('/v1/chat/completions', headers=self.AUTH,
            json={'model': 'mymodel', 'messages': [{'role': 'user', 'content': 'synthetic'}]})
        body = await response.read()
        return response.status, body

    async def test_backend_error_body_is_omitted_but_safe_diagnostics_remain(self):
        for status, expected in ((400, 400), (500, 502)):
            raw = json.dumps({'error': MARKER}).encode()
            async with self.upstream(raw, status):
                with self.assertLogs('aiohttp.web', level='DEBUG') as logs:
                    actual, body = await self.chat()
            self.assertEqual(actual, expected)
            combined = '\n'.join(logs.output)
            self.assertNotIn(MARKER, combined)
            self.assertIn('status=' + str(status), combined)
            self.assertIn('body_len=' + str(len(raw)), combined)
            if status == 400:
                self.assertIn(MARKER.encode(), body)  # existing user-facing 4xx contract
        self.assertEqual(await self.get_events(), [])

    async def test_context_length_mapping_does_not_require_logging_the_body(self):
        raw = json.dumps({'error': 'maximum context length exceeded: ' + MARKER}).encode()
        async with self.upstream(raw):
            with self.assertLogs('aiohttp.web', level='DEBUG') as logs:
                status, body = await self.chat()
        self.assertEqual(status, 422)
        self.assertEqual(json.loads(body)['error']['code'], 'context_length_exceeded')
        self.assertNotIn(MARKER, '\n'.join(logs.output))
        self.assertEqual(await self.get_events(), [])

    async def test_untrusted_content_type_is_not_an_alternative_log_payload(self):
        mime_marker = MARKER.lower()
        async with self.upstream(b'{}', content_type='application/x-' + mime_marker):
            with self.assertLogs('aiohttp.web', level='DEBUG') as logs:
                status, _ = await self.chat()
        self.assertEqual(status, 502)
        self.assertNotIn(mime_marker, '\n'.join(logs.output))
        self.assertEqual(await self.get_events(), [])

    async def test_conversion_success_false_has_no_document_preview_in_logs(self):
        raw = json.dumps({'success': False, 'error': MARKER}).encode()
        async with self.upstream(raw, status=200, alias='mymarker'):
            with self.assertLogs('aiohttp.web', level='DEBUG') as logs:
                status, _, body = await self.submit(background=None)
        self.assertEqual(status, 422)
        self.assertEqual(body['error'], MARKER)
        self.assertNotIn(MARKER, '\n'.join(logs.output))
        self.assertEqual(await self.get_events(), [])

    async def test_invalid_claim_cannot_copy_backend_body_into_financial_logs(self):
        job = await self.queued(_claim_raw=json.dumps(MARKER))
        with self.assertLogs('aiohttp.web', level='DEBUG') as logs:
            status, _, _ = await self.poll(job)
        self.assertEqual(status, 502)
        self.assertNotIn(MARKER, '\n'.join(logs.output))
        self.assertEqual(await self.get_events(), [])

    async def test_malformed_upstream_header_cannot_escape_through_exception_text(self):
        async def malformed(reader, writer):
            try:
                await reader.readuntil(b'\r\n\r\n')
                writer.write(b'HTTP/1.1 200 OK\r\nInvalid-' + MARKER.encode() + b'\r\nContent-Length: 0\r\n\r\n')
                await writer.drain()
            finally:
                writer.close()
                await writer.wait_closed()
        server = await asyncio.start_server(malformed, '127.0.0.1', 0)
        self.app['config']['backends']['mymodel']['url'] = 'http://127.0.0.1:' + str(server.sockets[0].getsockname()[1])
        try:
            with self.assertLogs('aiohttp.web', level='DEBUG') as logs:
                status, _ = await self.chat()
            self.assertEqual(status, 502)
            self.assertNotIn(MARKER, '\n'.join(logs.output))
        finally:
            server.close()
            await server.wait_closed()
        self.assertEqual(await self.get_events(), [])
