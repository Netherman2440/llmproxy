"""The access log must not persist linkable conversion identifiers."""
import logging
import types
import unittest

import aiohttp
import aiohttp.web

from llmproxy import app as app_module


class FakeRequest:
    version = types.SimpleNamespace(major=1, minor=1)
    remote = '127.0.0.1'

    def __init__(self, method, target):
        self.method = method
        self.path_qs = target


class FakeResponse:
    def __init__(self, status):
        self.status = status


class TestAccessLogRedaction(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.assertIsNotNone(getattr(app_module, 'RedactingAccessLogger', None),
            'Redacting access logger missing')
        self.logger = app_module.RedactingAccessLogger(
            logging.getLogger('test.access.redaction'), app_module.ACCESS_LOG_FORMAT)

    def request_line(self, method, target):
        [(_, line)] = [(key, value) for key, value
            in self.logger._format_line(FakeRequest(method, target), FakeResponse(401), 0.0)
            if key == 'first_request_line']
        return line

    def test_conversion_routes_redact_dynamic_id_and_query(self):
        for target in ('/v1/files/convert/abc123', '/v1/files/convert/a?x=1',
                '/v1/files/convert/' + 'i' * 64):
            with self.subTest(length=len(target)):
                line = self.request_line('GET', target)
                self.assertIn('/v1/files/convert/[REDACTED] HTTP/1.1', line)
                self.assertNotIn(target, line)
                self.assertTrue(line.startswith('GET '))

    def test_other_targets_and_methods_keep_exact_bytes(self):
        for method, target in (('GET', '/v1/models'), ('POST', '/v1/chat/completions'),
                ('GET', '/v1/realtime?model=mymodel'), ('GET', '/healthz')):
            with self.subTest(method=method, target=target):
                self.assertEqual(self.request_line(method, target),
                    '%s %s HTTP/1.1' % (method, target))

    def test_custom_logger_is_wired_into_the_production_entrypoint(self):
        self.assertTrue(issubclass(app_module.RedactingAccessLogger, aiohttp.web.AccessLogger))
        with open(app_module.__file__, 'rb') as stream:
            source = stream.read()
        self.assertIn(b'access_log_class=', source,
            'run_app must use the redacting access logger in production')

    async def test_actual_http_access_log_line_contains_no_conversion_id(self):
        marker = 'synthetic-conversion-id'
        records = []
        capture = logging.getLogger('test.access.redaction.http')
        handler = logging.Handler()
        handler.emit = lambda record: records.append(record.getMessage())
        capture.handlers[:] = [handler]
        capture.setLevel(logging.INFO)
        capture.propagate = False
        application = aiohttp.web.Application()
        application.router.add_get('/v1/files/convert/{id}',
            lambda request: aiohttp.web.json_response({}, status=401))
        runner = aiohttp.web.AppRunner(application, access_log=capture,
            access_log_class=app_module.RedactingAccessLogger,
            access_log_format=app_module.ACCESS_LOG_FORMAT)
        await runner.setup()
        site = aiohttp.web.TCPSite(runner, '127.0.0.1', 0)
        await site.start()
        client = aiohttp.ClientSession()
        try:
            port = runner.addresses[0][1]
            async with client.get('http://127.0.0.1:%d/v1/files/convert/%s' % (port, marker)) as response:
                self.assertEqual(response.status, 401)
                await response.read()
        finally:
            await client.close()
            await runner.cleanup()
        lines = [line for line in records if '/v1/files/convert/' in line]
        self.assertTrue(lines, 'Access log line missing entirely')
        for line in lines:
            self.assertNotIn(marker, line)
            self.assertIn('[REDACTED]', line)
            self.assertIn('401', line)
