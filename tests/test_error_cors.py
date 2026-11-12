"""Configured CORS origin must be readable on API errors, not only successes."""
from .test_proxy import LLMProxyAppTestCase


class TestErrorCorsHeaders(LLMProxyAppTestCase):
    async def get_application(self):
        app = await super().get_application()
        app['config']['http_origin'] = 'https://synthetic-origin.test'
        return app

    async def check(self, response):
        self.assertGreaterEqual(response.status, 400)
        self.assertEqual(response.headers.get('Access-Control-Allow-Origin'), 'https://synthetic-origin.test')
        self.assertEqual(response.headers.get('Access-Control-Expose-Headers'),
            'X-Request-ID, X-AI-Generated, X-LLMProxy-Extensions, '
            'X-LLMProxy-Max-Output-Tokens, X-LLMProxy-Budget-Source, '
            'X-LLMProxy-Budget-Clamped, X-LLMProxy-Truncation, Location, Retry-After')
        self.assertTrue(response.headers.get('Access-Control-Allow-Headers'))
        await response.read()

    async def test_authentication_error_carries_cors_headers(self):
        response = await self.client.post('/v1/chat/completions',
            headers={'Authorization': 'Bearer invalid-synthetic'},
            json={'model': 'mymodel', 'messages': []})
        await self.check(response)

    async def test_client_error_carries_cors_headers(self):
        response = await self.client.post('/v1/chat/completions',
            headers={'Authorization': 'Bearer mytoken', 'Content-Type': 'application/json'},
            data=b'not-json')
        await self.check(response)

    async def test_unauthorized_metrics_scrape_carries_cors_headers(self):
        response = await self.client.get('/metrics')
        await self.check(response)

    async def test_successful_response_keeps_existing_cors_headers(self):
        response = await self.client.get('/v1/models',
            headers={'Authorization': 'Bearer mytoken'})
        self.assertEqual(response.status, 200)
        self.assertEqual(response.headers.get('Access-Control-Allow-Origin'), 'https://synthetic-origin.test')
        await response.read()
