import copy
import os
import unittest
from unittest import mock

from aiohttp.test_utils import TestClient

from llmproxy import auth, config, metrics
from llmproxy.app import create_app
from llmproxy.db import get_db
from .metrics_fixtures import AUTH_HEADERS, MYUSER_LABEL, TOKEN
from .test_proxy import LLMProxyAppTestCase, ProductionTestServer


class TestMetricsPrivacy(LLMProxyAppTestCase):
    async def asyncSetUp(self):
        await super().asyncSetUp()
        patcher = mock.patch.dict(os.environ, {'LLMPROXY_METRICS_TOKEN': TOKEN})
        patcher.start()
        self.addCleanup(patcher.stop)

    async def test_anonymous_api_key_cookie_and_query_cannot_scrape(self):
        cases = [('/metrics', {}), ('/metrics', {'Authorization': 'Bearer mytoken'}),
            ('/metrics?token=' + TOKEN, {}), ('/metrics', {'Cookie': 'metrics_token=' + TOKEN}),
            ('/metrics', {'Authorization': 'Basic ' + TOKEN}),
            ('/metrics', {'Authorization': 'Bearer ' + 'x' * 300})]
        for path, headers in cases:
            with self.subTest(path=path, header_names=list(headers)):
                async with self.client.get(path, headers=headers) as response:
                    self.assertEqual(response.status, 401)
                    self.assertNotIn(b'# HELP', await response.read())
                    self.assertIn('WWW-Authenticate', response.headers)
        self.assertEqual(await self.get_events(), [])

    async def test_dedicated_credential_scrapes_without_api_auth_or_billing(self):
        with mock.patch.object(auth, 'require_auth', side_effect=AssertionError('No LLM user auth for scrape')):
            for scheme in ('Bearer', 'bearer'):
                async with self.client.get('/metrics', headers={'Authorization': scheme + ' ' + TOKEN}) as response:
                    self.assertEqual(response.status, 200)
                    self.assertIn(b'# HELP llmproxy_', await response.read())
        self.assertEqual(await self.get_events(), [])
        async with self.client.get('/v1/models', headers=AUTH_HEADERS) as response:
            self.assertEqual(response.status, 401, 'Scrape credential cannot become an LLM API key')

    async def test_disabled_metrics_are_not_served_even_to_the_scrape_principal(self):
        cfg = copy.deepcopy(self.app['config'])
        cfg['metrics'] = {'enabled': False}
        app = await create_app(cfg)
        async with TestClient(ProductionTestServer(app)) as client:
            async with client.get('/metrics', headers=AUTH_HEADERS) as response:
                self.assertEqual(response.status, 404)

    async def test_head_cannot_bypass_scrape_authorization(self):
        async with self.client.head('/metrics') as response:
            self.assertEqual(response.status, 401)
        async with self.client.head('/metrics', headers=AUTH_HEADERS) as response:
            self.assertEqual(response.status, 200)

    async def test_missing_short_or_invalid_scrape_secret_fails_closed(self):
        for secret in ('', 'short', 'contains\ncontrol' * 4):
            with self.subTest(length=len(secret)), mock.patch.dict(os.environ, {'LLMPROXY_METRICS_TOKEN': secret}):
                async with self.client.get('/metrics', headers=AUTH_HEADERS) as response:
                    self.assertEqual(response.status, 401)
                    self.assertNotIn(b'# HELP', await response.read())

    async def test_secret_rotation_rejects_old_credential_without_reassigning_users(self):
        replacement = 'different-test-' * 4
        with mock.patch.dict(os.environ, {'LLMPROXY_METRICS_TOKEN': replacement}):
            async with self.client.get('/metrics', headers=AUTH_HEADERS) as response:
                self.assertEqual(response.status, 401)
            async with self.client.get('/metrics', headers={'Authorization': 'Bearer ' + replacement}) as response:
                self.assertEqual(response.status, 200)

    async def test_custom_path_and_secret_reference_keep_separate_authorization(self):
        cfg = copy.deepcopy(self.app['config'])
        cfg['metrics'] = {'enabled': True, 'path': '/internal/telemetry', 'token_env': 'TEST_SCRAPE_CREDENTIAL'}
        app = await create_app(cfg)
        async with TestClient(ProductionTestServer(app)) as client:
            with mock.patch.dict(os.environ, {'TEST_SCRAPE_CREDENTIAL': 'alternate-test-' * 4}):
                async with client.get('/internal/telemetry', headers=AUTH_HEADERS) as response:
                    self.assertEqual(response.status, 401)
                async with client.get('/internal/telemetry', headers={'Authorization': 'Bearer ' + 'alternate-test-' * 4}) as response:
                    self.assertEqual(response.status, 200)
                async with client.get('/metrics', headers=AUTH_HEADERS) as response:
                    self.assertEqual(response.status, 404)

    async def test_metrics_never_use_comment_raw_key_id_or_credentials_as_labels(self):
        db = await get_db(self.app['config']['db']['uri'])
        try:
            await db.user_update({'id': 'myuser'}, comment='Synthetic Person <private@example.invalid>')
        finally:
            await db.close()
        auth.flush_cache()
        async with self.client.post('/v1/chat/completions', headers={'Authorization': 'Bearer mytoken'},
                json={'model': 'mymodel', 'messages': [{'role': 'user', 'content': 'synthetic'}]}) as response:
            self.assertEqual(response.status, 200)
            await response.read()
        async with self.client.get('/metrics', headers=AUTH_HEADERS) as response:
            self.assertEqual(response.status, 200)
            text = await response.text()
        for private in ('Synthetic Person', 'private@example.invalid', 'myuser', 'mytoken', TOKEN):
            self.assertNotIn(private, text)
        lines = [line for line in text.splitlines() if line.startswith('llmproxy_user_requests_total{')]
        self.assertTrue(any('user="' + MYUSER_LABEL + '"' in line and 'model="mymodel"' in line for line in lines))
        self.assertEqual(len(await self.get_events()), 2)


class TestMetricsIdentityAndConfig(unittest.TestCase):
    def test_identity_is_stable_across_comment_and_secret_changes(self):
        first = {'id': 'myuser', 'comment': 'Synthetic Person', 'secret': 'fixture-one'}
        second = {'id': 'myuser', 'comment': 'Another Person', 'secret': 'fixture-two'}
        self.assertEqual(metrics.user_label(first), MYUSER_LABEL)
        self.assertEqual(metrics.user_label(second), MYUSER_LABEL)
        self.assertNotEqual(metrics.user_label({'id': 'another-key'}), MYUSER_LABEL)
        self.assertEqual(metrics.user_label(None), '__anon__')

    def test_credential_reference_is_typed_and_literal_credentials_are_rejected(self):
        for value in (None, 1, [], '', 'a/b', 'A B', 'A=B', 'A\nB'):
            with self.subTest(value=value), self.assertRaises(config.ConfigError):
                config.validate({'metrics': {'token_env': value}})
        with self.assertRaises(config.ConfigError):
            config.validate({'metrics': {'token': TOKEN}})
        config.validate({'metrics': {'token_env': 'TEST_SCRAPE_CREDENTIAL'}})
