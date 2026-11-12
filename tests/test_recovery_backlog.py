"""Retained unresolved obligations must stay observable after a clean pass."""
import os
from unittest import mock

import aiohttp.test_utils

from . import accounting_fixtures
from .metrics_fixtures import AUTH_HEADERS as METRICS_AUTH, TOKEN as METRICS_TOKEN
from .test_proxy import LLMProxyAppTestCase, _parse_metrics
from llmproxy import delivery


class TestRecoveryBacklog(LLMProxyAppTestCase):
    AUTH = {'Authorization': 'Bearer mytoken'}

    async def asyncSetUp(self):
        patcher = mock.patch.dict(os.environ, {'LLMPROXY_METRICS_TOKEN': METRICS_TOKEN})
        patcher.start()
        self.addCleanup(patcher.stop)
        await super().asyncSetUp()

    async def get_client(self, server):
        return aiohttp.test_utils.TestClient(server, headers=METRICS_AUTH)

    async def journal(self):
        from llmproxy.db import SqliteDatabase
        return await SqliteDatabase.create('sqlite://' + self.db_path)

    async def prepared_binding(self, suffix='1'):
        binding = accounting_fixtures.binding() | {
            'backend_alias': 'audit-delivery', 'request_id': 'e' * 31 + suffix,
            'job_id': 'f' * 31 + suffix, 'device': 'gpu',
            'backend_url': 'http://private-converter:8000'}
        return binding

    async def test_clean_page_does_not_mask_retained_unresolved_conversion_debt(self):
        db = await self.journal()
        try:
            await delivery.prepare_delivery(db, await self.prepared_binding(), 3)
        finally:
            await db.close()
        with self.assertLogs(level='ERROR') as captured:
            settled = await delivery.recover_once(self.app)
        self.assertEqual(settled, 0, 'Unproven claim must not be guessed into a bill')
        self.assertFalse(self.app['delivery_recovery_healthy'],
            'A clean worker page masked retained conversion debt')
        self.assertTrue(any('conversion' in line.lower() or 'delivery' in line.lower()
            for line in captured.output), 'No operator-facing debt alarm')
        db = await self.journal()
        try:
            [retained] = await delivery.pending_deliveries(db)
            self.assertEqual(retained['state'], 'prepared')
        finally:
            await db.close()

    async def test_recovery_health_is_exported_for_scraping_and_alerting(self):
        db = await self.journal()
        try:
            await delivery.prepare_delivery(db, await self.prepared_binding(), 3)
        finally:
            await db.close()
        await delivery.recover_once(self.app)
        async with self.client.get('/metrics', headers=METRICS_AUTH) as response:
            self.assertEqual(response.status, 200)
            body = await response.text()
        lines = _parse_metrics(body).get('llmproxy_recovery_healthy', [])
        self.assertTrue(lines, 'Recovery health gauge missing from /metrics')
        values = {line.split('{', 1)[1].split('}', 1)[0]: float(line.rsplit(' ', 1)[1])
            for line in lines}
        self.assertEqual(values.get('journal="conversion_delivery"'), 0.0,
            'Retained conversion debt must read as unhealthy')
        self.assertIn('journal="http_attempt"', values,
            'HTTP attempt journal health missing')

    async def test_drained_backlog_returns_to_healthy(self):
        db = await self.journal()
        try:
            binding = await self.prepared_binding()
            intent = await delivery.prepare_delivery(db, binding, 3)
            await db.delivery_outcome(intent['key'], delivery.fingerprint(
                delivery.normalize(binding, 3)), True)
            intent['state'] = 'confirmed'
        finally:
            await db.close()
        settled = await delivery.recover_once(self.app)
        self.assertEqual(settled, 1)
        self.assertTrue(self.app['delivery_recovery_healthy'])
        db = await self.journal()
        try:
            self.assertEqual(await delivery.pending_deliveries(db), [])
        finally:
            await db.close()
