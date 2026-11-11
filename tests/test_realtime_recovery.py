"""Real HTTP + persistent SQLite; only the GPU service is a wire fixture."""
import copy
import importlib
import importlib.util
import asyncio
import multiprocessing
import os
import tempfile
import unittest

import aiohttp
from aiohttp import web
from aiohttp.test_utils import TestServer

from llmproxy import accounting
from llmproxy.db import SqliteDatabase
from .accounting_fixtures import INSTANCE, SESSION, binding, receipt


def reconciliation_writer(path, url, barrier, results):
    async def run():
        from llmproxy.reconciliation import reconcile_once
        async with aiohttp.ClientSession() as client:
            app = {'config': {'db': {'uri': 'sqlite://' + path}, 'backends': {'parakeet': {
                'type': 'realtime', 'url': url, 'model': binding()['model_id'],
                'token': 'synthetic-service-auth'}}}, 'reconcile_client': client}
            await asyncio.to_thread(barrier.wait, 5)
            results.put(await reconcile_once(app))
    asyncio.run(run())


class TestReconciliation(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        fd, self.path = tempfile.mkstemp()
        os.close(fd)
        self.addCleanup(os.unlink, self.path)
        self.db = await SqliteDatabase.create('sqlite://' + self.path)
        self.addAsyncCleanup(self.db.close)
        self.items = [receipt()]
        self.state, self.pending = 'finished', 0
        self.overrides = {}
        self.acks, self.afters = [], []
        self.ack_errors = 0
        self.ack_without_bill = False
        backend = web.Application()
        backend.router.add_get('/internal/v1/sessions/{session}/receipts', self.page)
        backend.router.add_post('/internal/v1/receipts/{event}/ack', self.ack)
        self.server = TestServer(backend)
        await self.server.start_server()
        self.addAsyncCleanup(self.server.close)
        self.client = aiohttp.ClientSession()
        self.addAsyncCleanup(self.client.close)
        self.binding = binding() | {'backend_url': str(self.server.make_url('')).rstrip('/')}
        await accounting.bind_session(self.db, self.binding)
        self.app = {'config': {'db': {'uri': 'sqlite://' + self.path}, 'backends': {
            'parakeet': {'type': 'realtime', 'url': self.binding['backend_url'],
                'model': self.binding['model_id'], 'token': 'synthetic-service-auth'}}},
            'reconcile_client': self.client}
        self.assertIsNotNone(importlib.util.find_spec('llmproxy.reconciliation'),
            'Crash-independent reconciliation is missing')
        self.module = importlib.import_module('llmproxy.reconciliation')

    async def page(self, request):
        if request.headers.get('Authorization') != 'Bearer synthetic-service-auth':
            return web.Response(status=401)
        if request.match_info['session'] != SESSION:
            return web.Response(status=404)
        after = int(request.query['after'])
        self.afters.append(after)
        selected = [copy.deepcopy(item) for item in self.items if item['cursor'] > after][:1]
        next_cursor = selected[-1]['cursor'] if selected else after
        last_cursor = max([0] + [item['cursor'] for item in self.items])
        return web.json_response({'session_id': SESSION, 'backend_instance_id': INSTANCE,
            'protocol_version': 'receipt-v1', 'items': selected, 'next_cursor': next_cursor,
            'last_cursor': last_cursor, 'pending': self.pending, 'state': self.state,
            'has_more': bool(selected) and next_cursor < last_cursor,
            'pruned_cursor': 0, 'retention_gap': False, 'discarded_samples': 0} | self.overrides)

    async def ack(self, request):
        if request.headers.get('Authorization') != 'Bearer synthetic-service-auth':
            return web.Response(status=401)
        rows = await (await self.db.db.execute('SELECT * FROM event_oneoff')).fetchall()
        if not rows:
            self.ack_without_bill = True
        self.acks.append(request.match_info['event'])
        if self.ack_errors:
            self.ack_errors -= 1
            return web.Response(status=503)
        return web.json_response({'status': 'acknowledged'})

    async def count(self):
        return (await (await self.db.db.execute('SELECT COUNT(*) AS n FROM event_oneoff')).fetchone())['n']

    async def state_row(self):
        return await (await self.db.db.execute('SELECT * FROM accounting_session')).fetchone()

    async def test_restart_replays_completion_before_ack_without_a_user_key(self):
        self.assertEqual(await self.module.reconcile_once(self.app, 100), 1)
        self.assertEqual(await self.count(), 1)
        self.assertEqual(self.acks, [receipt()['event_id']])
        self.assertFalse(self.ack_without_bill)
        self.assertEqual((await self.state_row())['status'], 'settled')
        self.assertEqual((await self.state_row())['reconcile_cursor'], 1)
        self.assertEqual(await accounting.unacked_receipts(self.db, SESSION), [])
        self.assertEqual(await self.module.reconcile_once(self.app, 100), 0)
        self.assertEqual(await self.count(), 1)

    async def test_lost_ack_retries_without_duplicate_financial_record(self):
        self.ack_errors = 1
        await self.module.reconcile_once(self.app, 100)
        self.assertEqual(await self.count(), 1)
        self.assertEqual(len(await accounting.unacked_receipts(self.db, SESSION)), 1)
        self.assertNotEqual((await self.state_row())['status'], 'settled')
        await self.module.reconcile_once(self.app, 100)
        self.assertEqual(await self.count(), 1)
        self.assertEqual(len(self.acks), 2)
        self.assertEqual((await self.state_row())['status'], 'settled')
        self.assertTrue(self.app['accounting_healthy'])

    async def test_pending_session_is_not_mistaken_for_zero_debt(self):
        self.items, self.state, self.pending = [], 'active', 1
        await self.module.reconcile_once(self.app, 100)
        self.assertNotEqual((await self.state_row())['status'], 'settled')
        self.items, self.state, self.pending = [receipt()], 'finished', 0
        await self.module.reconcile_once(self.app, 100)
        self.assertEqual(await self.count(), 1)
        self.assertEqual((await self.state_row())['status'], 'settled')

    async def test_pagination_does_not_assume_contiguous_global_cursors(self):
        second = receipt()
        second.update(segment_id='seg_2', event_id=second['event_id'].replace('seg_1', 'seg_2'),
            cursor=7, start_sample=16000)
        self.items.append(second)
        await self.module.reconcile_once(self.app, 100)
        self.assertEqual(await self.count(), 2)
        self.assertEqual(self.afters, [0, 1])
        self.assertEqual((await self.state_row())['reconcile_cursor'], 7)

    async def test_retention_gap_is_an_incident_not_zero_usage(self):
        self.items = []
        self.overrides.update(retention_gap=True, pruned_cursor=1, last_cursor=1)
        await self.module.reconcile_once(self.app, 100)
        self.assertEqual(await self.count(), 0)
        self.assertEqual((await self.state_row())['status'], 'blocked')
        self.assertEqual((await self.state_row())['last_error'], 'retention_gap')

    async def test_wrong_backend_instance_never_bills_or_acks(self):
        self.overrides['backend_instance_id'] = '9' * 32
        await self.module.reconcile_once(self.app, 100)
        self.assertEqual(await self.count(), 0)
        self.assertEqual(self.acks, [])
        self.assertEqual((await self.state_row())['status'], 'blocked')

    async def test_empty_has_more_page_is_rejected_instead_of_looping(self):
        self.items = []
        self.overrides['has_more'] = True
        await self.module.reconcile_once(self.app, 100)
        self.assertEqual(self.afters, [0])
        self.assertEqual((await self.state_row())['status'], 'blocked')

    async def test_never_activated_preconnect_binding_settles_without_fabricating_work(self):
        await self.db.db.execute('DELETE FROM accounting_session')
        await self.db.db.commit()
        await accounting.bind_session(self.db, self.binding | {'backend_instance_id': None})
        await self.module.reconcile_once(self.app)
        self.assertEqual(await self.count(), 0)
        self.assertEqual(self.afters, [])
        self.assertEqual((await self.state_row())['status'], 'settled')

    async def test_two_actual_reconciliation_processes_create_one_financial_row(self):
        ctx = multiprocessing.get_context('spawn')
        barrier, results = ctx.Barrier(2), ctx.Queue()
        processes = [ctx.Process(target=reconciliation_writer, args=(self.path,
            self.binding['backend_url'], barrier, results)) for _ in range(2)]
        try:
            for process in processes:
                process.start()
            await asyncio.gather(*(asyncio.to_thread(results.get, True, 12) for _ in processes))
        finally:
            for process in processes:
                await asyncio.to_thread(process.join, 5)
                if process.is_alive():
                    process.terminate()
                    await asyncio.to_thread(process.join, 5)
            results.close()
        self.assertTrue(all(process.exitcode == 0 for process in processes))
        self.assertEqual(await self.count(), 1)
        self.assertEqual((await self.state_row())['status'], 'settled')
        self.assertFalse(self.ack_without_bill)

    async def test_live_bill_replayed_by_reconciler_is_not_charged_twice(self):
        await accounting.record_receipt(self.db, self.binding, receipt())
        await self.module.reconcile_once(self.app, 100)
        self.assertEqual(await self.count(), 1)
        self.assertFalse(self.ack_without_bill)
        self.assertEqual((await self.state_row())['status'], 'settled')
