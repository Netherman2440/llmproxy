import asyncio
import copy
import importlib
import importlib.util
import importlib.resources
import multiprocessing
import os
import sqlite3
import tempfile
import unittest
from unittest.mock import patch

import aiosqlite

from llmproxy.db import SqliteDatabase
from .accounting_fixtures import binding
from .test_conversion import ConversionTestCase


def delivery_binding():
    value = binding()
    return {name: copy.deepcopy(value[name]) for name in (
        'owner', 'backend_alias', 'backend_url', 'device', 'created_at')} | {
        'request_id': 'a' * 32, 'job_id': 'a' * 32}


def concurrent_settlement(path, intent, barrier, results):
    async def run():
        from llmproxy.delivery import settle_delivery
        db = await SqliteDatabase.create('sqlite://' + path)
        try:
            await asyncio.to_thread(barrier.wait, 5)
            results.put(await settle_delivery(db, intent, {'delivery_claimed': True}))
        finally:
            await db.close()
    asyncio.run(run())


def crashing_settlement(path, intent):
    async def run():
        from llmproxy.delivery import settle_delivery
        db = await SqliteDatabase.create('sqlite://' + path)
        await db.db.create_function('crash_delivery', 0, lambda: os._exit(95))
        await db.db.execute('''CREATE TEMP TRIGGER crash_delivery AFTER INSERT ON event_oneoff
            BEGIN SELECT crash_delivery(); END''')
        await db.db.commit()
        await settle_delivery(db, intent, {'delivery_claimed': True})
    asyncio.run(run())


class TestDeliveryJournal(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        fd, self.path = tempfile.mkstemp()
        os.close(fd)
        self.addCleanup(os.unlink, self.path)
        self.db = await SqliteDatabase.create('sqlite://' + self.path)
        self.addAsyncCleanup(self.db.close)
        self.assertIsNotNone(importlib.util.find_spec('llmproxy.delivery'), 'Durable conversion journal missing')
        self.api = importlib.import_module('llmproxy.delivery')

    async def count(self):
        return (await (await self.db.db.execute('SELECT COUNT(*) AS n FROM event_oneoff')).fetchone())['n']

    async def test_prepared_intent_is_durable_and_contains_no_credential_or_body(self):
        value = delivery_binding()
        value['owner'].update(secret='synthetic-auth-material', comment='synthetic-description')
        intent = await self.api.prepare_delivery(self.db, value, 5)
        self.assertEqual(intent['state'], 'prepared')
        self.assertNotIn('secret', intent['binding']['owner'])
        self.assertNotIn('comment', intent['binding']['owner'])
        reopened = await SqliteDatabase.create('sqlite://' + self.path)
        try:
            pending = await self.api.pending_deliveries(reopened)
            self.assertEqual([row['key'] for row in pending], [intent['key']])
        finally:
            await reopened.close()
        self.assertEqual(await self.count(), 0)

    async def test_confirmed_claim_bills_once_with_historical_quantity_and_time(self):
        intent = await self.api.prepare_delivery(self.db, delivery_binding(), 5)
        self.assertTrue(await self.api.settle_delivery(self.db, intent, {'delivery_claimed': True}))
        self.assertFalse(await self.api.settle_delivery(self.db, intent, {'delivery_claimed': False}))
        [row] = await (await self.db.db.execute('SELECT * FROM event_oneoff')).fetchall()
        self.assertEqual((row['api_key'], row['product'], row['quantity_exact']),
            ('fixture-key', 'parakeet/a5000/conversion', '5'))
        self.assertEqual(row['created'], '2026-06-01T00:00:00.000000+00:00')
        self.assertEqual(await self.api.pending_deliveries(self.db), [])

    async def test_missing_historical_bill_is_a_conflict_not_a_new_financial_write(self):
        intent = await self.api.prepare_delivery(self.db, delivery_binding(), 5)
        await self.api.settle_delivery(self.db, intent, {'delivery_claimed': True})
        # Preserve the FK in the ordinary path: direct corruption is simulated
        # through an independent administrative connection with FK checks off.
        other = sqlite3.connect(self.path)
        other.execute('DELETE FROM event_oneoff')
        other.commit()
        other.close()
        with self.assertRaises(self.api.AccountingConflict):
            await self.api.settle_delivery(self.db, intent, {'delivery_claimed': False})
        self.assertEqual(await self.count(), 0)

    async def test_unconfirmed_or_legacy_false_claim_never_gives_a_free_success(self):
        intent = await self.api.prepare_delivery(self.db, delivery_binding(), 5)
        with self.assertRaises(self.api.DeliveryUnresolved):
            await self.api.settle_delivery(self.db, intent, {'delivery_claimed': False})
        self.assertEqual(await self.count(), 0)
        self.assertEqual((await self.api.pending_deliveries(self.db))[0]['state'], 'unknown')

    async def test_bill_failure_preserves_confirmed_obligation_for_restart_recovery(self):
        intent = await self.api.prepare_delivery(self.db, delivery_binding(), 5)
        await self.db.db.executescript("""CREATE TRIGGER fail_bill BEFORE INSERT ON event_oneoff
            BEGIN SELECT RAISE(ABORT, 'injected failure'); END;""")
        with self.assertRaises(self.api.AccountingUnavailable):
            await self.api.settle_delivery(self.db, intent, {'delivery_claimed': True})
        self.assertEqual(await self.count(), 0)
        self.assertEqual((await self.api.pending_deliveries(self.db))[0]['state'], 'confirmed')
        await self.db.db.execute('DROP TRIGGER fail_bill')
        await self.db.db.commit()
        reopened = await SqliteDatabase.create('sqlite://' + self.path)
        try:
            self.assertTrue(await self.api.settle_delivery(reopened, intent))
            self.assertFalse(await self.api.settle_delivery(reopened, intent))
        finally:
            await reopened.close()
        self.assertEqual(await self.count(), 1)

    async def test_conflicting_page_count_or_tampered_owner_is_not_rebilled(self):
        value = delivery_binding()
        intent = await self.api.prepare_delivery(self.db, value, 5)
        with self.assertRaises(self.api.AccountingConflict):
            await self.api.prepare_delivery(self.db, value, 6)
        changed = copy.deepcopy(intent)
        changed['binding']['owner']['id'] = 'another-owner'
        with self.assertRaises(self.api.AccountingConflict):
            await self.api.settle_delivery(self.db, changed, {'delivery_claimed': True})
        self.assertEqual(await self.count(), 0)

    async def test_background_recovery_settles_only_proven_obligations_without_user_auth(self):
        self.assertTrue(hasattr(self.api, 'recover_once'), 'Independent delivery recovery missing')
        intent = await self.api.prepare_delivery(self.db, delivery_binding(), 5)
        await self.db.db.executescript("""CREATE TRIGGER fail_recovery BEFORE INSERT ON event_oneoff
            BEGIN SELECT RAISE(ABORT, 'injected failure'); END;""")
        with self.assertRaises(self.api.AccountingUnavailable):
            await self.api.settle_delivery(self.db, intent, {'delivery_claimed': True})
        await self.db.db.execute('DROP TRIGGER fail_recovery')
        await self.db.db.commit()
        other = delivery_binding() | {'job_id': 'b' * 32, 'request_id': 'b' * 32}
        unknown = await self.api.prepare_delivery(self.db, other, 7)
        with self.assertRaises(self.api.DeliveryUnresolved):
            await self.api.settle_delivery(self.db, unknown, {'delivery_claimed': False})
        app = {'config': {'db': {'uri': 'sqlite://' + self.path}}}
        self.assertEqual(await self.api.recover_once(app), 1)
        self.assertEqual(await self.api.recover_once(app), 0)
        self.assertEqual(await self.count(), 1)
        remaining = await self.api.pending_deliveries(self.db)
        self.assertEqual([item['state'] for item in remaining], ['unknown'])

    async def test_process_death_during_bill_retains_claim_proof_and_rolls_back_finance(self):
        intent = await self.api.prepare_delivery(self.db, delivery_binding(), 5)
        process = multiprocessing.get_context('spawn').Process(target=crashing_settlement, args=(self.path, intent))
        process.start()
        await asyncio.to_thread(process.join, 10)
        if process.is_alive():
            process.terminate()
            await asyncio.to_thread(process.join, 5)
        self.assertEqual(process.exitcode, 95)
        self.assertEqual(await self.count(), 0)
        self.assertEqual((await self.api.pending_deliveries(self.db))[0]['state'], 'confirmed')
        self.assertTrue(await self.api.settle_delivery(self.db, intent))
        self.assertFalse(await self.api.settle_delivery(self.db, intent))
        self.assertEqual(await self.count(), 1)

    async def test_paired_backup_restore_preserves_delivery_dedup_and_financial_sum(self):
        intent = await self.api.prepare_delivery(self.db, delivery_binding(), 5)
        await self.api.settle_delivery(self.db, intent, {'delivery_claimed': True})
        with tempfile.TemporaryDirectory() as directory:
            path = directory + '/restored.sqlite'
            target = sqlite3.connect(path)
            try:
                await self.db.db.backup(target)
            finally:
                target.close()
            restored = await SqliteDatabase.create('sqlite://' + path)
            try:
                self.assertFalse(await self.api.settle_delivery(restored, intent))
                rows = await (await restored.db.execute('SELECT quantity_exact FROM event_oneoff')).fetchall()
                self.assertEqual([row['quantity_exact'] for row in rows], ['5'])
            finally:
                await restored.close()

    async def test_genuine_v3_upgrade_preserves_prior_financial_records(self):
        with tempfile.TemporaryDirectory() as directory:
            path = directory + '/v3.sqlite'
            legacy = sqlite3.connect(path)
            package = importlib.resources.files('llmproxy')
            for resource in ('schema.sql', 'migrations/002-admission.sql', 'migrations/003-accounting.sql'):
                legacy.executescript(package.joinpath(resource).read_text())
            legacy.execute("INSERT INTO event_oneoff(created,api_key,product,quantity,rid,quantity_exact) VALUES('2026-06-01','old-owner','legacy/product',13,'old-request','13')")
            legacy.execute('PRAGMA user_version=3')
            legacy.commit()
            legacy.close()
            upgraded = await SqliteDatabase.create('sqlite://' + path)
            try:
                rows = await (await upgraded.db.execute('SELECT api_key,quantity_exact,rid FROM event_oneoff')).fetchall()
                self.assertEqual(rows, [{'api_key': 'old-owner', 'quantity_exact': '13', 'rid': 'old-request'}])
                self.assertEqual(await self.api.pending_deliveries(upgraded), [])
                version = await (await upgraded.db.execute('PRAGMA user_version')).fetchone()
                self.assertEqual(version['user_version'], 5)
            finally:
                await upgraded.close()

    async def test_two_real_processes_share_one_financial_write(self):
        intent = await self.api.prepare_delivery(self.db, delivery_binding(), 5)
        ctx = multiprocessing.get_context('spawn')
        barrier, results = ctx.Barrier(2), ctx.Queue()
        processes = [ctx.Process(target=concurrent_settlement, args=(self.path, intent, barrier, results)) for _ in range(2)]
        try:
            for process in processes:
                process.start()
            answers = [await asyncio.to_thread(results.get, True, 12) for _ in processes]
            self.assertEqual(sorted(answers), [False, True])
        finally:
            for process in processes:
                await asyncio.to_thread(process.join, 5)
                if process.is_alive():
                    process.terminate()
                    await asyncio.to_thread(process.join, 5)
            results.close()
        self.assertTrue(all(process.exitcode == 0 for process in processes))
        self.assertEqual(await self.count(), 1)


class TestConversionRecovery(ConversionTestCase):
    async def test_failed_bill_after_remote_claim_recovers_one_charge_on_next_poll(self):
        job = await self.queued(_pages='5')
        original = aiosqlite.Connection.execute
        async def fail_financial(connection, sql, *args, **kwargs):
            if 'INSERT INTO event_oneoff' in sql:
                raise RuntimeError('injected financial I/O failure')
            return await original(connection, sql, *args, **kwargs)
        with patch.object(aiosqlite.Connection, 'execute', fail_financial):
            status, _, _ = await self.poll(job)
        self.assertIn(status, (500, 503))
        self.assertEqual(await self.get_events(), [])
        status, _, body = await self.poll(job)
        self.assertEqual(status, 200)
        self.assertEqual(body['status'], 'completed')
        self.assertEqual(await self.get_events(), [{'product': 'mymarker/none/conversion', 'quantity': 5}])

    async def test_unknown_claim_is_not_issued_as_unbilled_output(self):
        job = await self.queued(_pages='5')
        for entry in self.backend.app['marker_jobs'].values():
            entry['claimed'] = True  # e.g. pre-cutover pickup with no local journal
        status, _, _ = await self.poll(job)
        self.assertEqual(status, 503)
        self.assertEqual(await self.get_events(), [])
