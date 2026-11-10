import asyncio
import copy
from decimal import Decimal
import importlib
import importlib.util
import multiprocessing
import os
import sqlite3
import tempfile
import unittest

from llmproxy.db import SqliteDatabase
from .accounting_fixtures import SESSION, binding, receipt


def competing_writer(path, barrier, results):
    async def run():
        from llmproxy.accounting import record_receipt
        db = await SqliteDatabase.create('sqlite://' + path)
        try:
            await asyncio.to_thread(barrier.wait, 5)
            results.put(await record_receipt(db, binding(), receipt()))
        finally:
            await db.close()
    asyncio.run(run())


def crashing_writer(path):
    async def run():
        from llmproxy.accounting import record_receipt
        db = await SqliteDatabase.create('sqlite://' + path)
        await db.db.create_function('crash_process', 0, lambda: os._exit(91))
        await db.db.execute('''CREATE TEMP TRIGGER crash_after_bill AFTER INSERT ON event_oneoff
            BEGIN SELECT crash_process(); END''')
        await db.db.commit()
        await record_receipt(db, binding(), receipt())
    asyncio.run(run())


class TestSqliteAccounting(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        fd, self.path = tempfile.mkstemp()
        os.close(fd)
        self.addCleanup(os.unlink, self.path)
        self.db = await SqliteDatabase.create('sqlite://' + self.path)
        self.addAsyncCleanup(self.db.close)
        self.assertIsNotNone(importlib.util.find_spec('llmproxy.accounting'), 'Durable accounting API is missing')
        self.api = importlib.import_module('llmproxy.accounting')
        await self.api.bind_session(self.db, binding())

    async def rows(self):
        return await (await self.db.db.execute('SELECT * FROM event_oneoff')).fetchall()

    async def test_bind_is_immutable_and_never_persists_auth_material(self):
        value = binding()
        value['owner'].update(secret='synthetic-private-auth-value', comment='private-description')
        await self.api.bind_session(self.db, value)
        saved = await self.api.get_binding(self.db, SESSION)
        self.assertNotIn('secret', saved['owner'])
        self.assertNotIn('comment', saved['owner'])
        changed = copy.deepcopy(value)
        changed['owner']['id'] = 'another-account'
        with self.assertRaises(self.api.AccountingConflict):
            await self.api.bind_session(self.db, changed)

    async def test_one_receipt_creates_one_exact_financial_record_and_survives_reopen(self):
        self.assertTrue(await self.api.record_receipt(self.db, binding(), receipt()))
        self.assertFalse(await self.api.record_receipt(self.db, binding(), receipt()))
        reopened = await SqliteDatabase.create('sqlite://' + self.path)
        try:
            self.assertFalse(await self.api.record_receipt(reopened, binding(), receipt()))
        finally:
            await reopened.close()
        rows = await self.rows()
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]['api_key'], 'fixture-key')
        self.assertEqual(rows[0]['product'], 'parakeet/a5000/transcription')
        self.assertEqual(Decimal(rows[0]['quantity_exact']), Decimal('1'))

    async def test_fractional_samples_have_exact_authoritative_storage(self):
        value = receipt()
        value.update(sample_count=1, usage={'type': 'duration', 'seconds': 0.0000625})
        self.assertTrue(await self.api.record_receipt(self.db, binding(), value))
        [row] = await self.rows()
        self.assertEqual(row['quantity_exact'], '0.0000625')
        self.assertEqual(Decimal(row['quantity_exact']) * Decimal('0.001'), Decimal('0.0000000625'))

    async def test_conflicting_duplicate_or_overlapping_range_never_creates_a_second_bill(self):
        await self.api.record_receipt(self.db, binding(), receipt())
        changed = receipt()
        changed.update(sample_count=16001, usage={'type': 'duration', 'seconds': 1.0000625})
        with self.assertRaises(self.api.AccountingConflict):
            await self.api.record_receipt(self.db, binding(), changed)
        overlap = receipt()
        overlap.update(segment_id='seg_2', event_id=overlap['event_id'].replace('seg_1', 'seg_2'), cursor=2)
        with self.assertRaises(self.api.AccountingConflict):
            await self.api.record_receipt(self.db, binding(), overlap)
        self.assertEqual(len(await self.rows()), 1)

    async def test_terminal_failure_is_durable_but_never_billable(self):
        value = receipt()
        value.update(state='interrupted', completed_at=None)
        value.pop('usage')
        self.assertFalse(await self.api.record_receipt(self.db, binding(), value))
        self.assertEqual(await self.rows(), [])
        unacked = await self.api.unacked_receipts(self.db, SESSION, limit=100)
        self.assertEqual([item['event_id'] for item in unacked], [value['event_id']])

    async def test_two_processes_bill_the_same_receipt_only_once(self):
        ctx = multiprocessing.get_context('spawn')
        barrier, results = ctx.Barrier(2), ctx.Queue()
        processes = [ctx.Process(target=competing_writer, args=(self.path, barrier, results)) for _ in range(2)]
        try:
            for process in processes:
                process.start()
            answers = [await asyncio.to_thread(results.get, True, 8) for _ in processes]
            self.assertEqual(sorted(answers), [False, True])
        finally:
            for process in processes:
                await asyncio.to_thread(process.join, 5)
                if process.is_alive():
                    process.terminate()
                    await asyncio.to_thread(process.join, 5)
            results.close()
        self.assertTrue(all(process.exitcode == 0 for process in processes))
        self.assertEqual(len(await self.rows()), 1)

    async def test_failure_after_financial_insert_rolls_back_the_entire_sqlite_transaction(self):
        await self.db.db.execute('''CREATE TRIGGER reject_accounting_marker BEFORE UPDATE ON accounting_receipt
            WHEN NEW.status='billed' BEGIN SELECT RAISE(ABORT,'injected persistence failure'); END''')
        await self.db.db.commit()
        with self.assertRaises(self.api.AccountingUnavailable):
            await self.api.record_receipt(self.db, binding(), receipt())
        self.assertEqual(await self.rows(), [])
        await self.db.db.execute('DROP TRIGGER reject_accounting_marker')
        await self.db.db.commit()
        self.assertTrue(await self.api.record_receipt(self.db, binding(), receipt()))

    async def test_ack_and_reconcile_cursor_are_separate_from_live_delivery(self):
        await self.api.record_receipt(self.db, binding(), receipt())
        [session] = await self.api.pending_sessions(self.db, limit=100)
        self.assertEqual(session['reconcile_cursor'], 0)
        self.assertEqual(len(await self.api.unacked_receipts(self.db, SESSION, limit=100)), 1)
        await self.api.mark_acked(self.db, receipt()['event_id'])
        self.assertEqual(await self.api.unacked_receipts(self.db, SESSION, limit=100), [])
        await self.api.advance_cursor(self.db, SESSION, 1)
        await self.api.advance_cursor(self.db, SESSION, 0)
        [session] = await self.api.pending_sessions(self.db, limit=100)
        self.assertEqual(session['reconcile_cursor'], 1)

    async def test_crash_after_bill_insert_before_commit_recovers_without_double_charge(self):
        process = multiprocessing.get_context('spawn').Process(target=crashing_writer, args=(self.path,))
        process.start()
        await asyncio.to_thread(process.join, 8)
        if process.is_alive():
            process.terminate()
            await asyncio.to_thread(process.join, 5)
        self.assertEqual(process.exitcode, 91)
        self.assertEqual(await self.rows(), [])
        self.assertTrue(await self.api.record_receipt(self.db, binding(), receipt()))
        self.assertEqual(len(await self.rows()), 1)

    async def test_retry_after_user_revocation_keeps_original_historical_owner(self):
        await self.api.record_receipt(self.db, binding(), receipt())
        await self.db.db.execute("DELETE FROM api_key WHERE id='fixture-key'")
        await self.db.db.commit()
        self.assertFalse(await self.api.record_receipt(self.db, binding(), receipt()))
        [row] = await self.rows()
        self.assertEqual(row['api_key'], 'fixture-key')

    async def test_financial_owner_tampering_is_not_mistaken_for_an_idempotent_retry(self):
        await self.api.record_receipt(self.db, binding(), receipt())
        await self.db.db.execute("UPDATE event_oneoff SET api_key='another-account'")
        await self.db.db.commit()
        with self.assertRaises(self.api.AccountingConflict):
            await self.api.record_receipt(self.db, binding(), receipt())

    async def test_backend_identity_can_be_attached_exactly_once_after_preconnect_binding(self):
        value = binding()
        value.update(session_id='3' * 32, backend_instance_id=None)
        await self.api.bind_session(self.db, value)
        self.assertTrue(callable(getattr(self.api, 'attach_instance', None)), 'Backend attach API is missing')
        await self.api.attach_instance(self.db, value['session_id'], '2' * 32)
        await self.api.attach_instance(self.db, value['session_id'], '2' * 32)
        saved = await self.api.get_binding(self.db, value['session_id'])
        self.assertEqual(saved['backend_instance_id'], '2' * 32)
        with self.assertRaises(self.api.AccountingConflict):
            await self.api.attach_instance(self.db, value['session_id'], '4' * 32)

    async def test_backup_restore_preserves_billing_and_deduplication_together(self):
        await self.api.record_receipt(self.db, binding(), receipt())
        with tempfile.TemporaryDirectory() as directory:
            target = os.path.join(directory, 'restored.sqlite')
            source, destination = sqlite3.connect(self.path), sqlite3.connect(target)
            try:
                source.backup(destination)
            finally:
                source.close()
                destination.close()
            restored = await SqliteDatabase.create('sqlite://' + target)
            try:
                self.assertFalse(await self.api.record_receipt(restored, binding(), receipt()))
                rows = await (await restored.db.execute('SELECT quantity_exact FROM event_oneoff')).fetchall()
                self.assertEqual([row['quantity_exact'] for row in rows], ['1'])
            finally:
                await restored.close()
