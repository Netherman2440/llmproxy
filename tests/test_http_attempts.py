"""HTTP/SSE obligations survive lost frontend/process, without guessed usage."""
import asyncio
import importlib
import importlib.util
import importlib.resources
from pathlib import Path
import sqlite3
import sys
import tempfile
import unittest

from llmproxy.accounting import AccountingConflict
from llmproxy.db import SqliteDatabase


def binding():
    return {'request_id': 'a' * 32, 'owner': {'id': 'fixture-key'},
        'backend_alias': 'fixture-model', 'backend_url': 'http://private-backend',
        'device': 'cpu', 'endpoint': '/v1/chat/completions',
        'created_at': '2026-06-01T00:00:00.000000+00:00'}


RESOURCES = {'fixture-model/cpu/prompt': 13, 'fixture-model/cpu/completion': 7}


class TestHttpAttempts(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.assertIsNotNone(importlib.util.find_spec('llmproxy.attempts'), 'Durable HTTP/SSE attempt ledger missing')
        self.api = importlib.import_module('llmproxy.attempts')
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.path = str(Path(self.directory.name) / 'state.sqlite')
        self.db = await SqliteDatabase.create('sqlite://' + self.path)
        self.addAsyncCleanup(self.db.close)

    async def rows(self):
        return await self.db.db.execute_fetchall('SELECT * FROM event_oneoff ORDER BY product')

    async def test_prepared_unknown_survives_reopen_without_auth_material_or_invented_zero_bill(self):
        value = binding()
        value['owner'].update(secret='synthetic-auth', comment='synthetic-comment')
        value['body'] = 'never persist request body'
        intent = await self.api.prepare(self.db, value)
        await self.api.abandon(self.db, intent)
        reopened = await SqliteDatabase.create('sqlite://' + self.path)
        try:
            [stored] = await self.api.pending(reopened)
            self.assertEqual(stored['state'], 'unknown')
            self.assertEqual(stored['binding']['owner'], {'id': 'fixture-key'})
            self.assertNotIn('body', stored['binding'])
            with self.assertRaises(AccountingConflict):
                await self.api.settle(reopened, stored)
            app = {'config': {'db': {'uri': 'sqlite://' + self.path}}}
            self.assertEqual(await self.api.recover_once(app), 0)
            self.assertFalse(app['http_attempt_recovery_healthy'])
            self.assertEqual(await self.rows(), [])
        finally:
            await reopened.close()

    async def test_confirmed_usage_replays_once_with_immutable_historical_owner_and_time(self):
        intent = await self.api.prepare(self.db, binding())
        await self.api.confirm(self.db, intent, RESOURCES, observed_at='2026-06-01T00:00:05.123456+00:00')
        # Original connection/browser/key lifecycle is not recovery authority.
        app = {'config': {'db': {'uri': 'sqlite://' + self.path}}}
        self.assertEqual(await self.api.recover_once(app), 1)
        self.assertEqual(await self.api.recover_once(app), 0)
        rows = await self.rows()
        self.assertEqual([(r['api_key'], r['product'], r['quantity_exact'], r['created']) for r in rows], [
            ('fixture-key', 'fixture-model/cpu/completion', '7', '2026-06-01T00:00:05.123456+00:00'),
            ('fixture-key', 'fixture-model/cpu/prompt', '13', '2026-06-01T00:00:05.123456+00:00')])
        self.assertTrue(app['http_attempt_recovery_healthy'])
        self.assertEqual(await self.api.pending(self.db), [])
        self.assertFalse(await self.api.settle(self.db, intent))

    async def test_conflicting_owner_proof_and_foreign_or_fractional_products_cannot_bill(self):
        intent = await self.api.prepare(self.db, binding())
        foreign = binding()
        foreign['owner'] = {'id': 'other-key'}
        with self.assertRaises(AccountingConflict):
            await self.api.prepare(self.db, foreign)
        for resources in ({'other/cpu/prompt': 13}, {'fixture-model/cpu/prompt': -1},
                          {'fixture-model/cpu/prompt': True}, {'fixture-model/cpu/prompt': 1.5}):
            with self.subTest(resources=resources), self.assertRaises(ValueError):
                await self.api.confirm(self.db, intent, resources)
        await self.api.confirm(self.db, intent, RESOURCES)
        with self.assertRaises(AccountingConflict):
            await self.api.confirm(self.db, intent, {**RESOURCES, 'fixture-model/cpu/completion': 8})
        self.assertTrue(await self.api.settle(self.db, intent))
        self.assertEqual(len(await self.rows()), 2)

    async def test_transaction_failure_rolls_back_both_products_but_preserves_confirmed_usage(self):
        intent = await self.api.prepare(self.db, binding())
        await self.api.confirm(self.db, intent, RESOURCES)
        await self.db.db.execute("""CREATE TRIGGER fail_prompt BEFORE INSERT ON event_oneoff
            WHEN NEW.product LIKE '%/completion' BEGIN SELECT RAISE(ABORT, 'injected finance fault'); END""")
        await self.db.db.commit()
        with self.assertRaises(Exception):
            await self.api.settle(self.db, intent)
        self.assertEqual(await self.rows(), [])
        [stored] = await self.api.pending(self.db)
        self.assertEqual(stored['state'], 'confirmed')
        await self.db.db.execute('DROP TRIGGER fail_prompt')
        await self.db.db.commit()
        self.assertTrue(await self.api.settle(self.db, stored))
        self.assertEqual(len(await self.rows()), 2)

    async def test_two_actual_recovery_processes_produce_one_financial_set(self):
        intent = await self.api.prepare(self.db, binding())
        await self.api.confirm(self.db, intent, RESOURCES)
        code = '''import asyncio,sys
from llmproxy import attempts
from llmproxy.db import SqliteDatabase
async def run():
 db=await SqliteDatabase.create('sqlite://'+sys.argv[1])
 try:
  [intent]=await attempts.pending(db)
  await attempts.settle(db,intent)
 finally: await db.close()
asyncio.run(run())
'''
        # Both processes receive the retained intent, even if the other wins first.
        code = code.replace('[intent]=await attempts.pending(db)',
            "intent=" + repr(intent))
        processes = [await asyncio.create_subprocess_exec(sys.executable, '-c', code, self.path,
            stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE) for _ in range(2)]
        for process in processes:
            out, err = await asyncio.wait_for(process.communicate(), 10)
            self.assertEqual(process.returncode, 0, (out + err).decode())
        self.assertEqual(len(await self.rows()), 2)

    async def test_actual_process_exit_during_finance_retains_proof_without_partial_bill(self):
        intent = await self.api.prepare(self.db, binding())
        await self.api.confirm(self.db, intent, RESOURCES)
        code = '''import asyncio,os,sys
from llmproxy import attempts
from llmproxy.db import SqliteDatabase
async def run():
 db=await SqliteDatabase.create('sqlite://'+sys.argv[1])
 await db.db.create_function('crash_finance',0,lambda:os._exit(96))
 await db.db.execute("CREATE TEMP TRIGGER crash BEFORE INSERT ON event_oneoff WHEN NEW.product LIKE '%/completion' BEGIN SELECT crash_finance(); END")
 [intent]=await attempts.pending(db)
 await attempts.settle(db,intent)
asyncio.run(run())
'''
        process = await asyncio.create_subprocess_exec(sys.executable, '-c', code, self.path)
        self.assertEqual(await asyncio.wait_for(process.wait(), 10), 96)
        self.assertEqual(await self.rows(), [])
        [stored] = await self.api.pending(self.db)
        self.assertEqual(stored['state'], 'confirmed')
        self.assertTrue(await self.api.settle(self.db, stored))
        self.assertEqual(len(await self.rows()), 2)

    async def test_missing_previously_billed_finance_is_conflict_not_a_new_bill(self):
        intent = await self.api.prepare(self.db, binding())
        await self.api.confirm(self.db, intent, RESOURCES)
        await self.api.settle(self.db, intent)
        with sqlite3.connect(self.path) as corrupt:
            corrupt.execute('DELETE FROM event_oneoff')
        with self.assertRaises(AccountingConflict):
            await self.api.settle(self.db, intent)
        self.assertEqual(await self.rows(), [])

    async def test_genuine_v4_upgrade_preserves_finance_before_adding_http_obligations(self):
        path = str(Path(self.directory.name) / 'legacy.sqlite')
        with sqlite3.connect(path) as legacy:
            for resource in ('schema.sql', 'migrations/002-admission.sql',
                             'migrations/003-accounting.sql', 'migrations/004-delivery.sql'):
                legacy.executescript(importlib.resources.files('llmproxy').joinpath(resource).read_text())
            legacy.execute('PRAGMA user_version=4')
            legacy.execute("INSERT INTO event_oneoff(created,api_key,product,quantity,rid) VALUES('historical','old-key','old/product',3,'old-rid')")
        migrated = await SqliteDatabase.create('sqlite://' + path)
        try:
            [row] = await migrated.db.execute_fetchall('SELECT * FROM event_oneoff')
            self.assertEqual((row['api_key'], row['quantity'], row['rid']), ('old-key', 3, 'old-rid'))
            await self.api.prepare(migrated, binding())
            self.assertEqual(len(await self.api.pending(migrated)), 1)
        finally:
            await migrated.close()

    async def test_consistent_backup_restores_proof_and_unknown_without_double_billing(self):
        intent = await self.api.prepare(self.db, binding())
        await self.api.confirm(self.db, intent, RESOURCES)
        other = binding()
        other['request_id'] = 'b' * 32
        unknown = await self.api.prepare(self.db, other)
        await self.api.abandon(self.db, unknown)
        restored_path = str(Path(self.directory.name) / 'restored.sqlite')
        with sqlite3.connect(self.path) as source, sqlite3.connect(restored_path) as destination:
            source.backup(destination)
        app = {'config': {'db': {'uri': 'sqlite://' + restored_path}}}
        self.assertEqual(await self.api.recover_once(app), 1)
        self.assertEqual(await self.api.recover_once(app), 0)
        self.assertFalse(app['http_attempt_recovery_healthy'])
        restored = await SqliteDatabase.create('sqlite://' + restored_path)
        try:
            [remaining] = await self.api.pending(restored)
            self.assertEqual(remaining['key'], 'b' * 32)
            self.assertEqual(remaining['state'], 'unknown')
            self.assertEqual(len(await restored.db.execute_fetchall('SELECT * FROM event_oneoff')), 2)
        finally:
            await restored.close()

    async def test_rejected_attempt_has_no_debt_and_late_abandon_cannot_erase_confirmed_proof(self):
        intent = await self.api.prepare(self.db, binding())
        await self.api.confirm(self.db, intent, RESOURCES)
        await self.api.abandon(self.db, intent)
        [stored] = await self.api.pending(self.db)
        self.assertEqual(stored['state'], 'confirmed')
        other = binding()
        other['request_id'] = 'b' * 32
        rejected = await self.api.prepare(self.db, other)
        await self.api.abandon(self.db, rejected, rejected=True)
        self.assertEqual(len(await self.api.pending(self.db)), 1)
        self.assertEqual(await self.rows(), [])
