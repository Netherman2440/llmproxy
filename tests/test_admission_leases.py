import asyncio
import multiprocessing
import importlib.resources
import sqlite3
import os
import tempfile
import unittest
from unittest.mock import patch

from llmproxy.db import DatabaseError, SqliteDatabase


def _competing_process(path, barrier, release, results, owner):
    async def run():
        db = await SqliteDatabase.create("sqlite://" + path)
        try:
            await asyncio.to_thread(barrier.wait, 5)
            acquired = await db.admission_acquire("shared-bucket", owner, 1, 30)
            results.put((owner, acquired))
            await asyncio.to_thread(release.wait, 5)
            if acquired:
                await db.admission_release("shared-bucket", owner)
        finally:
            await db.close()
    asyncio.run(run())


class TestSqliteAdmissionLeases(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        fd, self.path = tempfile.mkstemp()
        os.close(fd)
        self.db = await SqliteDatabase.create("sqlite://" + self.path)
        self.addCleanup(os.unlink, self.path)
        self.addAsyncCleanup(self.db.close)

    def api(self):
        for name in ("admission_acquire", "admission_renew", "admission_release"):
            self.assertTrue(callable(getattr(self.db, name, None)), "Missing adapter-owned lease API: " + name)

    async def test_late_release_cannot_free_another_owner(self):
        self.api()
        self.assertTrue(await self.db.admission_acquire("bucket", "old-owner", 1, 30))
        self.assertFalse(await self.db.admission_release("bucket", "wrong-owner"))
        self.assertFalse(await self.db.admission_acquire("bucket", "new-owner", 1, 30))
        self.assertTrue(await self.db.admission_release("bucket", "old-owner"))
        self.assertTrue(await self.db.admission_acquire("bucket", "new-owner", 1, 30))
        self.assertFalse(await self.db.admission_release("bucket", "old-owner"))
        self.assertFalse(await self.db.admission_acquire("bucket", "third-owner", 1, 30))

    async def test_expired_lease_is_replaced_without_reviving_old_ownership(self):
        self.api()
        self.assertTrue(await self.db.admission_acquire("bucket", "old-owner", 1, 30))
        await self.db.db.execute("UPDATE admission_lease SET expires_at = 0")
        await self.db.db.commit()
        self.assertTrue(await self.db.admission_acquire("bucket", "new-owner", 1, 30))
        self.assertFalse(await self.db.admission_renew("bucket", "old-owner", 30))
        self.assertFalse(await self.db.admission_release("bucket", "old-owner"))
        self.assertFalse(await self.db.admission_acquire("bucket", "third-owner", 1, 30))

    async def test_renewal_requires_current_unexpired_ownership(self):
        self.api()
        self.assertFalse(await self.db.admission_renew("bucket", "missing-owner", 30))
        self.assertTrue(await self.db.admission_acquire("bucket", "owner", 1, 30))
        self.assertTrue(await self.db.admission_renew("bucket", "owner", 60))
        self.assertTrue(await self.db.admission_release("bucket", "owner"))
        self.assertFalse(await self.db.admission_renew("bucket", "owner", 30))

    async def test_renewal_checks_database_time_when_update_executes_not_before_queue_delay(self):
        self.assertTrue(await self.db.admission_acquire('delayed', 'old-owner', 1, 0.2))
        native = self.db.db._execute
        delayed_updates = []

        async def dispatch(function, *args, **kwargs):
            if args and isinstance(args[0], str) and args[0].lstrip().startswith('UPDATE admission_lease'):
                delayed_updates.append(True)
                await asyncio.sleep(0.25)
            return await native(function, *args, **kwargs)

        # Both implementations run the real SQLite statement after the same
        # controlled queue delay. No result/clock/ownership is mocked.
        with patch.object(self.db.db, '_execute', dispatch):
            renewed = await self.db.admission_renew('delayed', 'old-owner', 1)
        self.assertTrue(delayed_updates, 'Harness did not delay the actual SQL update')
        self.assertFalse(renewed, 'A lease expired in the worker queue was resurrected')
        self.assertTrue(await self.db.admission_acquire('delayed', 'replacement', 1, 30))
        self.assertFalse(await self.db.admission_release('delayed', 'old-owner'))

    async def test_current_schema_connection_validation_does_not_take_financial_writer_lock(self):
        await self.db.db.execute('BEGIN IMMEDIATE')
        opening = asyncio.create_task(SqliteDatabase.create('sqlite://' + self.path))
        opened = None
        try:
            done, _ = await asyncio.wait({opening}, timeout=0.03)
            # Release a controlled competing financial writer even on RED, so
            # no native SQLite worker is abandoned behind its busy timeout.
            await self.db.db.rollback()
            opened = await asyncio.wait_for(opening, 1)
            self.assertTrue(done, 'Current-schema validation waits for a write lock before admission')
        finally:
            await self.db.db.rollback()
            if opened is not None:
                await opened.close()
            elif not opening.done():
                opening.cancel()
                await asyncio.gather(opening, return_exceptions=True)

    async def test_two_processes_cannot_overadmit_one_shared_slot(self):
        self.api()
        ctx = multiprocessing.get_context("spawn")
        barrier, release, results = ctx.Barrier(2), ctx.Event(), ctx.Queue()
        processes = [ctx.Process(target=_competing_process,
            args=(self.path, barrier, release, results, owner)) for owner in ("process-a", "process-b")]
        try:
            for process in processes:
                process.start()
            answers = [await asyncio.to_thread(results.get, True, 8) for _ in processes]
            self.assertEqual(sorted(acquired for _, acquired in answers), [False, True])
        finally:
            release.set()
            for process in processes:
                await asyncio.to_thread(process.join, 5)
                if process.is_alive():
                    process.terminate()
                    await asyncio.to_thread(process.join, 5)
            results.close()
        for process in processes:
            self.assertEqual(process.exitcode, 0)

    async def test_v1_upgrade_adds_leases_without_dropping_user_data(self):
        # A genuine legacy schema, not a newer database with a downgraded marker.
        with tempfile.TemporaryDirectory() as directory:
            path = os.path.join(directory, 'legacy.sqlite')
            legacy = sqlite3.connect(path)
            legacy.executescript(importlib.resources.files('llmproxy').joinpath('schema.sql').read_text())
            legacy.execute("INSERT INTO api_key(id,secret,type) VALUES('legacy','synthetic','LLM')")
            legacy.execute('PRAGMA user_version=1')
            legacy.commit()
            legacy.close()
            migrated = await SqliteDatabase.create('sqlite://' + path)
            try:
                version = await (await migrated.db.execute('PRAGMA user_version')).fetchone()
                self.assertEqual(version['user_version'], 6)
                self.assertEqual(len(await migrated.user_list('synthetic')), 1)
                self.assertEqual(await (await migrated.db.execute('SELECT * FROM admission_lease')).fetchall(), [])
            finally:
                await migrated.close()

    async def test_unknown_future_schema_fails_closed_without_downgrade(self):
        await self.db.db.execute("PRAGMA user_version=99")
        reopened = None
        try:
            with self.assertRaises(DatabaseError):
                reopened = await SqliteDatabase.create("sqlite://" + self.path)
        finally:
            if reopened is not None:
                await reopened.close()
        version = await (await self.db.db.execute("PRAGMA user_version")).fetchone()
        self.assertEqual(version["user_version"], 99)

    async def test_incompatible_lease_table_rolls_back_version_upgrade(self):
        await self.db.db.executescript("""
            DROP TABLE IF EXISTS admission_lease;
            CREATE TABLE admission_lease(unexpected TEXT);
            PRAGMA user_version=1;
        """)
        reopened = None
        try:
            with self.assertRaises(DatabaseError):
                reopened = await SqliteDatabase.create("sqlite://" + self.path)
        finally:
            if reopened is not None:
                await reopened.close()
        version = await (await self.db.db.execute("PRAGMA user_version")).fetchone()
        self.assertEqual(version["user_version"], 1)

    async def test_invalid_bounds_never_disable_a_lease(self):
        self.api()
        for limit in (True, 0, -1, 1.5):
            with self.subTest(limit=limit), self.assertRaises(ValueError):
                await self.db.admission_acquire("bucket", "owner", limit, 30)
        for ttl in (None, True, 0, -1, float("inf"), float("nan")):
            with self.subTest(ttl=ttl), self.assertRaises(ValueError):
                await self.db.admission_acquire("bucket", "owner", 1, ttl)
