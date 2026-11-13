import asyncio
import importlib.resources
import os
import sqlite3
import tempfile
import unittest
from unittest.mock import patch

import aiosqlite

from llmproxy.db import SqliteDatabase


def _user_version(path):
    conn = sqlite3.connect(path)
    try:
        return conn.execute("PRAGMA user_version").fetchone()[0]
    finally:
        conn.close()


def _schema_sql():
    return importlib.resources.files("llmproxy") \
        .joinpath("schema.sql").read_text()


class TestSqliteInit(unittest.IsolatedAsyncioTestCase):
    """SqliteDatabase.create() must initialize idempotently.

    The schema is applied on a version-0 database, but a database can already
    have the tables while its user_version is still 0 -- exactly what the
    documented `sqlite3 db.sqlite < schema.sql` setup produces (the sqlite CLI
    does not stamp user_version). Re-applying the schema must not crash, so the
    schema is idempotent (CREATE TABLE IF NOT EXISTS) and the initializer heals
    the version marker through the supported migration ladder.
    """

    def setUp(self):
        fd, self.path = tempfile.mkstemp()
        os.close(fd)

    def tearDown(self):
        os.unlink(self.path)

    async def test_cancelled_schema_read_cannot_hold_a_lock_through_its_retained_result(self):
        initialized = await SqliteDatabase.create('sqlite://' + self.path)
        await initialized.close()
        entered, suspend = asyncio.Event(), asyncio.Event()
        retained = []
        original = aiosqlite.Connection._execute
        async def pause_after_metadata_read(connection, function, *args, **kwargs):
            result = await original(connection, function, *args, **kwargs)
            if args and args[0] == 'PRAGMA user_version':
                # A cancelled await/traceback can retain its last result even
                # after Connection.close(). Never let that result be a cursor.
                retained.append(result)
                entered.set()
                await suspend.wait()
            return result
        try:
            with patch.object(aiosqlite.Connection, '_execute', pause_after_metadata_read):
                pending = asyncio.create_task(SqliteDatabase.create('sqlite://' + self.path))
                await asyncio.wait_for(entered.wait(), 2)
                pending.cancel()
                with self.assertRaises(asyncio.CancelledError):
                    await pending
            def write_after_cancellation():
                writer = sqlite3.connect(self.path, timeout=0.1)
                try:
                    writer.execute("INSERT INTO api_key(id,secret,type) VALUES('after-cancel','synthetic-digest','LLM')")
                    writer.commit()
                finally:
                    writer.close()
            await asyncio.to_thread(write_after_cancellation)
        finally:
            for result in retained:
                if isinstance(result, sqlite3.Cursor):
                    try:
                        result.close()
                    except sqlite3.Error:
                        pass

    async def test_empty_db_is_initialized_to_the_current_version(self):
        self.assertEqual(_user_version(self.path), 0)  # precondition

        db = await SqliteDatabase.create("sqlite://%s" % self.path)
        try:
            # Schema is usable (query does not raise on a missing table).
            self.assertEqual(await db.user_list("deadbeef"), [])
        finally:
            await db.close()

        self.assertEqual(_user_version(self.path), 6)

    async def test_preexisting_schema_does_not_crash(self):
        # Simulate `sqlite3 db.sqlite < schema.sql`: tables exist, version 0.
        conn = sqlite3.connect(self.path)
        conn.executescript(_schema_sql())
        conn.close()
        self.assertEqual(_user_version(self.path), 0)  # precondition

        db = await SqliteDatabase.create("sqlite://%s" % self.path)
        try:
            self.assertEqual(await db.user_list("deadbeef"), [])
        finally:
            await db.close()

        # Version healed so subsequent boots skip re-initialization.
        self.assertEqual(_user_version(self.path), 6)

    async def test_reopen_at_the_current_version_preserves_data(self):
        # First create() migrates to v5; reopening validates that schema
        # without dropping or replacing any data.
        db = await SqliteDatabase.create("sqlite://%s" % self.path)
        await db.db.execute(
            "INSERT INTO api_key (id, secret, type) VALUES ('u', 'abc', 'LLM')")
        await db.db.commit()
        await db.close()
        self.assertEqual(_user_version(self.path), 6)

        db = await SqliteDatabase.create("sqlite://%s" % self.path)
        try:
            rows = await db.user_list("abc")
        finally:
            await db.close()

        self.assertEqual(len(rows), 1)

    async def test_partial_schema_is_healed(self):
        # Only one of the two tables exists at version 0 (e.g. an interrupted
        # manual setup). create() must add the missing table, not crash.
        conn = sqlite3.connect(self.path)
        conn.execute(
            "CREATE TABLE event_oneoff (id INTEGER PRIMARY KEY, created TEXT, "
            "api_key TEXT, product TEXT, quantity INTEGER, rid TEXT)")
        conn.close()
        self.assertEqual(_user_version(self.path), 0)

        db = await SqliteDatabase.create("sqlite://%s" % self.path)
        try:
            # The previously-missing api_key table is now queryable.
            self.assertEqual(await db.user_list("deadbeef"), [])
        finally:
            await db.close()

        self.assertEqual(_user_version(self.path), 6)

    async def test_preexisting_data_is_preserved(self):
        # Healing an un-versioned database must never drop existing rows.
        conn = sqlite3.connect(self.path)
        conn.executescript(_schema_sql())
        conn.execute(
            "INSERT INTO api_key (id, secret, type) VALUES ('u', 'abc', 'LLM')")
        conn.commit()
        conn.close()

        db = await SqliteDatabase.create("sqlite://%s" % self.path)
        try:
            rows = await db.user_list("abc")
        finally:
            await db.close()

        self.assertEqual(len(rows), 1)
        self.assertEqual(_user_version(self.path), 6)


if __name__ == "__main__":
    unittest.main()
