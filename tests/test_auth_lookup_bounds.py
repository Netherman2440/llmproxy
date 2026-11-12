"""Auth work must be an exact, indexed, bounded lookup before any admission."""
import asyncio
import hashlib
import tempfile
import unittest
from unittest import mock

from llmproxy import config
from llmproxy.db import SqliteDatabase

from .test_proxy import LLMProxyAppTestCase


def digest(value):
    return hashlib.sha256(value.encode()).hexdigest()


class TestExactAuthLookup(unittest.IsolatedAsyncioTestCase):
    async def test_sqlite_user_get_is_exact_not_prefix(self):
        with tempfile.TemporaryDirectory() as directory:
            db = await SqliteDatabase.create('sqlite://' + directory + '/state.sqlite')
            try:
                short, long = digest('short'), digest('short-with-more')
                await db.user_create(short)
                await db.user_create(long)
                self.assertIsNone(await db.user_get(short[:16]),
                    'A prefix must not resolve another key')
                row = await db.user_get(short)
                self.assertEqual(row['id'], (await db.user_list(short))[0]['id'])
                self.assertEqual(row['secret'], short)
                self.assertIsNone(await db.user_get(digest('missing')))
            finally:
                await db.close()

    async def test_current_schema_carries_an_auth_index(self):
        with tempfile.TemporaryDirectory() as directory:
            db = await SqliteDatabase.create('sqlite://' + directory + '/state.sqlite')
            try:
                columns = await db.db.execute_fetchall('PRAGMA index_list(api_key)')
                indexes = [row['name'] for row in columns]
                self.assertTrue(any('api_key_type_secret' == name for name in indexes),
                    'No auth lookup index was created')
                version = (await db.db.execute_fetchall('PRAGMA user_version'))[0]['user_version']
                self.assertEqual(version, 6)
            finally:
                await db.close()


class TestBoundedAuthWork(LLMProxyAppTestCase):
    async def get_application(self):
        app = await super().get_application()
        app['config'].update(auth_lookup_concurrency=1, auth_lookup_timeout=2.0)
        app['auth_lookup_slots'] = asyncio.Semaphore(1)
        return app

    async def fire(self, tokens):
        return await asyncio.gather(*(self.client.get('/v1/models',
            headers={'Authorization': 'Bearer ' + token}) for token in tokens))

    async def test_distinct_auth_misses_share_one_bounded_lookup_slot(self):
        observed, active = [], 0
        original = SqliteDatabase.user_get

        async def counted(self, secret_hash):
            nonlocal active
            active += 1
            observed.append(active)
            await asyncio.sleep(0.05)
            active -= 1
            return await original(self, secret_hash)

        with mock.patch.object(SqliteDatabase, 'user_get', counted):
            responses = await self.fire(['distinct-invalid-%d' % index for index in range(4)])
        self.assertEqual(observed, [1] * 4, 'Concurrent auth lookups exceeded the configured bound')
        for response in responses:
            self.assertEqual(response.status, 401)
            await response.read()

    async def test_saturated_auth_slot_fails_closed_within_the_deadline(self):
        self.app['config'].update(auth_lookup_timeout=0.35, auth_lookup_concurrency=1)
        self.app['auth_lookup_slots'] = asyncio.Semaphore(1)

        async def slow(self, secret_hash):
            await asyncio.sleep(0.2)
            return None

        with mock.patch.object(SqliteDatabase, 'user_get', slow):
            first, second = await self.fire(['slow-invalid-a', 'slow-invalid-b'])
        self.assertEqual(sorted((first.status, second.status)), [401, 503],
            'Exactly one lookup fits the single slot; the queued one fails closed')
        for response in (first, second):
            await response.read()


class TestAuthLookupWiring(LLMProxyAppTestCase):
    async def test_authentication_resolves_through_the_exact_lookup(self):
        calls = []
        original = SqliteDatabase.user_get

        async def observed(self, secret_hash):
            calls.append(secret_hash)
            return await original(self, secret_hash)

        with mock.patch.object(SqliteDatabase, 'user_get', observed):
            response = await self.client.get('/v1/models',
                headers={'Authorization': 'Bearer mytoken'})
            self.assertEqual(response.status, 200)
            await response.read()
        self.assertEqual(calls, [digest('mytoken')], 'Auth must use the exact user_get lookup')

    async def test_prefix_credential_cannot_authenticate_as_a_longer_key(self):
        secret = digest('long-credential-value')
        db = await SqliteDatabase.create('sqlite://' + self.db_path)
        try:
            await db.user_create(secret)
        finally:
            await db.close()
        response = await self.client.get('/v1/models',
            headers={'Authorization': 'Bearer ' + 'x' * 63})
        self.assertEqual(response.status, 401)
        await response.read()


class TestAuthLookupConfig(unittest.TestCase):
    def test_lookup_bounds_are_validated(self):
        config.validate({'auth_lookup_concurrency': 8, 'auth_lookup_timeout': 5})
        for options in ({'auth_lookup_concurrency': 0}, {'auth_lookup_concurrency': True},
                {'auth_lookup_concurrency': 1.5}, {'auth_lookup_concurrency': 5000},
                {'auth_lookup_timeout': 0}, {'auth_lookup_timeout': None},
                {'auth_lookup_timeout': float('inf')}, {'auth_lookup_timeout': 61}):
            with self.subTest(options=options):
                with self.assertRaises(config.ConfigError):
                    config.validate(options)
