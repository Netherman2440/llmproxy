"""Auth expiry through the real SQLite adapter, independent of wall-clock date."""
import datetime
import hashlib
import logging
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import aiohttp.web

from llmproxy import auth
from llmproxy.db import SqliteDatabase


NOW = datetime.datetime(2030, 5, 10, 12, 0, 0, 500000, tzinfo=datetime.UTC)


class FrozenDateTime(datetime.datetime):
    @classmethod
    def now(cls, tz=None):
        if tz is None:
            return cls(2030, 5, 10, 12, 0, 0, 500000)
        return cls(2030, 5, 10, 12, 0, 0, 500000,
                   tzinfo=datetime.UTC).astimezone(tz)


class App(dict):
    logger = logging.getLogger(__name__)


class Request(dict):
    def __init__(self, database, token, ttl):
        super().__init__(db=database)
        self.app = App(config={"db": {"uri": "sqlite://unused"},
                               "auth_cache_ttl": ttl})
        self.headers = {"Authorization": "Bearer " + token}


class TestAuthExpiry(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.directory = tempfile.TemporaryDirectory()
        path = Path(self.directory.name) / "auth.sqlite"
        self.database = await SqliteDatabase.create("sqlite://" + str(path))
        auth.flush_cache()

    async def asyncTearDown(self):
        auth.flush_cache()
        await self.database.close()
        self.directory.cleanup()

    async def add_key(self, token="test-credential", expires=None, suffix=""):
        digest = hashlib.sha256(token.encode()).hexdigest() + suffix
        return await self.database.user_create(digest, expires=expires)

    async def authenticate(self, token="test-credential", ttl=5):
        # auth and db share the stdlib datetime module; both see this clock.
        with patch("llmproxy.auth.datetime.datetime", FrozenDateTime):
            return await auth.require_auth(Request(self.database, token, ttl))

    async def test_same_day_expired_key_rejected_on_every_cache_miss(self):
        await self.add_key(expires="2030-05-10T11:59:59+00:00")
        for ttl in (0, 5):
            for _ in range(2):
                with self.subTest(ttl=ttl):
                    with self.assertRaises(aiohttp.web.HTTPUnauthorized):
                        await self.authenticate(ttl=ttl)

    async def test_offset_and_naive_expired_keys_are_rejected(self):
        for index, expires in enumerate(("2030-05-10T13:00:00+01:00",
                                          "2030-05-10T11:59:59",
                                          "2030-05-10T12:00:00.500000Z")):
            token = f"expired-fixture-{index}"
            await self.add_key(token, expires)
            with self.subTest(expires=expires):
                with self.assertRaises(aiohttp.web.HTTPUnauthorized):
                    await self.authenticate(token, ttl=0)

    async def test_malformed_expiry_never_grants_access(self):
        for index, expires in enumerate(("not-a-date", "NaN", "9999-invalid")):
            token = f"invalid-fixture-{index}"
            await self.add_key(token, expires)
            for ttl in (0, 5):
                with self.subTest(expires=expires, ttl=ttl):
                    with self.assertRaises(aiohttp.web.HTTPUnauthorized):
                        await self.authenticate(token, ttl)

    async def test_future_and_unlimited_keys_work_on_miss_and_hit(self):
        for index, expires in enumerate((None, "2030-05-10T12:00:01Z",
                                          "2030-05-10T13:00:01+01:00",
                                          "2030-05-10T12:00:00.600000Z",
                                          "2030-05-10T12:00:00.500001Z")):
            token = f"valid-fixture-{index}"
            identifier = await self.add_key(token, expires)
            for ttl in (0, 5):
                for _ in range(2):
                    with self.subTest(expires=expires, ttl=ttl):
                        row = await self.authenticate(token, ttl)
                        self.assertEqual(row["id"], identifier)

    async def test_cli_status_and_filter_agree_for_bad_and_expired_dates(self):
        fixtures = (("old", "2030-05-10T11:00:00Z", "expired"),
                    ("bad", "not-a-date", "expired"),
                    ("future", "2030-05-10T12:00:01Z", "active"),
                    ("unlimited", None, "active"))
        identifiers = {}
        for token, expires, expected in fixtures:
            identifiers[await self.add_key(token, expires)] = expected
        with patch("llmproxy.auth.datetime.datetime", FrozenDateTime):
            all_rows = await self.database.user_list(include_expired=True)
            active = await self.database.user_list()
        self.assertEqual({row["id"]: row["status"] for row in all_rows},
                         identifiers)
        self.assertEqual({row["id"] for row in active},
                         {identifier for identifier, status in identifiers.items()
                          if status == "active"})

    async def test_prefix_collision_is_not_an_authentication_match(self):
        await self.add_key(suffix="unexpected-suffix")
        with self.assertRaises(aiohttp.web.HTTPUnauthorized):
            await self.authenticate(ttl=0)

    async def test_empty_bearer_is_not_an_api_key(self):
        await self.add_key(token="")
        with self.assertRaises(aiohttp.web.HTTPUnauthorized):
            await self.authenticate(token="", ttl=0)


if __name__ == "__main__":
    unittest.main()
