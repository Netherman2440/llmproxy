import asyncio
import copy
import contextlib
import multiprocessing
from unittest import mock

import aiohttp
import aiohttp.test_utils

from llmproxy.app import create_app, reload_config
from llmproxy.db import SqliteDatabase, get_db

from . import mockbackend
from . import test_parser_admission as parser_tests
from .test_proxy import LLMProxyAppTestCase


def _peer_process(config, ready, stop):
    async def run():
        app = await create_app(config)
        runner = aiohttp.web.AppRunner(app, handler_cancellation=False, access_log=None)
        await runner.setup()
        site = aiohttp.web.TCPSite(runner, "127.0.0.1", 0)
        await site.start()
        ready.put(runner.addresses[0][1])
        await asyncio.to_thread(stop.wait, 10)
        await runner.cleanup()
    try:
        asyncio.run(run())
    except Exception as error:
        ready.put({"exception_type": type(error).__name__})
        raise SystemExit(1)


class TestSharedParserAdmission(LLMProxyAppTestCase):
    send = parser_tests.TestParserAdmission.send
    blocked_parser = parser_tests.TestParserAdmission.blocked_parser

    async def get_application(self):
        app = await super().get_application()
        app["config"]["admission"] = {"mode": "shared", "scope": "audit-test",
            "lease_ttl": 1, "io_timeout": 0.05}
        app["config"]["parsing"] = {"concurrency": 1, "per_user": 1, "timeout": 2}
        return app

    async def peer(self):
        config = copy.deepcopy(self.app["config"])
        app = await create_app(config)
        client = aiohttp.test_utils.TestClient(aiohttp.test_utils.TestServer(app))
        await client.start_server()
        self.addAsyncCleanup(client.close)
        return client

    @contextlib.asynccontextmanager
    async def remote_peer(self):
        ctx = multiprocessing.get_context("spawn")
        ready, stop = ctx.Queue(), ctx.Event()
        process = ctx.Process(target=_peer_process, args=(copy.deepcopy(self.app["config"]), ready, stop))
        client = None
        try:
            process.start()
            port = await asyncio.to_thread(ready.get, True, 5)
            self.assertIsInstance(port, int, "Peer process startup failed")
            client = aiohttp.ClientSession(base_url=f"http://127.0.0.1:{port}")
            yield client
        finally:
            if client is not None:
                await client.close()
            stop.set()
            await asyncio.to_thread(process.join, 5)
            if process.is_alive():
                process.terminate()
                await asyncio.to_thread(process.join, 5)
            ready.close()
        self.assertEqual(process.exitcode, 0)

    @contextlib.asynccontextmanager
    async def backend_busy(self):
        entered, release = asyncio.Event(), asyncio.Event()
        original = mockbackend._chat_stream

        async def blocked(request, message, mode):
            entered.set()
            await release.wait()
            return await original(request, message, mode)

        with mock.patch("tests.mockbackend._chat_stream", blocked):
            pending = asyncio.create_task(self.client.post("/v1/chat/completions",
                headers={"Authorization": "Bearer mytoken"}, json={"model": "mymodel", "stream": True,
                    "messages": [{"role": "user", "content": "hi"}]}))
            try:
                await asyncio.wait_for(entered.wait(), 1)
                yield
            finally:
                release.set()
                response = await asyncio.wait_for(pending, 1)
                self.assertEqual(response.status, 200)
                await response.read()
                response.release()

    async def test_two_processes_preserve_parser_cap_across_reload(self):
        async with self.remote_peer() as peer:
            async with self.blocked_parser():
                self.app["config"]["_path"] = "synthetic-config.toml"
                with mock.patch("llmproxy.app.config.load", return_value=copy.deepcopy(self.app["config"])):
                    reload_config(self.app)
                async with peer.post("/v1/chat/completions", headers={"Authorization": "Bearer mytoken"},
                        json={"model": "mymodel", "messages": [{"role": "user", "content": "hi"}]}) as response:
                    self.assertEqual(response.status, 429)
            async with peer.post("/v1/chat/completions", headers={"Authorization": "Bearer mytoken"},
                    json={"model": "mymodel", "messages": [{"role": "user", "content": "hi"}]}) as response:
                self.assertEqual(response.status, 200)
        self.assertEqual(len(await self.get_events()), 4)

    async def test_model_concurrency_is_shared_between_real_http_processes(self):
        self.app["config"]["rate_limit"] = {"concurrency": 1}
        async with self.remote_peer() as peer:
            async with self.backend_busy():
                self.app["config"]["_path"] = "synthetic-config.toml"
                with mock.patch("llmproxy.app.config.load", return_value=copy.deepcopy(self.app["config"])):
                    reload_config(self.app)
                async with peer.post("/v1/chat/completions", headers={"Authorization": "Bearer mytoken"},
                        json={"model": "mymodel", "messages": [{"role": "user", "content": "hi"}]}) as response:
                    self.assertEqual(response.status, 429)
        self.assertEqual(len(await self.get_events(expect=2)), 2)

    async def test_rpm_window_is_shared_between_real_http_processes(self):
        self.app["config"]["rate_limit"] = {"rpm": 1}
        async with self.remote_peer() as peer:
            warm = await self.send(probe="allowed")
            self.assertEqual(warm.status, 200)
            warm.release()
            async with peer.post("/v1/chat/completions", headers={"Authorization": "Bearer mytoken"},
                    json={"model": "mymodel", "messages": [{"role": "user", "content": "hi"}]}) as response:
                self.assertEqual(response.status, 429)
                self.assertGreater(int(response.headers["Retry-After"]), 0)
        self.assertEqual(len(await self.get_events()), 2)

    async def test_independent_application_cannot_bypass_global_parser_lease(self):
        peer = await self.peer()
        async with self.blocked_parser():
            async with peer.post("/v1/chat/completions",
                    headers={"Authorization": "Bearer mytoken"},
                    json={"model": "mymodel", "messages": [{"role": "user", "content": "hi"}]}) as response:
                self.assertEqual(response.status, 429)
        async with peer.post("/v1/chat/completions",
                headers={"Authorization": "Bearer mytoken"},
                json={"model": "mymodel", "messages": [{"role": "user", "content": "hi"}]}) as response:
            self.assertEqual(response.status, 200)
        self.assertEqual(len(await self.get_events()), 4)

    async def test_active_body_renews_its_shared_lease_past_the_original_expiry(self):
        self.app["config"]["admission"].update(lease_ttl=0.4, io_timeout=0.03)
        peer = await self.peer()
        observer = await get_db(self.app["config"]["db"]["uri"])
        try:
            async with self.blocked_parser():
                async def expiry():
                    row = await (await observer.db.execute(
                        "SELECT min(expires_at) AS expiry FROM admission_lease")).fetchone()
                    return row["expiry"]
                initial = await expiry()
                self.assertIsNotNone(initial, "Shared profile did not acquire durable leases")
                async with asyncio.timeout(1):
                    while (await expiry()) < initial + 0.4:
                        await asyncio.sleep(0.005)
                async with peer.post("/v1/chat/completions",
                        headers={"Authorization": "Bearer mytoken"},
                        json={"model": "mymodel", "messages": [{"role": "user", "content": "hi"}]}) as response:
                    self.assertEqual(response.status, 429)
        finally:
            await observer.close()

    async def test_transient_renewal_timeout_does_not_discard_still_confirmed_ownership(self):
        self.app['config']['admission'].update(lease_ttl=0.4, io_timeout=0.03)
        original = SqliteDatabase.admission_renew
        calls = 0

        async def delayed_once(database, *args):
            nonlocal calls
            calls += 1
            if calls == 1:
                await asyncio.sleep(0.06)  # exceeds unchanged30ms I/O, not lease TTL
            return await original(database, *args)

        observer = await get_db(self.app['config']['db']['uri'])
        try:
            with mock.patch.object(SqliteDatabase, 'admission_renew', delayed_once):
                async with self.blocked_parser():
                    initial = (await observer.db.execute_fetchall('SELECT min(expires_at) AS expiry FROM admission_lease'))[0]['expiry']
                    self.assertIsNotNone(initial)
                    async with asyncio.timeout(1):
                        while True:
                            expiry = (await observer.db.execute_fetchall('SELECT min(expires_at) AS expiry FROM admission_lease'))[0]['expiry']
                            self.assertIsNotNone(expiry, 'Transient renewal discarded unexpired confirmed ownership')
                            if expiry > initial + 0.4:
                                break
                            await asyncio.sleep(0.005)
            self.assertGreater(calls, 2)
        finally:
            await observer.close()

    async def test_repeated_renewal_timeouts_cannot_extend_unconfirmed_ownership(self):
        self.app['config']['admission'].update(lease_ttl=0.4, io_timeout=0.03)
        never = asyncio.Event()
        calls = 0

        async def unavailable(*args):
            nonlocal calls
            calls += 1
            await asyncio.sleep(0.06)
            self.fail('Timed-out renewal unexpectedly completed')

        async def body():
            yield b'{"model":"mymodel","messages":['
            await never.wait()

        with mock.patch.object(SqliteDatabase, 'admission_renew', unavailable):
            async with asyncio.timeout(1):
                response = await self.client.post('/v1/chat/completions', data=body(),
                    headers={'Authorization': 'Bearer mytoken', 'Content-Type': 'application/json'})
                self.assertEqual(response.status, 503)
                response.release()
        self.assertLessEqual(calls, 10, 'Unbounded renewal retry')
        self.assertEqual(await self.get_events(), [])

    async def test_lost_ownership_aborts_body_and_never_releases_the_replacement(self):
        self.app["config"]["admission"].update(lease_ttl=0.4, io_timeout=0.03)
        observer = await get_db(self.app["config"]["db"]["uri"])
        never = asyncio.Event()

        async def body():
            yield b'{"model":"mymodel","messages":['
            await never.wait()

        pending = asyncio.create_task(self.client.post("/v1/chat/completions", data=body(),
            headers={"Authorization": "Bearer mytoken", "Content-Type": "application/json"}))
        try:
            try:
                async with asyncio.timeout(0.5):
                    while True:
                        rows = await (await observer.db.execute("SELECT bucket FROM admission_lease")).fetchall()
                        if len(rows) == 2:
                            break
                        await asyncio.sleep(0.005)
            except TimeoutError:
                self.fail("Shared parser acquired no durable lease")
            await observer.db.execute("DELETE FROM admission_lease")
            await observer.db.commit()
            for row in rows:
                self.assertTrue(await observer.admission_acquire(row["bucket"], "replacement", 1, 30))
            async with asyncio.timeout(1):
                response = await pending
                self.assertEqual(response.status, 503)
                response.release()
            owners = await (await observer.db.execute("SELECT owner FROM admission_lease")).fetchall()
            self.assertEqual([row["owner"] for row in owners], ["replacement", "replacement"])
        finally:
            pending.cancel()
            await asyncio.gather(pending, return_exceptions=True)
            await observer.close()

    async def test_explicit_shared_profile_cannot_silently_fall_back_on_database_failure(self):
        warm = await self.send(probe="allowed")
        self.assertEqual(warm.status, 200)
        warm.release()
        original = self.app["config"]["db"]["uri"]
        self.app["config"]["db"]["uri"] = "sqlite:///path-that-does-not-exist/admission.sqlite"
        try:
            async with asyncio.timeout(1):
                response = await self.send(probe="allowed")
                self.assertEqual(response.status, 503)
                response.release()
        except TimeoutError:
            self.fail("Shared admission database failure was not bounded")
        finally:
            self.app["config"]["db"]["uri"] = original
