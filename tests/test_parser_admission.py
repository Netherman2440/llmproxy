import asyncio
import contextlib
from unittest import mock

from llmproxy import proxy
from llmproxy.app import reload_config

from .test_proxy import LLMProxyAppTestCase


class TestParserAdmission(LLMProxyAppTestCase):
    async def get_application(self):
        app = await super().get_application()
        app["config"]["parsing"] = {"concurrency": 2, "per_user": 1, "timeout": 2}
        return app

    async def send(self, token="mytoken", probe="rejected"):
        return await self.client.post("/v1/chat/completions",
            headers={"Authorization": "Bearer " + token, "X-Probe": probe},
            json={"model": "mymodel", "messages": [{"role": "user", "content": "hi"}]})

    @contextlib.asynccontextmanager
    async def blocked_parser(self):
        entered, release, second_parsed = asyncio.Event(), asyncio.Event(), asyncio.Event()
        original = proxy._read_body

        async def observed(request):
            if request.headers.get("X-Probe") == "first":
                entered.set()
            elif request.headers.get("X-Probe") == "rejected":
                second_parsed.set()
            return await original(request)

        async def body():
            yield b'{"model":"mymodel","messages":['
            await release.wait()
            yield b'{"role":"user","content":"hi"}]}'

        with mock.patch("llmproxy.proxy._read_body", observed):
            first = asyncio.create_task(self.client.post("/v1/chat/completions",
                headers={"Authorization": "Bearer mytoken", "X-Probe": "first",
                    "Content-Type": "application/json"}, data=body()))
            try:
                await asyncio.wait_for(entered.wait(), 1)
                yield second_parsed
            finally:
                release.set()
                response = await asyncio.wait_for(first, 1)
                self.assertEqual(response.status, 200)
                response.release()

    async def test_per_user_cap_rejects_before_reading_but_another_user_can_parse(self):
        await self.insert_key("othertoken", "other-user")
        async with self.blocked_parser() as second_parsed:
            response = await self.send()
            self.assertEqual(response.status, 429)
            response.release()
            self.assertFalse(second_parsed.is_set(), "Rejected request entered the expensive parser")
            response = await self.send("othertoken", probe="allowed")
            self.assertEqual(response.status, 200)
            response.release()
        self.assertEqual(len(await self.get_events()), 4)

    async def test_global_cap_rejects_another_user_before_reading(self):
        await self.insert_key("othertoken", "other-user")
        self.app["config"]["parsing"]["concurrency"] = 1
        async with self.blocked_parser() as second_parsed:
            response = await self.send("othertoken")
            self.assertEqual(response.status, 429)
            response.release()
            self.assertFalse(second_parsed.is_set(), "Global parser cap is checked too late")
        self.assertEqual(len(await self.get_events()), 2)

    async def test_reload_changes_policy_without_forgetting_the_active_parser_owner(self):
        await self.insert_key("othertoken", "other-user")
        self.app["config"]["_path"] = "synthetic-config.toml"
        replacement = dict(self.app["config"], parsing={"concurrency": 1, "per_user": 1, "timeout": 2})
        async with self.blocked_parser() as second_parsed:
            with mock.patch("llmproxy.app.config.load", return_value=replacement):
                reload_config(self.app)
            response = await self.send("othertoken")
            self.assertEqual(response.status, 429)
            response.release()
            self.assertFalse(second_parsed.is_set())
        response = await self.send(probe="allowed")
        self.assertEqual(response.status, 200)
        response.release()
        self.assertEqual(len(await self.get_events()), 4)

    async def test_unfinished_body_times_out_and_releases_admission(self):
        self.app["config"]["parsing"] = {"concurrency": 1, "per_user": 1, "timeout": 0.02}
        never = asyncio.Event()

        async def body():
            yield b'{"model":"mymodel","messages":['
            await never.wait()

        try:
            async with asyncio.timeout(1):
                async with self.client.post("/v1/chat/completions", data=body(),
                        headers={"Authorization": "Bearer mytoken", "Content-Type": "application/json"}) as response:
                    self.assertEqual(response.status, 408)
        except TimeoutError:
            self.fail("Unfinished request body has no parser deadline")
        response = await self.send(probe="allowed")
        self.assertEqual(response.status, 200)
        response.release()
        self.assertEqual(len(await self.get_events()), 2)
