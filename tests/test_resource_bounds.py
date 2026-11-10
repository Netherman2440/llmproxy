import asyncio
import inspect
import logging
import types
import unittest
from unittest import mock

import aiohttp
import aiohttp.test_utils
import aiohttp.web

from llmproxy import streaming


class TestBoundedSSE(aiohttp.test_utils.AioHTTPTestCase):
    async def get_application(self):
        app = aiohttp.web.Application()

        async def long_frame(req):
            return aiohttp.web.Response(body=b"synthetic-private-fragment:" + b"x" * 10000 + b"\n\n")

        async def split_frames(req):
            response = aiohttp.web.StreamResponse()
            response.enable_chunked_encoding()
            await response.prepare(req)
            for part in (b"first\n", b"\nsecond", b"\n", b"\nlast"):
                await response.write(part)
            return response

        app.router.add_get("/long", long_frame)
        app.router.add_get("/split", split_frames)
        return app

    async def bounded(self, stream, separator=b"\n\n", *, limit=64):
        self.assertIn("max_bytes", inspect.signature(streaming.readuntil).parameters,
                      "SSE reader has no byte budget")
        return await streaming.readuntil(stream, separator, max_bytes=limit)

    async def test_oversized_frame_is_rejected_without_echoing_upstream_text(self):
        async with self.client.get("/long") as response:
            with self.assertRaises(ValueError) as error:
                await self.bounded(response.content, limit=64)
        self.assertNotIn("synthetic-private-fragment", str(error.exception))

    async def test_delimiter_and_unconsumed_tail_survive_chunk_boundaries(self):
        async with self.client.get("/split") as response:
            self.assertEqual(await self.bounded(response.content), b"first\n\n")
            self.assertEqual(await self.bounded(response.content), b"second\n\n")
            self.assertEqual(await self.bounded(response.content), b"last")

    async def test_reader_does_not_perform_one_await_per_byte(self):
        calls = 0
        original = aiohttp.StreamReader.read

        async def counted(reader, n=-1):
            nonlocal calls
            calls += 1
            return await original(reader, n)

        with mock.patch.object(aiohttp.StreamReader, "read", new=counted):
            async with self.client.get("/long") as response:
                raw = await streaming.readuntil(response.content, b"\n\n")
                self.assertTrue(raw.endswith(b"\n\n"))
        self.assertLess(calls, 10, "SSE parsing is still bytewise")

    async def test_invalid_budget_does_not_disable_the_frame_limit(self):
        async with self.client.get("/long") as response:
            for value in (0, -1, True, float("inf")):
                with self.subTest(limit=value), self.assertRaises(ValueError):
                    await self.bounded(response.content, limit=value)

    async def test_empty_separator_is_rejected(self):
        async with self.client.get("/long") as response:
            with self.assertRaises(ValueError):
                await streaming.readuntil(response.content, b"")


class TestBoundedDrain(unittest.IsolatedAsyncioTestCase):
    async def test_slow_writer_times_out_but_final_usage_is_still_drained(self):
        self.assertIn("write_timeout", inspect.signature(streaming.drain).parameters)
        chunks = iter((b"partial", b"final-usage", b"done", b""))
        seen, disconnects, writes = [], [], []
        blocked = asyncio.Event()

        async def read():
            return next(chunks)

        async def write(data):
            writes.append(data)
            await blocked.wait()

        async with asyncio.timeout(1):
            await streaming.drain(read, write, seen.append, disconnects.append, write_timeout=0.02)
        self.assertEqual(seen, [b"partial", b"final-usage", b"done"])
        self.assertEqual(writes, [b"partial"])
        self.assertEqual(len(disconnects), 1)
        self.assertIsInstance(disconnects[0], TimeoutError)

    async def test_invalid_write_deadline_is_rejected(self):
        self.assertIn("write_timeout", inspect.signature(streaming.drain).parameters)

        async def eof():
            return b""

        for value in (None, True, 0, -1, float("inf"), float("nan")):
            with self.subTest(timeout=value), self.assertRaises(ValueError):
                await streaming.drain(eof, eof, lambda chunk: None, write_timeout=value)

    async def test_explicit_cancellation_is_not_reclassified_as_disconnect(self):
        async def read():
            return b"partial"

        async def write(data):
            raise asyncio.CancelledError()

        with self.assertRaises(asyncio.CancelledError):
            await streaming.drain(read, write, lambda chunk: None)


class TestBoundedStreamResponse(unittest.IsolatedAsyncioTestCase):
    async def exercise(self, blocked_prepare=False, blocked_eof=False, blocked_write=False):
        class App(dict):
            logger = logging.getLogger("test.stream.deadline")

        class Request(dict):
            app = App(config={"write_timeout": 0.02})

        chunks = iter((b"data: partial\n\n", b"data: final-usage\n\n", b""))
        seen, writes, aborts = [], [], []
        never = asyncio.Event()

        async def readuntil(separator, *, max_size):
            return next(chunks)

        async def prepare(response, request):
            if blocked_prepare:
                await never.wait()

        async def write(response, data):
            writes.append(data)
            if blocked_write:
                await never.wait()

        async def eof(response):
            if blocked_eof:
                await never.wait()

        request = Request(request_id="synthetic-request-id")
        request.transport = types.SimpleNamespace(abort=lambda: aborts.append(True))
        backend = types.SimpleNamespace(headers={"Content-Type": "text/event-stream"},
            content=types.SimpleNamespace(readuntil=readuntil))
        with mock.patch.object(aiohttp.web.StreamResponse, "prepare", new=prepare), \
                mock.patch.object(aiohttp.web.StreamResponse, "write", new=write), \
                mock.patch.object(aiohttp.web.StreamResponse, "write_eof", new=eof):
            try:
                await asyncio.wait_for(streaming.stream_through(request, backend, seen.append), timeout=0.5)
            except TimeoutError:
                self.fail("Streaming response prepare/EOF has no bounded deadline")
        return seen, writes, aborts

    async def test_slow_prepare_preserves_backend_usage_without_waiting_forever(self):
        seen, writes, aborts = await self.exercise(blocked_prepare=True)
        self.assertEqual(seen, [b"data: partial\n\n", b"data: final-usage\n\n"])
        self.assertEqual(writes, [])
        self.assertEqual(aborts, [True])

    async def test_slow_eof_cannot_pin_completed_stream_handler(self):
        seen, writes, aborts = await self.exercise(blocked_eof=True)
        self.assertEqual(seen, [b"data: partial\n\n", b"data: final-usage\n\n"])
        self.assertEqual(writes, seen)
        self.assertEqual(aborts, [True])

    async def test_slow_body_writer_uses_configured_deadline_and_preserves_usage(self):
        seen, writes, aborts = await self.exercise(blocked_write=True)
        self.assertEqual(seen, [b"data: partial\n\n", b"data: final-usage\n\n"])
        self.assertEqual(writes, [b"data: partial\n\n"])
        self.assertEqual(aborts, [True])


if __name__ == "__main__":
    unittest.main()
