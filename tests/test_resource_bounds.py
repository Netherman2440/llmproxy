import asyncio
import gzip
import inspect
import json
import logging
import types
import unittest
from unittest import mock

import aiohttp
import aiohttp.test_utils
import aiohttp.web

from llmproxy import app as application
from llmproxy import proxy, streaming

from .test_proxy import LLMProxyAppTestCase


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


class TestInputParserBounds(aiohttp.test_utils.AioHTTPTestCase):
    async def get_application(self):
        app = aiohttp.web.Application(client_max_size=128 * 1024,
            middlewares=[application.limit_request_body])
        app["config"] = {"max_json_body": 128, "max_multipart_fields": 4,
            "max_multipart_text_bytes": 64}

        async def parse(request):
            request["request_id"] = "synthetic-request-id"
            data = await proxy._read_body(request)
            upload = data.get("file")
            return aiohttp.web.json_response({"model": data.get("model"),
                "file_bytes": len(upload.file.read()) if upload is not None else 0})

        app.router.add_post("/v1/audio/transcriptions", parse)
        return app

    async def test_json_on_upload_route_still_has_the_json_byte_cap(self):
        async with self.client.post("/v1/audio/transcriptions",
                json={"model": "stt", "text": "x" * 256}) as response:
            self.assertEqual(response.status, 413)

    async def test_chunked_compressed_json_is_limited_by_decoded_bytes(self):
        compressed = gzip.compress(json.dumps({"model": "stt", "text": "x" * 4096}).encode())
        self.assertLess(len(compressed), 128)

        async def chunks():
            for offset in range(0, len(compressed), 7):
                yield compressed[offset:offset + 7]

        async with self.client.post("/v1/audio/transcriptions", data=chunks(),
                headers={"Content-Type": "application/json", "Content-Encoding": "gzip"}) as response:
            self.assertEqual(response.status, 413)

    async def test_multipart_text_part_has_a_separate_small_limit(self):
        form = aiohttp.FormData()
        form.add_field("model", "stt")
        form.add_field("prompt", "x" * 128)
        form.add_field("file", b"audio", filename="sample.pcm")
        async with self.client.post("/v1/audio/transcriptions", data=form) as response:
            self.assertEqual(response.status, 413)

    async def test_many_empty_multipart_fields_cannot_bypass_the_byte_cap(self):
        form = aiohttp.FormData()
        form.add_field("model", "stt")
        form.add_field("file", b"audio", filename="sample.pcm")
        for index in range(5):
            form.add_field("empty" + str(index), "")
        async with self.client.post("/v1/audio/transcriptions", data=form) as response:
            self.assertEqual(response.status, 413)

    async def test_multipart_header_count_is_bounded_before_field_buffering(self):
        payload = (b'--test-boundary\r\nContent-Disposition: form-data; name="model"\r\n'
            + b"X-Small: value\r\n" * 40 + b"\r\nstt\r\n--test-boundary--\r\n")
        async with self.client.post("/v1/audio/transcriptions", data=payload,
                headers={"Content-Type": "multipart/form-data; boundary=test-boundary"}) as response:
            self.assertEqual(response.status, 400)

    async def test_valid_file_is_not_restricted_to_the_text_field_limit(self):
        form = aiohttp.FormData()
        form.add_field("model", "stt")
        form.add_field("file", b"x" * 4096, filename="sample.pcm")
        async with self.client.post("/v1/audio/transcriptions", data=form) as response:
            self.assertEqual(response.status, 200)
            self.assertEqual(await response.json(), {"model": "stt", "file_bytes": 4096})

    async def test_oversized_text_is_rejected_before_the_sender_finishes(self):
        never = asyncio.Event()

        async def chunks():
            yield (b'--test-boundary\r\nContent-Disposition: form-data; name="prompt"\r\n\r\n'
                   + b"x" * 16384)
            await never.wait()

        try:
            async with asyncio.timeout(0.5):
                async with self.client.post("/v1/audio/transcriptions", data=chunks(),
                        headers={"Content-Type": "multipart/form-data; boundary=test-boundary"}) as response:
                    self.assertEqual(response.status, 413)
        except TimeoutError:
            self.fail("Multipart text is still buffered until the entire field arrives")


class TestResponseBounds(LLMProxyAppTestCase):
    async def get_application(self):
        app = await super().get_application()
        app["config"]["max_response_bytes"] = 64
        app["config"]["max_error_response_bytes"] = 32
        app["config"]["files"] = {"max_poll_response_bytes": 64}
        return app

    async def test_every_nonstream_frontend_rejects_oversized_backend_before_billing(self):
        cases = (
            ("/v1/chat/completions", {"model": "mymodel", "messages": [{"role": "user", "content": "hi"}]}),
            ("/v1/embeddings", {"model": "myembedding", "input": "hi"}),
            ("/v1/messages", {"model": "mymodel", "max_tokens": 10,
                "messages": [{"role": "user", "content": "hi"}]}),
            ("/v1/responses", {"model": "mymodel", "input": "hi"}),
            ("/v1/audio/transcriptions", "mywhisper"),
            ("/v1/files/convert", "mymarker"),
        )
        for path, payload in cases:
            with self.subTest(path=path):
                if isinstance(payload, str):
                    form = aiohttp.FormData()
                    form.add_field("model", payload)
                    form.add_field("file", b"synthetic-input", filename="sample.bin")
                    kwargs = {"data": form}
                else:
                    kwargs = {"json": payload}
                async with self.client.post(path,
                        headers={"Authorization": "Bearer mytoken"}, **kwargs) as response:
                    self.assertEqual(response.status, 502)
        self.assertEqual(await self.get_events(), [])

    async def test_chunked_nonstream_response_cannot_bypass_actual_byte_limit(self):
        async with self.client.post("/v1/chat/completions",
                headers={"Authorization": "Bearer mytoken"},
                json={"model": "mymodel", "_chunked_nonstream": True,
                    "messages": [{"role": "user", "content": "hi"}]}) as response:
            self.assertEqual(response.status, 502)
        self.assertEqual(await self.get_events(), [])

    async def test_backend_error_body_is_also_bounded(self):
        async with self.client.post("/v1/chat/completions",
                headers={"Authorization": "Bearer mytoken"},
                json={"model": "mymodel", "_trigger_error": 400}) as response:
            self.assertEqual(response.status, 502)
        self.assertEqual(await self.get_events(), [])


if __name__ == "__main__":
    unittest.main()
