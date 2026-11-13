import unittest

from llmproxy import streaming
from llmproxy.chat import _ChatStreamUsage


class FragmentReader:
    """Byte source with controlled transport fragment size, no framing logic."""
    def __init__(self, data, width):
        self.data, self.width, self.offset = data, width, 0

    async def read(self, size):
        chunk = self.data[self.offset:self.offset + min(size, self.width)]
        self.offset += len(chunk)
        return chunk


class TestSseFraming(unittest.IsolatedAsyncioTestCase):
    async def frames(self, data, width, limit=2048):
        function = getattr(streaming, 'sse_blocks', None)
        self.assertIsNotNone(function, 'Bounded SSE line-ending reader missing')
        return [frame async for frame in function(FragmentReader(data, width), max_bytes=limit)]

    async def test_legal_line_endings_preserve_bytes_at_every_small_fragment_boundary(self):
        for ending in (b'\n', b'\r\n', b'\r'):
            frames = [b'data: first' + ending * 2, b'data: second' + ending * 2]
            for width in range(1, 8):
                with self.subTest(ending=ending, width=width):
                    self.assertEqual(await self.frames(b''.join(frames), width), frames)
        mixed = [b'data: first\r\n\r', b'data: second\n\r\n']
        for width in range(1, 8):
            with self.subTest(mixed_width=width):
                self.assertEqual(await self.frames(b''.join(mixed), width), mixed)

    async def test_frame_budget_applies_to_one_event_not_total_transport_chunk(self):
        frame = b'data: x\r\n\r\n'
        self.assertEqual(await self.frames(frame * 20, 8192, len(frame)), [frame] * 20)
        for data in (b'data: ' + b'x' * 100 + b'\n\n', b'x' * 100):
            with self.subTest(delimited=data.startswith(b'data:')):
                with self.assertRaises(ValueError):
                    await self.frames(data, 5, 32)

    async def test_eof_partial_is_retained_without_hiding_an_oversized_frame(self):
        self.assertEqual(await self.frames(b'data: final', 1), [b'data: final'])
        self.assertEqual(await self.frames(b'', 1), [])

    def test_usage_accumulator_ignores_keepalives_and_done_for_every_line_ending(self):
        for ending in (b'\n', b'\r\n', b'\r'):
            with self.subTest(ending=ending):
                usage = _ChatStreamUsage()
                usage(b'data: {"choices":[],"usage":{"prompt_tokens":1,"completion_tokens":2}}' + ending * 2)
                usage(b': keepalive' + ending * 2)
                usage(b'data: [DONE]' + ending * 2)
                usage(ending)
                self.assertEqual(usage.usage(), {'prompt_tokens': 1, 'completion_tokens': 2})

    def test_multiline_data_usage_is_parsed_as_one_event(self):
        usage = _ChatStreamUsage()
        usage(b'event: chunk\r\ndata: {"choices":[],\r\ndata: "usage":{"prompt_tokens":1,"completion_tokens":2}}\r\n\r\n')
        self.assertEqual(usage.usage(), {'prompt_tokens': 1, 'completion_tokens': 2})
