import asyncio
import decimal
import json
import math
import re

import aiohttp.web
from aiohttp.http_exceptions import LineTooLong

from .budget_headers import EXPOSE_HEADERS


SSE_FRAME_MAX_BYTES = 2 * 1024 * 1024


async def readuntil(stream, separator: bytes = b"\n", *,
                    max_bytes: int = SSE_FRAME_MAX_BYTES) -> bytes:
    """Use the pinned aiohttp chunk-aware delimiter reader with a frame budget.

    aiohttp >=3.14 preserves separators split across buffers; the old one-byte
    workaround is no longer necessary. Never expose LineTooLong's raw prefix.
    """
    if not isinstance(separator, bytes) or not separator:
        raise ValueError("A nonempty byte separator is required")
    if type(max_bytes) is not int or max_bytes <= 0:
        raise ValueError("A positive integer frame budget is required")
    try:
        return await stream.readuntil(separator, max_size=max_bytes)
    except LineTooLong:
        raise ValueError("Backend stream frame exceeds the byte limit") from None


# Atomic CRLF prevents regex backtracking from counting one CRLF as two
# separate line endings. Each SSE empty line consists of two line endings.
_SSE_END = re.compile(rb'(?>\r\n|\r|\n){2}')


async def sse_blocks(stream, *, max_bytes=SSE_FRAME_MAX_BYTES):
    """Bounded, byte-preserving SSE events across LF/CRLF/CR fragments."""
    if type(max_bytes) is not int or max_bytes <= 0:
        raise ValueError('A positive integer frame budget is required')
    buffer, scan_from, eof = bytearray(), 0, False
    while True:
        match = _SSE_END.search(buffer, scan_from)
        # A final CR might be the first half of a CRLF in the next read.
        if match is not None and (eof or not (match.end() == len(buffer) and buffer[-1] == 13)):
            end = match.end()
            if end > max_bytes:
                raise ValueError('Backend stream frame exceeds the byte limit')
            frame = bytes(buffer[:end])
            del buffer[:end]
            scan_from = 0
            yield frame
            continue
        if len(buffer) > max_bytes:
            raise ValueError('Backend stream frame exceeds the byte limit')
        if eof:
            if buffer:
                yield bytes(buffer)  # preserve legacy EOF-tail handling
            return
        # A delimiter has at most4bytes; only its last3 can cross a read.
        scan_from = max(0, len(buffer) - 3)
        chunk = await stream.read(8192)
        if chunk:
            buffer.extend(chunk)
        else:
            eof = True


def parse_sse_event(raw_event):
    """Decode the JSON from the ``data:`` field(s) of one SSE block (bytes up to
    the blank line). Returns the decoded object, or None for blocks with no JSON
    payload (SSE comments, ``[DONE]``, keep-alives, a bare ``event:`` line).
    Joins multi-line ``data:`` per the SSE spec. Used by the Anthropic and
    OpenAI-Responses usage accumulators (both send named ``event:``/``data:``
    blocks, unlike chat's plain ``data: {json}``)."""
    data_lines = []
    for line in re.split(rb'\r\n|\r|\n', raw_event):
        if line.startswith(b"data:"):
            payload = line[len(b"data:"):]
            if payload.startswith(b" "):
                payload = payload[1:]
            data_lines.append(payload)

    if not data_lines:
        return None

    raw = b"\n".join(data_lines)
    if raw == b"[DONE]":
        return None

    try:
        return json.loads(raw, parse_float=decimal.Decimal)
    except json.decoder.JSONDecodeError:
        return None


async def drain(read_block, write_block, on_chunk, on_disconnect=None,
        disconnected=False, *, write_timeout=10.0):
    """Forward SSE blocks from a backend to the client without ever losing usage.

    Reads blocks via ``read_block()`` until it returns ``b""`` (backend EOF),
    feeding every block to ``on_chunk`` and forwarding it via ``write_block``.
    If the client goes away (``write_block`` raises ``OSError``) we STOP writing
    but KEEP reading and feeding ``on_chunk`` — so a mid-stream disconnect never
    costs us the usage, and therefore the billing. This is the billing moat.

    ``read_block``/``write_block`` are injected, keeping this pure w.r.t. aiohttp
    and unit-testable without a real connection. Pass ``disconnected=True`` to
    start already disconnected (e.g. the client vanished during the header
    flush): we then drain the backend for usage without writing.
    """
    if (type(write_timeout) not in (int, float) or write_timeout <= 0
            or not math.isfinite(write_timeout)):
        raise ValueError("A finite positive stream write deadline is required")
    while (chunk := await read_block()):
        on_chunk(chunk)
        if disconnected:
            continue
        try:
            async with asyncio.timeout(write_timeout):
                await write_block(chunk)
        except (OSError, TimeoutError) as e:
            disconnected = True
            if on_disconnect is not None:
                on_disconnect(e)


def set_prepared_headers(f_req, f_res):
    """Set the headers the middlewares add, on a response the handler prepares.

    A response prepared before the handler returns is past
    add_request_id_header and add_cors_headers, so X-Request-ID and CORS must
    be on it before prepare().
    """
    f_res.headers["X-Request-ID"] = str(f_req["request_id"])
    f_res.headers["Access-Control-Allow-Headers"] = "Authorization, Content-Type"
    f_res.headers["Access-Control-Expose-Headers"] = EXPOSE_HEADERS
    if o := f_req.app["config"].get("http_origin"):
        f_res.headers["Access-Control-Allow-Origin"] = o


def is_terminal_event(chunk):
    if chunk.strip() == b'data: [DONE]':
        return True
    event = parse_sse_event(chunk)
    if not isinstance(event, dict):
        return False
    if event.get('type') in ('message_delta', 'message_stop', 'response.completed',
            'response.incomplete', 'response.failed'):
        return True
    return (event.get('usage') is not None or any(choice.get('finish_reason') is not None
        for choice in event.get('choices', []) if isinstance(choice, dict)))


async def stream_through(f_req, b_res, on_chunk, extra_headers=None, *, before_complete=None):
    """Wire a chunked backend response to the client, invoking ``on_chunk`` for
    every SSE block (including during the post-disconnect drain). Returns the
    prepared client ``StreamResponse``.

    ``on_chunk`` is a per-format usage accumulator, so the same pump/drain logic
    serves chat, Anthropic messages and OpenAI responses.

    ``extra_headers`` (e.g. the provenance marking) must be passed HERE, not
    added by middleware: the response is prepared before the handler returns,
    and prepared responses skip add_request_id_header/add_cors_headers -- the
    same reason X-Request-ID and CORS are set manually below.
    """
    app = f_req.app
    write_timeout = app["config"].get("write_timeout", 10.0)
    headers = {"Content-Type":
        b_res.headers.get("Content-Type", "application/octet-stream")}
    headers["X-Request-ID"] = str(f_req["request_id"])
    if extra_headers:
        headers.update(extra_headers)
    f_res = aiohttp.web.StreamResponse(headers=headers)
    set_prepared_headers(f_req, f_res)

    # Prepare (flush) response headers. If the client is already gone, prepare()
    # raises OSError — but we still MUST drain the backend to capture usage, so
    # treat a prepare-time disconnect as an immediate disconnected state instead
    # of letting it abort billing (matches the pre-refactor handle_resp_stream,
    # which wrapped prepare in the same try/except as the write loop).
    disconnected = False

    def on_disconnect(error):
        nonlocal disconnected
        disconnected = True
        f_res.force_close()
        if f_req.transport is not None:
            # Do not let the framework retry an unfinished prepare/EOF against
            # a stalled socket after the handler returns. Only the client closes;
            # the independent upstream is still drained for final usage.
            f_req.transport.abort()
        app.logger.info("Client disconnected (%s)", type(error).__name__)

    try:
        async with asyncio.timeout(write_timeout):
            await f_res.prepare(f_req)
    except (OSError, TimeoutError) as error:
        on_disconnect(error)

    blocks = sse_blocks(b_res.content)

    async def read_block():
        return await anext(blocks, b'')

    held, held_bytes = [], 0

    async def write_block(chunk):
        nonlocal held_bytes
        # A finished choice is not global stream completion. Hold only its
        # terminal event; unrelated choices must keep streaming genuine deltas.
        # Nullable OpenAI usage on normal deltas is not terminal proof.
        if before_complete is not None and is_terminal_event(chunk):
            held_bytes += len(chunk)
            if held_bytes > 4 * SSE_FRAME_MAX_BYTES or len(held) >= 64:
                raise ValueError('Deferred stream completion exceeds its budget')
            held.append(chunk)
        else:
            await f_res.write(chunk)

    try:
        await drain(read_block, write_block, on_chunk,
            on_disconnect=on_disconnect, disconnected=disconnected,
            write_timeout=write_timeout)
        # Persist final usage even if the consumer disconnected. Never send a
        # success sentinel/terminal event or HTTP EOF before that commit.
        if before_complete is not None:
            await before_complete()
    except BaseException:
        f_res.force_close()
        if f_req.transport is not None:
            f_req.transport.abort()
        raise

    if not disconnected:
        try:
            for chunk in held:
                async with asyncio.timeout(write_timeout):
                    await f_res.write(chunk)
        except (OSError, TimeoutError) as error:
            on_disconnect(error)

    if not disconnected:
        try:
            async with asyncio.timeout(write_timeout):
                await f_res.write_eof()
        except (OSError, TimeoutError) as error:
            on_disconnect(error)

    return f_res
