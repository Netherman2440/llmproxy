"""Incremental multipart parsing with separate file, text and field budgets."""

import asyncio
from tempfile import SpooledTemporaryFile

from aiohttp import BodyPartReader, MultipartReader
from aiohttp.web import FileField, HTTPRequestEntityTooLarge
from multidict import MultiDict, MultiDictProxy


MAX_FIELDS = 128
MAX_TEXT_BYTES = 64 * 1024
SPOOL_MEMORY_BYTES = 1024 * 1024


async def read_multipart(request):
    cfg = request.app["config"]
    max_fields = cfg.get("max_multipart_fields", MAX_FIELDS)
    max_text = cfg.get("max_multipart_text_bytes", MAX_TEXT_BYTES)
    max_upload = request._client_max_size
    reader = MultipartReader(request.headers, request.content,
        client_max_size=max_upload, max_headers=32, max_field_size=8190,
        max_size_error_cls=HTTPRequestEntityTooLarge)
    fields, opened = MultiDict(), []

    def check_total():
        if request.content.total_bytes > max_upload:
            raise HTTPRequestEntityTooLarge(max_upload)

    try:
        while (part := await reader.next()) is not None:
            check_total()
            if len(fields) >= max_fields:
                raise HTTPRequestEntityTooLarge(max_fields, text="Too many multipart fields")
            if not isinstance(part, BodyPartReader) or part.name is None:
                raise ValueError("Multipart requires named, non-nested fields")
            content_type = part.headers.get("Content-Type")
            if part.filename:
                spool = SpooledTemporaryFile(SPOOL_MEMORY_BYTES)
                opened.append(spool)
                while chunk := await part.read_chunk(size=65536):
                    check_total()
                    async for decoded in part.decode_iter(chunk):
                        if spool.tell() + len(decoded) > max_upload:
                            raise HTTPRequestEntityTooLarge(max_upload)
                        if spool.tell() + len(decoded) > SPOOL_MEMORY_BYTES:
                            await asyncio.to_thread(spool.write, decoded)
                        else:
                            spool.write(decoded)
                if spool.tell() > SPOOL_MEMORY_BYTES:
                    await asyncio.to_thread(spool.seek, 0)
                else:
                    spool.seek(0)
                fields.add(part.name, FileField(part.name, part.filename, spool,
                    content_type or "application/octet-stream", part.headers))
            else:
                value, raw_size = bytearray(), 0
                # Small configured budgets must reject before an unfinished
                # large field is buffered. Reader needs room for its boundary.
                chunk_size = min(65536, max(1024, max_text + 1))
                while chunk := await part.read_chunk(size=chunk_size):
                    check_total()
                    raw_size += len(chunk)
                    if raw_size > max_text:
                        raise HTTPRequestEntityTooLarge(max_text)
                    async for decoded in part.decode_iter(chunk):
                        if len(value) + len(decoded) > max_text:
                            raise HTTPRequestEntityTooLarge(max_text)
                        value.extend(decoded)
                if content_type is None or content_type.startswith("text/"):
                    value = value.decode(part.get_charset(default="utf-8"))
                fields.add(part.name, value)
        check_total()
        # Preserve aiohttp's successful POST ownership: Request._finish closes
        # FileFields after the handler and its backend forward have finished.
        request._post = MultiDictProxy(fields)
        return request._post
    except BaseException:
        for spool in opened:
            spool.close()
        raise
