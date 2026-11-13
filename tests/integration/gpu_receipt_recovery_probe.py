"""Kill/restart a private local proxy against the EXISTING audit GPU STT.

All financial data is synthetic and isolated in a temporary SQLite database.
No live demo process, volume, key, tariff or customer account is changed.
"""
import asyncio
import base64
import hashlib
import json
import logging
import multiprocessing
import os
import secrets
import sqlite3
import sys
import tempfile
from pathlib import Path

import aiohttp


STAGE = 'scope'
BASE = 'http://audit-stt-parakeet:8000'
MODEL = 'nvidia/parakeet-tdt-0.6b-v3'


def proxy_worker(path, ready):
    from aiohttp import web
    from llmproxy.app import create_app
    async def serve():
        logging.basicConfig(level=logging.ERROR)
        app = await create_app({'db': {'uri': 'sqlite://' + path},
            'timeout_connect': 3, 'timeout_read': 30,
            'realtime': {'mode': 'receipt-v1'},
            'backends': {'parakeet': {'type': 'realtime', 'url': BASE,
                'token': os.environ['AUDIT_BACKEND_API_KEY'], 'model': MODEL, 'device': 'a5000'}}})
        runner = web.AppRunner(app, handle_signals=False, handler_cancellation=False)
        await runner.setup()
        await web.TCPSite(runner, '127.0.0.1', 8765).start()
        ready.set()
        await asyncio.Future()
    asyncio.run(serve())


async def probe():
    global STAGE
    from llmproxy.db import SqliteDatabase
    assert os.environ['AUDIT_NAMESPACE'] == 'izajac'
    processes = []
    ctx = multiprocessing.get_context('spawn')
    lock = None
    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=60)) as client:
        STAGE = 'public_audio_fixture'
        # Reuse the exact normalized public Whisper JFK fixture from the
        # verified demo chain. The upstream source is tests/jfk.flac, NOT wav.
        fixture = Path(os.environ.get('AUDIT_AUDIO_PCM', '/tmp/accounting-src/tests/jfk.pcm'))
        assert fixture.stat().st_size == 352000
        pcm = fixture.read_bytes()
        assert hashlib.sha256(pcm).hexdigest() == '79ae760030afb0c93ae1f99078e4cbc6a744144dabea1cbaf082941c73ba48ce'
        api_key = secrets.token_urlsafe(32)
        headers = {'Authorization': 'Bearer ' + api_key}
        backend_headers = {'Authorization': 'Bearer ' + os.environ['AUDIT_BACKEND_API_KEY']}
        with tempfile.TemporaryDirectory(prefix='receipt-gpu-') as directory:
            path = directory + '/usage.sqlite'
            db = await SqliteDatabase.create('sqlite://' + path)
            try:
                await db.user_create(hashlib.sha256(api_key.encode()).hexdigest())
            finally:
                await db.close()

            async def start():
                ready = ctx.Event()
                process = ctx.Process(target=proxy_worker, args=(path, ready))
                processes.append(process)
                process.start()
                assert await asyncio.to_thread(ready.wait, 15), 'Isolated proxy did not start'
                return process

            def snapshot():
                with sqlite3.connect(path) as connection:
                    connection.row_factory = sqlite3.Row
                    bills = connection.execute('SELECT * FROM event_oneoff').fetchall()
                    receipts = connection.execute('SELECT * FROM accounting_receipt').fetchall()
                    sessions = connection.execute('SELECT * FROM accounting_session').fetchall()
                return bills, receipts, sessions

            async def audio(ws):
                await ws.send_json({'type': 'session.update', 'session': {'audio': {'input': {
                    'format': {'type': 'audio/pcm', 'rate': 16000}, 'turn_detection': None}}}})
                while (await ws.receive_json(timeout=10))['type'] != 'session.updated':
                    pass
                for offset in range(0, len(pcm), 6400):
                    await ws.send_json({'type': 'input_audio_buffer.append',
                        'audio': base64.b64encode(pcm[offset:offset + 6400]).decode()})
                    await asyncio.sleep(0.2)

            try:
                STAGE = 'kill_after_gpu_receipt_before_financial_commit'
                first = await start()
                ws = await client.ws_connect('http://127.0.0.1:8765/v1/realtime?model=parakeet', headers=headers)
                created = await ws.receive_json(timeout=10)
                assert created['protocol_version'] == 'receipt-v1'
                await audio(ws)
                lock = sqlite3.connect(path, timeout=5)
                lock.execute('BEGIN IMMEDIATE')
                await ws.send_json({'type': 'input_audio_buffer.commit'})
                async with asyncio.timeout(35):
                    while True:
                        async with client.get(BASE + '/internal/v1/sessions/' + created['session_id'] + '/receipts',
                                headers=backend_headers, allow_redirects=False) as response:
                            assert response.status == 200
                            page = await response.json()
                        if page['items']:
                            assert page['items'][0]['state'] == 'completed'
                            assert page['items'][0]['sample_count'] == 176000
                            break
                        await asyncio.sleep(0.25)
                assert snapshot()[0] == [], 'A financial write escaped the lock'
                first.kill()
                await asyncio.to_thread(first.join, 5)
                assert first.exitcode == -9
                lock.rollback()
                lock.close()
                lock = None
                await ws.close()
                STAGE = 'startup_reconciliation_after_sigkill'
                second = await start()
                async with asyncio.timeout(20):
                    while True:
                        bills, receipts, sessions = snapshot()
                        if len(bills) == 1 and receipts[0]['acked_at'] is not None:
                            break
                        await asyncio.sleep(0.1)
                assert bills[0]['quantity_exact'] == '11'
                assert bills[0]['product'] == 'parakeet/a5000/transcription'
                STAGE = 'normal_strict_profile_gpu_transcript'
                async with client.ws_connect('http://127.0.0.1:8765/v1/realtime?model=parakeet', headers=headers) as ws:
                    await ws.receive_json(timeout=10)
                    await audio(ws)
                    await ws.send_json({'type': 'input_audio_buffer.commit'})
                    async with asyncio.timeout(35):
                        while True:
                            event = await ws.receive_json()
                            if event['type'] == 'conversation.item.input_audio_transcription.completed':
                                assert event['usage']['seconds'] == 11
                                assert 'americans' in event['transcript'].lower()
                                assert len(snapshot()[0]) == 2, 'Final preceded its financial commit'
                                break
                    await ws.send_json({'type': 'session.finish'})
                    while (await ws.receive_json(timeout=10))['type'] != 'session.finished':
                        pass
                STAGE = 'second_restart_deduplicates_both_receipts'
                second.kill()
                await asyncio.to_thread(second.join, 5)
                await start()
                async with asyncio.timeout(20):
                    while True:
                        bills, receipts, sessions = snapshot()
                        if len(receipts) == 2 and all(row['acked_at'] is not None for row in receipts):
                            break
                        await asyncio.sleep(0.1)
                assert len(bills) == 2 and all(row['quantity_exact'] == '11' for row in bills)
                print(json.dumps({'status': 'PASS', 'namespace': 'izajac', 'gpu': 'existing-audit-stt-parakeet',
                    'sigkill_after_stt_commit_before_bill': True, 'startup_recovered_one_bill': True,
                    'normal_final_after_financial_commit': True, 'two_restarts_no_duplicate': True,
                    'segments': 2, 'audio_seconds': 22, 'customer_charges': False}), flush=True)
            finally:
                if lock is not None:
                    lock.rollback()
                    lock.close()
                for process in processes:
                    if process.is_alive():
                        process.kill()
                    await asyncio.to_thread(process.join, 5)


if __name__ == '__main__':
    try:
        asyncio.run(probe())
    except Exception as error:
        print(json.dumps({'status': 'FAIL', 'stage': STAGE,
            'exception_type': type(error).__name__}), flush=True)
        sys.exit(1)
