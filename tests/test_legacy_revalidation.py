"""Legacy protocol compatibility must not extend authorization lifetime."""
import asyncio
import datetime

import aiohttp

from llmproxy.db import get_db
from .test_realtime import RealtimeTestCase


class TestLegacyRevalidation(RealtimeTestCase):
    async def update_key(self, *, expires=None, revoke=False):
        db = await get_db(self.app['config']['db']['uri'])
        try:
            if revoke:
                await db.db.execute("DELETE FROM api_key WHERE id='myuser'")
            else:
                await db.db.execute("UPDATE api_key SET expires=? WHERE id='myuser'", (expires,))
            await db.db.commit()
        finally:
            await db.close()

    async def test_expiry_refuses_new_work_on_an_existing_legacy_socket(self):
        await self.update_key(expires=datetime.datetime.now(datetime.UTC) + datetime.timedelta(seconds=0.5))
        ws = await self.client.ws_connect('/v1/realtime', headers=self.AUTH)
        try:
            await asyncio.sleep(0.8)
            try:
                await ws.send_json({'type': 'input_audio_buffer.commit'})
            except (ConnectionError, aiohttp.ClientError):
                pass
            message = await asyncio.wait_for(ws.receive(), 4)
            self.assertEqual(message.type, aiohttp.WSMsgType.CLOSE)
            self.assertEqual(message.data, 1008)
            self.assertEqual(await self.get_events(), [])
        finally:
            await ws.close()

    async def test_revocation_ignores_long_auth_cache_and_stops_idle_legacy_session(self):
        self.app['config']['auth_cache_ttl'] = 60
        ws = await self.client.ws_connect('/v1/realtime', headers=self.AUTH)
        try:
            await self.update_key(revoke=True)
            try:
                message = await asyncio.wait_for(ws.receive(), 5)
            except TimeoutError:
                self.fail('Revoked key retained an open legacy session beyond five seconds')
            self.assertEqual(message.type, aiohttp.WSMsgType.CLOSE)
            self.assertEqual(message.data, 1008)
            self.assertEqual(await self.get_events(), [])
        finally:
            await ws.close()

    async def test_expiry_preserves_bill_for_work_accepted_before_deadline(self):
        self.backend.app['ws_control']['completion_delay'] = 0.5
        accepted = self.backend.app['ws_control']['commit_started'] = asyncio.Event()
        await self.update_key(expires=datetime.datetime.now(datetime.UTC) + datetime.timedelta(seconds=0.35))
        ws = await self.client.ws_connect('/v1/realtime', headers=self.AUTH)
        try:
            await ws.send_json({'type': 'input_audio_buffer.commit'})
            await asyncio.wait_for(accepted.wait(), 2)
            event = await ws.receive_json(timeout=3)
            self.assertEqual(event['type'], 'conversation.item.input_audio_transcription.completed')
            self.assertEqual(await self.get_events(expect=1), [
                {'product': 'myrealtime/none/transcription', 'quantity': 3.7}])
        finally:
            await ws.close()

    async def test_revocation_preserves_historical_bill_for_pending_work(self):
        self.app['config']['auth_cache_ttl'] = 60
        self.backend.app['ws_control']['completion_delay'] = 0.5
        accepted = self.backend.app['ws_control']['commit_started'] = asyncio.Event()
        ws = await self.client.ws_connect('/v1/realtime', headers=self.AUTH)
        try:
            await ws.send_json({'type': 'input_audio_buffer.commit'})
            await asyncio.wait_for(accepted.wait(), 2)
            await self.update_key(revoke=True)
            event = await ws.receive_json(timeout=3)
            self.assertEqual(event['type'], 'conversation.item.input_audio_transcription.completed')
            self.assertEqual(await self.get_events(expect=1), [
                {'product': 'myrealtime/none/transcription', 'quantity': 3.7}])
        finally:
            await ws.close()
