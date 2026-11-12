from decimal import Decimal

import aiohttp

from .test_realtime import RealtimeTestCase


class TestLegacyCompletedUsage(RealtimeTestCase):
    async def test_invalid_duration_never_delivers_completed_transcript_or_creates_finance(self):
        for seconds in (None, 0, -1, True, '3.7', float('nan'), float('inf'), 10**400):
            with self.subTest(value_type=type(seconds).__name__):
                self.backend.app['ws_control']['usage_seconds'] = seconds
                ws = await self.client.ws_connect('/v1/realtime', headers=self.AUTH)
                try:
                    await ws.send_json({'type': 'input_audio_buffer.commit'})
                    message = await ws.receive(timeout=3)
                    self.assertEqual(message.type, aiohttp.WSMsgType.CLOSE,
                        'Invalid backend proof escaped as a completed transcript')
                    self.assertEqual(message.data, 1011)
                finally:
                    await ws.close()
                self.assertEqual(await self.get_events(), [])

    async def test_minimum_positive_fraction_remains_billable_before_delivery(self):
        self.backend.app['ws_control']['usage_seconds'] = 0.0000625
        ws = await self.client.ws_connect('/v1/realtime', headers=self.AUTH)
        try:
            await ws.send_json({'type': 'input_audio_buffer.commit'})
            event = await ws.receive_json(timeout=3)
            self.assertEqual(event['type'], 'conversation.item.input_audio_transcription.completed')
            records = await self.get_events()
            self.assertEqual(len(records), 1)
            self.assertEqual(Decimal(str(records[0]['quantity'])), Decimal('0.0000625'))
        finally:
            await ws.close()

    async def test_unrelated_non_json_text_is_still_transparent(self):
        ws = await self.client.ws_connect('/v1/realtime', headers=self.AUTH)
        try:
            await ws.send_str('synthetic non-json keepalive')
            message = await ws.receive(timeout=3)
            self.assertEqual(message.type, aiohttp.WSMsgType.TEXT)
            self.assertEqual(message.data, 'synthetic non-json keepalive')
            self.assertEqual(await self.get_events(), [])
        finally:
            await ws.close()
