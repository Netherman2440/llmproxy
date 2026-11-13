"""HTTP/WS fixture for the external GPU boundary; accounting stays real."""
import asyncio
import copy
import json

from aiohttp import web

from .accounting_fixtures import INSTANCE, receipt


class ReceiptBackend:
    def __init__(self, db):
        self.db = db
        self.sessions = {}
        self.receipts = []
        self.acks = []
        self.bound_before_connect = False
        self.ack_before_bill = False
        self.protocol = 'receipt-v1'
        self.malformed = False
        self.defer = False
        self.delay = 0
        self.terminal_overrides = {}
        self.app = web.Application()
        self.app.router.add_get('/health', lambda request: web.Response())
        self.app.router.add_get('/v1/realtime', self.websocket)
        self.app.router.add_get('/internal/v1/sessions/{session}/receipts', self.page)
        self.app.router.add_post('/internal/v1/receipts/{event}/ack', self.ack)

    async def websocket(self, request):
        if request.headers.get('Authorization') != 'Bearer synthetic-service-auth':
            return web.Response(status=401)
        sid = request.headers.get('X-STT-Session-ID', '0' * 32)
        row = await (await self.db.db.execute('SELECT * FROM accounting_session WHERE session_id=?', (sid,))).fetchone()
        self.bound_before_connect = row is not None
        self.sessions[sid] = {'state': 'active', 'pending': 0}
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        await ws.send_json({'type': 'session.created', 'session_id': sid,
            'backend_instance_id': INSTANCE, 'protocol_version': self.protocol})
        waiting = 0
        async def complete():
            value = receipt()
            segment = 1 + sum(item['session_id'] == sid for item in self.receipts)
            value.update(session_id=sid, segment_id='seg_' + str(segment),
                event_id=INSTANCE + ':' + sid + ':seg_' + str(segment),
                cursor=len(self.receipts) + 1, start_sample=(segment - 1) * 16000)
            self.receipts.append(value)
            self.sessions[sid]['pending'] -= 1
            event = {'type': 'conversation.item.input_audio_transcription.completed',
                'item_id': value['segment_id'], 'receipt': copy.deepcopy(value),
                'usage': copy.deepcopy(value['usage']), 'transcript': 'GPU boundary fixture'}
            if self.malformed:
                event['usage']['seconds'] = True
            await ws.send_json(event)
        async for message in ws:
            command = json.loads(message.data)
            if command.get('type') == 'input_audio_buffer.commit':
                self.sessions[sid]['pending'] += 1
                if self.defer:
                    waiting += 1
                else:
                    await complete()
            elif command.get('type') == 'session.finish':
                await asyncio.sleep(self.delay)
                for _ in range(waiting):
                    await complete()
                self.sessions[sid]['state'] = 'finished'
                last = max([0] + [item['cursor'] for item in self.receipts if item['session_id'] == sid])
                await ws.send_json({'type': 'session.finished', 'session_id': sid,
                    'state': 'finished', 'last_cursor': last, 'pending': 0, 'discarded_samples': 0} | self.terminal_overrides)
                break
        await ws.close()
        return ws

    async def page(self, request):
        if request.headers.get('Authorization') != 'Bearer synthetic-service-auth':
            return web.Response(status=401)
        sid = request.match_info['session']
        if sid not in self.sessions:
            return web.Response(status=404)
        after = int(request.query.get('after', 0))
        all_items = [item for item in self.receipts if item['session_id'] == sid]
        items = [item for item in all_items if item['cursor'] > after][:100]
        return web.json_response({'session_id': sid, 'backend_instance_id': INSTANCE,
            'protocol_version': 'receipt-v1', 'items': items,
            'next_cursor': items[-1]['cursor'] if items else after,
            'last_cursor': max([0] + [item['cursor'] for item in all_items]),
            'has_more': False, 'pruned_cursor': 0, 'retention_gap': False,
            'discarded_samples': 0} | self.sessions[sid])

    async def ack(self, request):
        if request.headers.get('Authorization') != 'Bearer synthetic-service-auth':
            return web.Response(status=401)
        event_id = request.match_info['event']
        row = await (await self.db.db.execute('SELECT * FROM accounting_receipt WHERE event_id=?', (event_id,))).fetchone()
        if row is None or row['status'] not in ('billed', 'terminal'):
            self.ack_before_bill = True
        self.acks.append(event_id)
        return web.json_response({'status': 'acknowledged'})
