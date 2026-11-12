"""One backend must map to one billing product in every profile."""
import unittest

from llmproxy import accounting, billing, delivery
from llmproxy.files import billing_product

from .test_realtime import RealtimeTestCase


class TestProductIdentityContract(unittest.TestCase):
    def test_constructors_agree_for_the_same_uppercase_backend(self):
        upper = getattr(billing, 'product', None)
        self.assertIsNotNone(upper, 'Single canonical product constructor missing')
        cases = (
            (billing.product('PDF', 'GPU', 'conversion'),
                billing_product(_FakeReq(), 'PDF', {'device': 'GPU'}),
                delivery.product({'backend_alias': 'PDF', 'device': 'GPU'}),
                'pdf/gpu/conversion'),
            (billing.product('RT', 'GPU', 'transcription'),
                accounting.product({'backend_alias': 'RT', 'device': 'GPU'}),
                'rt/gpu/transcription'),
        )
        for *values, expected in cases:
            with self.subTest(expected=expected):
                self.assertEqual(set(values), {expected})

    def test_invalid_identity_is_rejected_not_silently_normalized(self):
        for args in ((None, 'GPU', 'x'), ('PDF', 7, 'x'), ('PDF', 'GPU', None), ('', 'GPU', 'x')):
            with self.subTest(args=args), self.assertRaises(ValueError):
                billing.product(*args)


class _FakeReq(dict):
    app = {'logger': __import__('logging').getLogger('test.product')}

    def __getitem__(self, key):
        return {'request_id': 'synthetic'}.get(key)


class TestUppercaseBackendBilling(RealtimeTestCase):
    async def get_application(self):
        app = await super().get_application()
        url = "http://%s:%d" % (self.backend.host, self.backend.port)
        app['config']['backends'].update({
            'UPPER': {'url': url, 'token': 'mybackendtoken', 'device': 'GPU', 'type': 'chat', 'model': 'mymodel'},
            'EMB': {'url': url, 'token': 'mybackendtoken', 'device': 'GPU', 'type': 'embedding', 'model': 'mymodel'},
            'RT': {'url': url, 'token': 'mybackendtoken', 'device': 'GPU', 'type': 'realtime'},
        })
        return app

    async def test_chat_uppercase_alias_bills_canonical_products(self):
        response = await self.client.post('/v1/chat/completions', headers=self.AUTH,
            json={'model': 'UPPER', 'stream': True, 'messages': [{'role': 'user', 'content': 'synthetic'}]})
        self.assertEqual(response.status, 200)
        await response.read()
        self.assertEqual([row['product'] for row in await self.get_events(expect=2)],
            ['upper/gpu/prompt', 'upper/gpu/completion'])

    async def test_embeddings_uppercase_alias_bills_canonical_product(self):
        response = await self.client.post('/v1/embeddings', headers=self.AUTH,
            json={'model': 'EMB', 'input': 'synthetic'})
        self.assertEqual(response.status, 200)
        await response.read()
        self.assertEqual([row['product'] for row in await self.get_events(expect=1)],
            ['emb/gpu/embedding'])

    async def test_legacy_realtime_uppercase_alias_bills_canonical_product(self):
        ws = await self.client.ws_connect('/v1/realtime?model=RT', headers=self.AUTH)
        try:
            await ws.send_json({'type': 'input_audio_buffer.commit'})
            await ws.receive_json(timeout=3)
        finally:
            await ws.close()
        self.assertEqual([row['product'] for row in await self.get_events(expect=1)],
            ['rt/gpu/transcription'])
