"""Reject invalid backend usage before any financial row or complete output."""
from decimal import Decimal
from unittest import mock
import unittest

from aiohttp import web

from llmproxy import billing
from . import mockbackend
from .test_proxy import LLMProxyAppTestCase


class TestEmbeddingUsage(LLMProxyAppTestCase):
    async def asyncSetUp(self):
        self.reply = {'object': 'list', 'data': [], 'usage': {'prompt_tokens': 3, 'total_tokens': 3}}

        async def backend(request):
            return web.json_response(self.reply)

        patch = mock.patch.object(mockbackend, 'embeddings', backend)
        patch.start()
        self.addCleanup(patch.stop)
        await super().asyncSetUp()

    async def request(self):
        return await self.client.post('/v1/embeddings', headers={'Authorization': 'Bearer mytoken'},
            json={'model': 'myembedding', 'input': 'synthetic'})

    async def reject_quantity(self, value):
        self.reply['usage'] = {'prompt_tokens': value, 'total_tokens': value}
        response = await self.request()
        self.assertEqual(response.status, 502)
        await response.read()
        self.assertEqual(await self.get_events(), [], 'Invalid upstream proof committed finance')

    async def test_negative_tokens_never_become_financial_credit(self):
        await self.reject_quantity(-7)

    async def test_boolean_is_not_a_token_count(self):
        await self.reject_quantity(True)

    async def test_fractional_token_count_is_rejected(self):
        await self.reject_quantity(1.5)

    async def test_nonfinite_token_count_is_rejected(self):
        await self.reject_quantity(float('inf'))

    async def test_string_token_count_is_rejected(self):
        await self.reject_quantity('7')

    async def test_out_of_exact_token_range_is_rejected(self):
        await self.reject_quantity(2**53)

    async def test_missing_or_nonobject_usage_is_bad_gateway_not_success_or_server_error(self):
        for payload in ({'data': []}, {'usage': None}, {'usage': []}, {'usage': {}}, []):
            with self.subTest(payload_type=type(payload).__name__):
                self.reply = payload
                response = await self.request()
                self.assertEqual(response.status, 502)
                await response.read()
                self.assertEqual(await self.get_events(), [])

    async def test_valid_positive_count_is_preserved(self):
        response = await self.request()
        self.assertEqual(response.status, 200)
        self.assertEqual(await response.json(), self.reply)
        self.assertEqual(await self.get_events(), [{'product': 'myembedding/none/embedding', 'quantity': 3}])

    async def test_explicit_zero_count_is_valid_not_missing_proof(self):
        self.reply['usage'] = {'prompt_tokens': 0, 'total_tokens': 0}
        response = await self.request()
        self.assertEqual(response.status, 200)
        await response.read()
        self.assertEqual(await self.get_events(), [{'product': 'myembedding/none/embedding', 'quantity': 0}])


class TestCommonFinanceBoundary(unittest.TestCase):
    def test_invalid_quantities_are_rejected_independently_of_handler(self):
        validate = getattr(billing, 'validate_resources', None)
        self.assertIsNotNone(validate, 'Shared finance validation boundary missing')
        for quantity in (-1, Decimal('-0.1'), True, None, '1', float('nan'), float('inf'), Decimal('NaN')):
            with self.subTest(quantity_type=type(quantity).__name__), self.assertRaises(ValueError):
                validate({'synthetic/device/resource': quantity})
        for quantity in (0, 1, 0.25, Decimal('0.0000625')):
            with self.subTest(valid_type=type(quantity).__name__):
                validate({'synthetic/device/resource': quantity})
