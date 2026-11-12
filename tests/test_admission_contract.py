"""Conformance for existing credential-scoped policy; no account limit change."""
import types
import unittest

import aiohttp.web

from llmproxy import ratelimit


class TestCredentialAdmissionContract(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        ratelimit.flush()
        self.owner = 'same-synthetic-billing-owner'
        self.first = {'id': 'credential-1', '_user_id': self.owner, 'comment': 'one'}
        self.second = {'id': 'credential-2', '_user_id': self.owner, 'comment': 'two'}

    def tearDown(self):
        ratelimit.flush()

    def request(self, policy):
        return types.SimpleNamespace(app={'config': policy,
            'parser_admission': ratelimit.ParserAdmission()},
            rel_url=types.SimpleNamespace(path='/v1/chat/completions'))

    async def test_common_billing_owner_does_not_merge_rpm_buckets(self):
        request = self.request({'rate_limit': {'rpm': 1}})
        async with ratelimit.slot(request, self.first, 'model', {}):
            pass
        async with ratelimit.slot(request, self.second, 'model', {}):
            pass
        with self.assertRaises(aiohttp.web.HTTPTooManyRequests):
            async with ratelimit.slot(request, self.first, 'model', {}):
                self.fail('Repeated credential bypassed its own RPM')

    async def test_global_policy_spans_models_but_not_credentials(self):
        request = self.request({'rate_limit': {'concurrency': 1}})
        async with ratelimit.slot(request, self.first, 'model-a', {}):
            async with ratelimit.slot(request, self.second, 'model-b', {}):
                pass
            with self.assertRaises(aiohttp.web.HTTPTooManyRequests):
                async with ratelimit.slot(request, self.first, 'model-b', {}):
                    self.fail('Same credential bypassed global cross-model bucket')

    async def test_changed_metadata_does_not_reset_same_credential(self):
        request = self.request({'rate_limit': {'concurrency': 1}})
        changed = dict(self.first, comment='renamed')
        async with ratelimit.slot(request, self.first, 'model', {}):
            with self.assertRaises(aiohttp.web.HTTPTooManyRequests):
                async with ratelimit.slot(request, changed, 'model', {}):
                    self.fail('Comment change reset credential bucket')

    async def test_parser_per_user_setting_is_per_credential_and_global_cap_is_shared(self):
        request = self.request({'parsing': {'concurrency': 2, 'per_user': 1, 'timeout': 1}})
        async with ratelimit.parser_slot(request, self.first):
            with self.assertRaises(aiohttp.web.HTTPTooManyRequests):
                async with ratelimit.parser_slot(request, self.first):
                    self.fail('Same credential held two parser slots')
            async with ratelimit.parser_slot(request, self.second):
                with self.assertRaises(aiohttp.web.HTTPTooManyRequests):
                    async with ratelimit.parser_slot(request, {'id': 'credential-3'}):
                        self.fail('Service-wide parser cap was treated as per credential')
        self.assertEqual(request.app['parser_admission'].active, 0)
        self.assertEqual(request.app['parser_admission'].users, {})
