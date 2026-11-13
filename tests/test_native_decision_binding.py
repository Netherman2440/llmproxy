import copy
import unittest

from llmproxy import basal, config
from .test_proxy import LLMProxyAppTestCase


def choice(value='a', probabilities=None):
    return {'type': 'choice', 'choice': value, 'probabilities': probabilities or {'a': 1, 'b': 0}, 'confidence': 1}


def response(answer):
    return {'model': 'native-provider', 'answers': {'q': answer},
        'usage': {'input_tokens': 23, 'output_tokens': 0}}


class TestNativeDecisionBinding(LLMProxyAppTestCase):
    async def get_application(self):
        app = await super().get_application()
        app['config']['backends']['native'] = {'type': 'decision', 'device': 'cpu',
            'url': str(self.backend.make_url('')).rstrip('/'), 'model': 'native-provider'}
        return app

    async def decide(self, question, answer):
        self.backend.app['decision_control']['response'] = response(answer)
        async with self.client.post('/v1/systemone', headers={'Authorization': 'Bearer mytoken'},
                json={'model': 'native', 'state': 'synthetic', 'questions': {'q': question}}) as result:
            return result.status, await result.json()

    async def test_foreign_or_incomplete_choice_probabilities_are_not_billable(self):
        for criteria in ({'a': 'A', 'b': 'B'}, ['a', 'b']):
            for answer in (choice('z', {'z': 1}), choice('a', {'a': 1}),
                           choice('a', {'a': 1, 'b': 0, 'z': 0})):
                with self.subTest(criteria=criteria, answer=answer):
                    status, body = await self.decide({'type': 'choice', 'criteria': criteria}, answer)
                    self.assertEqual(status, 502)
                    self.assertEqual(body['error']['code'], 'invalid_backend_response')
        self.assertEqual(await self.get_events(), [])

    async def test_foreign_multi_score_and_boolean_keys_are_not_billable(self):
        cases = [({'type': 'multi', 'criteria': ['a', 'b']}, {'type': 'multi', 'selected': ['z'],
            'probabilities': {'a': 0, 'b': 0, 'z': 1}, 'set_confidence': 1}),
            ({'type': 'score', 'levels': ['low', 'high']}, {'type': 'score', 'score': 1,
                'probabilities': {'0': 0, '9': 1}, 'confidence': 1}),
            ({'type': 'noul'}, {'type': 'noul', 'noul': 0.9,
                'probabilities': {'yes': 0.9, 'no': 0.1}, 'confidence': 0.9})]
        for question, answer in cases:
            with self.subTest(kind=question['type']):
                self.assertEqual((await self.decide(question, answer))[0], 502)
        self.assertEqual(await self.get_events(), [])

    async def test_foreign_tool_selection_refusal_and_ranking_are_rejected(self):
        question = {'type': 'tool', 'tools': [{'name': 'lookup'}]}
        valid = {'type': 'tool', 'tool': 'lookup', 'confidence': 1,
            'probabilities': {'lookup': 1, 'answer_directly': 0, 'ask_user': 0, 'none': 0}}
        cases = [valid | {'tool': 'exfiltrate', 'probabilities': {'exfiltrate': 1}},
            valid | {'tool': None, 'deferred': True, 'refused': 'foreign'},
            valid | {'ranking': [{'tool': 'foreign', 'probability': 0.1}]},
            valid | {'ranking': [{'tool': {'foreign': True}, 'probability': 0.1}]},
            valid | {'probabilities': {'lookup': 1}}]
        for answer in cases:
            with self.subTest(answer=answer):
                self.assertEqual((await self.decide(question, answer))[0], 502)
        self.assertEqual(await self.get_events(), [])

    async def test_tool_multi_default_has_no_implicit_extra_options(self):
        question = {'type': 'tool', 'multi': True, 'tools': [{'type': 'function',
            'function': {'name': 'lookup', 'parameters': {'type': 'object'}}}]}
        foreign = {'type': 'tool', 'tools': ['none'], 'set_confidence': 1,
            'probabilities': {'lookup': 0, 'none': 1}}
        self.assertEqual((await self.decide(question, foreign))[0], 502)
        self.assertEqual(await self.get_events(), [])

    async def test_action_keys_are_bound_to_the_declared_cost_matrix(self):
        question = {'type': 'act', 'criteria': {'true': 'yes', 'false': 'no'},
            'costs': {'approve': {'true': 0, 'false': 10}, 'human': {'true': 1, 'false': 1}}}
        foreign = {'type': 'act', 'action': 'foreign', 'expected_costs': {'foreign': 0},
            'probabilities': {'true': 0.5, 'false': 0.5}, 'confidence': 0.5}
        self.assertEqual((await self.decide(question, foreign))[0], 502)
        malformed_answer = foreign | {'action': 'human', 'expected_costs': {'approve': 5, 'human': 1},
            'answer': {'not': 'an outcome'}}
        self.assertEqual((await self.decide(question, malformed_answer))[0], 502)
        self.assertEqual(await self.get_events(), [])

    async def test_valid_declared_types_bill_once_and_preserve_native_payload(self):
        cases = [({'type': 'choice', 'criteria': ['a', 'b']}, choice()),
            ({'type': 'noul'}, {'type': 'noul', 'noul': 1, 'confidence': 1,
                'probabilities': {'true': 1, 'false': 0}}),
            ({'type': 'score', 'criteria': ['low', 'high']}, {'type': 'score', 'score': 1, 'confidence': 1,
                'probabilities': {'0': 0, '1': 1}}),
            ({'type': 'multi', 'criteria': {'a': 'A', 'b': 'B'}}, {'type': 'multi', 'selected': [],
                'probabilities': {'a': 0.1, 'b': 0.2}, 'set_confidence': 0.8}),
            ({'type': 'tool', 'tools': [{'name': 'lookup'}]}, {'type': 'tool', 'tool': 'none',
                'probabilities': {'lookup': 0, 'answer_directly': 0, 'ask_user': 0, 'none': 1}, 'confidence': 1}),
            ({'type': 'act', 'costs': {'wrong': 10, 'defer': 0.1}}, {'type': 'act', 'action': 'defer',
                'expected_costs': {'true': 5, 'false': 5, 'defer': 0.1},
                'probabilities': {'true': 0.5, 'false': 0.5}, 'confidence': 0.5})]
        for question, answer in cases:
            with self.subTest(kind=question['type']):
                status, body = await self.decide(question, answer)
                self.assertEqual(status, 200, body)
                self.assertEqual(body, response(answer) | {'model': 'native'})
        events = await self.get_events()
        self.assertEqual(len(events), 12)
        self.assertEqual(sum(row['quantity'] for row in events), 138)


class TestPreparedOptionSnapshot(unittest.TestCase):
    def test_later_input_mutation_cannot_reassign_the_expected_option_universe(self):
        body = {'state': 'synthetic', 'questions': {'q': {'criteria': {'a': 'A', 'b': 'B'}}}}
        prepared = basal.prepare(body, config.DECISION_LIMITS)
        body['questions']['q']['criteria'] = {'z': 'Z'}
        with self.assertRaises(ValueError):
            prepared.decode(response(choice('z', {'z': 1})))
        # A correct result still uses the preparation snapshot, not the mutated input.
        self.assertEqual(prepared.decode(response(choice())).answers, {'q': choice()})

    def test_preparation_does_not_mutate_or_rename_native_tools(self):
        body = {'state': 'synthetic', 'questions': {'q': {'type': 'tool', 'multi': True,
            'tools': [{'name': 'lookup', 'inputSchema': {'type': 'object'}}], 'extra': []}}}
        original = copy.deepcopy(body)
        basal.prepare(body, config.DECISION_LIMITS)
        self.assertEqual(body, original)
