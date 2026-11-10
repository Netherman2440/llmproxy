import copy
import datetime
from decimal import Decimal
import importlib
import importlib.util
import unittest

from .accounting_fixtures import binding, completed_event, receipt


class TestReceiptContract(unittest.TestCase):
    def setUp(self):
        self.assertIsNotNone(importlib.util.find_spec('llmproxy.usage'), 'Receipt validator is missing')
        self.parse = importlib.import_module('llmproxy.usage').parse_receipt

    def test_exact_sample_quantity_is_authoritative(self):
        for samples, seconds, expected in ((1, 0.0000625, Decimal('0.0000625')),
                (16000, 1, Decimal('1')), (16001, 1.0000625, Decimal('1.0000625'))):
            with self.subTest(samples=samples):
                event = receipt()
                event.update(sample_count=samples, usage={'type': 'duration', 'seconds': seconds})
                result = self.parse(event, binding())
                self.assertEqual(result['quantity'], expected)
                self.assertEqual(result['completed_time'], datetime.datetime(2026, 6, 1, 0, 0, 2, 123456, datetime.UTC))

    def test_missing_fields_and_nonobject_receipts_are_rejected(self):
        for value in (None, False, 1, [], 'invalid'):
            with self.subTest(type=type(value).__name__), self.assertRaises(ValueError):
                self.parse(value, binding())
        for field in receipt():
            value = receipt()
            value.pop(field)
            with self.subTest(field=field), self.assertRaises(ValueError):
                self.parse(value, binding())

    def test_quantities_ranges_and_cursors_are_strictly_bounded(self):
        cases = [('sample_count', x) for x in (None, True, False, 0, -1, 1.0, '16000',
            float('nan'), float('inf'), 448001, 2**63)]
        cases += [(key, value) for key in ('start_sample', 'cursor', 'sample_rate')
            for value in (None, True, -1, 1.5, '16000')]
        cases += [('cursor', 0), ('cursor', 2**63), ('sample_rate', 24000), ('start_sample', 2**63)]
        for field, value in cases:
            with self.subTest(field=field, value=value), self.assertRaises(ValueError):
                self.parse(receipt() | {field: value}, binding())
        for seconds in (None, True, False, 0, -1, '1', float('nan'), float('inf'), 1.01):
            value = receipt()
            value['usage']['seconds'] = seconds
            with self.subTest(seconds=seconds), self.assertRaises(ValueError):
                self.parse(value, binding())

    def test_identity_provenance_and_timestamps_cannot_be_forged(self):
        cases = {'session_id': '3' * 32, 'backend_instance_id': '4' * 32,
            'event_id': 'other-event', 'segment_id': 'seg_0', 'protocol_version': 'legacy',
            'state': 'committed', 'committed_at': '2026-06-01',
            'completed_at': '2026-06-01T00:00:00Z', 'terminal_at': None,
            'provenance': {'model_id': 'other-model', 'code_revision': 'fixture'}}
        for field, value in cases.items():
            with self.subTest(field=field), self.assertRaises(ValueError):
                self.parse(receipt() | {field: value}, binding())
        for extra in ('audio', 'transcript', 'api_key'):
            with self.subTest(extra=extra), self.assertRaises(ValueError):
                self.parse(receipt() | {extra: 'must-not-be-stored'}, binding())
        huge = receipt()
        huge['provenance']['code_revision'] = 'x' * 20000
        with self.assertRaises(ValueError):
            self.parse(huge, binding())

    def test_completed_frame_and_receipt_must_agree_without_storing_text(self):
        value = completed_event()
        result = self.parse(value, binding())
        self.assertNotIn('transcript', result['receipt'])
        for field, replacement in (('usage', None), ('usage', {'seconds': 2}),
                ('item_id', 'seg_2'), ('type', 'other')):
            with self.subTest(field=field), self.assertRaises(ValueError):
                self.parse(value | {field: replacement}, binding())
        original = copy.deepcopy(value)
        self.parse(value, binding())
        self.assertEqual(value, original)

    def test_terminal_failure_is_not_billable(self):
        for state in ('failed', 'interrupted'):
            value = receipt()
            value.update(state=state, completed_at=None)
            value.pop('usage')
            result = self.parse(value, binding())
            self.assertIsNone(result['quantity'])
            self.assertIsNone(result['completed_time'])

    def test_fingerprint_is_stable_but_detects_changed_obligations(self):
        first = self.parse(receipt(), binding())
        equivalent = receipt()
        equivalent['usage']['seconds'] = 1
        self.assertEqual(first['fingerprint'], self.parse(equivalent, binding())['fingerprint'])
        changed = receipt()
        changed.update(sample_count=16001, usage={'type': 'duration', 'seconds': 1.0000625})
        self.assertNotEqual(first['fingerprint'], self.parse(changed, binding())['fingerprint'])
