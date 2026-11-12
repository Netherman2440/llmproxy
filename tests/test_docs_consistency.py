"""Documentation drift gates: documented finance shape and file links must exist."""
import re
from pathlib import Path
import unittest


ROOT = Path(__file__).resolve().parents[1]


class TestDocsConsistency(unittest.TestCase):
    def test_completion_logging_matches_actual_mongo_document_shape(self):
        text = (ROOT / 'README.md').read_text()
        section = text.split('## Completion logging', 1)[1].split('\n## ', 1)[0]
        self.assertIn('cgc.billing_record', section)
        self.assertIn('resources', section)
        self.assertNotIn('events_oneoff', section)
        self.assertIn('created_at', section)

    def test_documented_relative_links_exist(self):
        missing = []
        for _, target in re.findall(r'\[([^\]]+)\]\(([^)#?\s]+)\)',
                (ROOT / 'README.md').read_text()):
            if re.match(r'^[a-z]+://', target):
                continue
            if target.startswith('#') or '/' not in target and '.' not in target:
                continue
            if not (ROOT / target).is_file():
                missing.append(target)
        self.assertEqual(missing, [], 'README links to files that do not exist')

    def test_template_comments_match_runtime_behaviour(self):
        text = (ROOT / 'llmproxy' / 'config.toml').read_text()
        self.assertNotIn('billing events are not written yet', text)
        self.assertIn('billed as one ONEOFF event per completed', text)
