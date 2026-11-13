"""Onboarding never needs a bearer value in argv or transcript."""
import hashlib
import json
import os
from pathlib import Path
import sqlite3
import subprocess
import sys
import tempfile
import unittest


ROOT = Path(__file__).resolve().parents[1]


class TestCliOnboarding(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)
        self.db = self.root / 'db.sqlite'
        self.config = self.root / 'config.toml'
        self.config.write_text('[db]\nuri=' + json.dumps('sqlite://' + str(self.db)) + '\n')
        self.env = dict(os.environ, PYTHONPATH=str(ROOT))
        self.env.pop('LLMPROXY_DB_URI', None)
        self.env.pop('LLMPROXY_CONFIG', None)

    def run_ctl(self, path):
        return subprocess.run([sys.executable, '-m', 'llmproxy.ctl', '-c', str(self.config),
            'user', 'create', '--secret-file', str(path)], cwd=self.root,
            env=self.env, capture_output=True, text=True, timeout=5)

    def test_create_config_does_not_start_server_or_create_database(self):
        path = self.root / 'new.toml'
        script = """import sys
from llmproxy import app
async def forbidden(*a, **k):
    raise AssertionError('Configuration creation must not start the service')
app.create_app = forbidden
sys.argv = ['llmproxy', '--create-config', '-c', sys.argv[1]]
app.main()
"""
        result = subprocess.run([sys.executable, '-c', script, str(path)],
            cwd=self.root, env=self.env, capture_output=True, text=True, timeout=5)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertTrue(path.is_file())
        self.assertEqual(path.stat().st_mode & 0o077, 0)
        self.assertFalse(self.db.exists())

    def test_new_credential_is_referenced_by_private_file_not_stdout(self):
        path = self.root / 'api-key'
        result = self.run_ctl(path)
        self.assertEqual(result.returncode, 0, result.stderr)
        secret = path.read_text().strip()
        self.assertGreaterEqual(len(secret), 64)
        self.assertEqual(path.stat().st_mode & 0o077, 0)
        self.assertNotIn(secret, result.stdout + result.stderr)
        with sqlite3.connect(self.db) as connection:
            values = [row[0] for row in connection.execute('SELECT secret FROM api_key')]
        self.assertEqual(values, [hashlib.sha256(secret.encode()).hexdigest()])

    def test_existing_credential_file_is_not_overwritten_or_new_key_created(self):
        path = self.root / 'api-key'
        path.write_text('existing-protected-reference\n')
        result = self.run_ctl(path)
        self.assertNotEqual(result.returncode, 0)
        self.assertEqual(path.read_text(), 'existing-protected-reference\n')
        self.assertNotIn('existing-protected-reference', result.stdout + result.stderr)
        if self.db.exists():
            with sqlite3.connect(self.db) as connection:
                self.assertEqual(connection.execute('SELECT count(*) FROM api_key').fetchone()[0], 0)
