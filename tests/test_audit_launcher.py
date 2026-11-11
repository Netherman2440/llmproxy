import os
from pathlib import Path
import runpy
import sqlite3
import tempfile
import tomllib
import unittest
from unittest.mock import patch


class TestAuditLauncher(unittest.IsolatedAsyncioTestCase):
    async def test_launcher_enables_receipts_without_replacing_historical_billing(self):
        module = runpy.run_path(str(Path(__file__).resolve().parents[1] / 'scripts/run-audit-chain.py'))
        initialize = module['initialize']
        with tempfile.TemporaryDirectory() as directory:
            state = Path(directory)
            initialize.__globals__.update(STATE=state, CONFIG=state / 'config.toml', DATABASE=state / 'demo.sqlite')
            with patch.dict(os.environ, {'DEMO_API_KEY': 'synthetic-demo-auth',
                    'STT_API_KEY': 'synthetic-backend-auth'}):
                await initialize()
                with sqlite3.connect(state / 'demo.sqlite') as connection:
                    connection.execute('''INSERT INTO event_oneoff(created,api_key,product,quantity,rid)
                        VALUES('2026-06-01','audit-web-demo','parakeet/a5000/transcription',13,'historical')''')
                await initialize()
            config = tomllib.loads((state / 'config.toml').read_text())
            self.assertEqual(config.get('realtime', {}).get('mode'), 'receipt-v1')
            self.assertEqual(config['db']['uri'], 'sqlite://' + str(state / 'demo.sqlite'))
            self.assertEqual((state / 'config.toml').stat().st_mode & 0o077, 0)
            with sqlite3.connect(state / 'demo.sqlite') as connection:
                self.assertEqual(connection.execute('SELECT quantity,rid FROM event_oneoff').fetchall(), [(13, 'historical')])
                self.assertEqual(connection.execute('SELECT COUNT(*) FROM api_key').fetchone()[0], 1)
