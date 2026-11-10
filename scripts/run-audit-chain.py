"""Start the isolated demo proxy; credentials enter only through environment."""
import asyncio
import datetime
import hashlib
import json
import os
from pathlib import Path

from llmproxy.db import SqliteDatabase


STATE = Path('/workspace/proxy-state')
CONFIG = STATE / 'config.toml'
DATABASE = STATE / 'demo.sqlite'


async def initialize():
    STATE.mkdir(parents=True, exist_ok=True)
    key = os.environ['DEMO_API_KEY']
    backend_key = os.environ['STT_API_KEY']
    db = await SqliteDatabase.create('sqlite://' + str(DATABASE))
    try:
        await db.db.execute('''INSERT INTO api_key(id,secret,type,expires,comment)
            VALUES(?,?,'LLM',?,?) ON CONFLICT(id) DO UPDATE SET
            secret=excluded.secret,expires=excluded.expires''',
            ('audit-web-demo', hashlib.sha256(key.encode()).hexdigest(),
             datetime.datetime.now(datetime.UTC) + datetime.timedelta(days=7),
             'Isolated STT demonstration; no customer billing'))
        await db.db.commit()
    finally:
        await db.close()
    text = '\n'.join([
        'host="0.0.0.0"', 'port=8080', 'log_level="INFO"',
        'timeout_connect=10', 'timeout_read=60', 'write_timeout=10',
        '[db]', 'uri=' + json.dumps('sqlite://' + str(DATABASE)),
        '[metrics]', 'enabled=false',
        '[rate_limit]', 'concurrency=2', 'rpm=60',
        '[backends.parakeet]', 'type="realtime"', 'device="a5000"',
        'model="nvidia/parakeet-tdt-0.6b-v3"',
        'url=' + json.dumps(os.environ.get('STT_URL', 'http://audit-stt-parakeet:8000')),
        'token=' + json.dumps(backend_key), 'heartbeat=10',
    ])
    descriptor = os.open(CONFIG, os.O_CREAT | os.O_TRUNC | os.O_WRONLY, 0o600)
    with os.fdopen(descriptor, 'w') as stream:
        stream.write(text)


if __name__ == '__main__':
    asyncio.run(initialize())
    # The base image defines a default URI; do not let it override this
    # deployment's explicitly initialized persistent demonstration database.
    os.environ['LLMPROXY_DB_URI'] = 'sqlite://' + str(DATABASE)
    os.execvp('python', ['python', '-m', 'llmproxy', '-c', str(CONFIG)])
