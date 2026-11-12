"""Installed-package, blank-cwd SQLite onboarding/restart; no model or charges."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import socket
import sqlite3
import subprocess
import sys
import time
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen


ROOT = Path(__file__).resolve().parents[2]


class ProbeFailure(RuntimeError):
    """Only static diagnostic labels, never raw response/credential content."""


def require(value, message):
    if not value:
        raise ProbeFailure(message)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--evidence-directory', type=Path, required=True,
        help='create-only private output; retains failed logs/state/credential reference')
    args = parser.parse_args()
    os.umask(0o077)
    state = args.evidence_directory.resolve()
    state.mkdir(mode=0o700, parents=True, exist_ok=False)
    config, keyfile, db = state / 'config.toml', state / 'api-key', state / 'db.sqlite'
    environment = {key: value for key, value in os.environ.items()
        if key in ('PATH', 'HOME', 'LANG', 'LC_ALL', 'TMPDIR', 'SSL_CERT_FILE', 'SSL_CERT_DIR')}
    environment['LLMPROXY_DB_URI'] = 'sqlite://' + str(db)
    environment['PATH'] = str(Path(sys.executable).parent) + os.pathsep + environment.get('PATH', '')

    def run(command):
        result = subprocess.run(command, cwd=state, env=environment,
            text=True, capture_output=True, timeout=5)
        require(result.returncode == 0, 'Installed CLI failed (no raw credential/log output)')
        return result.stdout, result.stderr

    [origin, _] = run([sys.executable, '-c', 'import llmproxy; print(llmproxy.__file__)'])
    require(not Path(origin.strip()).is_relative_to(ROOT), 'Probe imported checkout instead of installed wheel')
    run([sys.executable, '-m', 'llmproxy', '--create-config', '-c', str(config)])
    require(config.exists() and not db.exists(), 'Create-config started a database or did not create template')
    require(config.stat().st_mode & 0o077 == 0, 'Config is not private')
    with socket.socket() as listener:
        listener.bind(('127.0.0.1', 0))
        port = listener.getsockname()[1]
    template = (ROOT / 'examples/local-sqlite.toml').read_text()
    require('port = 8080' in template, 'Example profile drifted')
    config.write_text(template.replace('port = 8080', f'port = {port}'))
    creation = run(['llmproxyctl', '-c', str(config), 'user', 'create', '--secret-file', str(keyfile),
        '--comment', 'owned-local-probe'])
    secret = keyfile.read_text().strip()
    require(len(secret) >= 64 and keyfile.stat().st_mode & 0o077 == 0, 'Credential reference is not private/valid')
    require(secret not in ''.join(creation), 'CLI emitted bearer to transcript')
    run([sys.executable, '-m', 'llmproxy.ctl', '-c', str(config), 'user', 'list'])
    for generation in (1, 2):
        with (state / f'server-{generation}.log').open('wb') as log:
            process = subprocess.Popen([sys.executable, '-m', 'llmproxy', '-c', str(config)],
                cwd=state, env=environment, stdout=log, stderr=subprocess.STDOUT)
            try:
                deadline = time.monotonic() + 5
                while True:
                    require(process.poll() is None, 'Installed server exited before authenticated request')
                    try:
                        with urlopen(f'http://127.0.0.1:{port}/v1/models', timeout=1):
                            raise ProbeFailure('Anonymous model-list unexpectedly allowed')
                    except HTTPError as error:
                        require(error.code == 401, 'Anonymous endpoint did not fail closed')
                        break
                    except URLError:
                        require(time.monotonic() < deadline, 'Server did not become available within fixed 5s budget')
                        time.sleep(0.05)
                request = Request(f'http://127.0.0.1:{port}/v1/models',
                    headers={'Authorization': 'Bearer ' + secret})
                with urlopen(request, timeout=2) as response:
                    require(response.status == 200 and json.load(response)['data'] == [], 'Authenticated local model list failed')
            finally:
                process.terminate()
                try:
                    process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait(timeout=2)
                    raise ProbeFailure('Owned server did not shut down within fixed budget') from None
    with sqlite3.connect(db) as connection:
        values = [row[0] for row in connection.execute('SELECT secret FROM api_key')]
        require(values == [hashlib.sha256(secret.encode()).hexdigest()], 'Restart duplicated/lost credential binding')
        require(connection.execute('SELECT count(*) FROM event_oneoff').fetchone()[0] == 0, 'Local probe created financial usage')
        tables = {row[0] for row in connection.execute('SELECT name FROM sqlite_master WHERE type="table"')}
        require({'accounting_session', 'conversion_delivery', 'http_attempt', 'http_attempt_financial'} <= tables,
            'Installed package missed current additive migrations')
    print(json.dumps({'status': 'passed', 'installed_outside_checkout': True, 'restarts': 1,
        'authenticated_model_lists': 2, 'anonymous_denied': 2, 'billing_rows': 0, 'credentials_printed': False}))


if __name__ == '__main__':
    try:
        main()
    except Exception as error:
        print(json.dumps({'status': 'failed', 'category': type(error).__name__,
            'check': str(error) if isinstance(error, ProbeFailure) else 'redacted'}))
        raise SystemExit(1) from None
