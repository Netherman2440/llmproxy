"""Run inside the candidate image, not on the host or a model-free substitute."""
import importlib.metadata
import json
import os
from pathlib import Path
import socket
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request


STAGE = "runtime_identity"


def smoke():
    global STAGE
    assert os.geteuid() == 10001 and os.getegid() == 10001
    expected = {"aiohttp": "3.14.5", "aiosqlite": "0.22.1", "motor": "3.7.1",
                "pymongo": "4.19.0", "prometheus-client": "0.26.0", "yarl": "1.25.1"}
    for name, version in expected.items():
        assert importlib.metadata.version(name) == version
    assert os.access("/state", os.W_OK)
    STAGE = "installed_entrypoint"
    result = subprocess.run([sys.executable, "-m", "llmproxy", "--help"],
                            capture_output=True, timeout=15)
    assert result.returncode == 0
    STAGE = "authenticated_http_and_sqlite"
    with tempfile.TemporaryDirectory() as folder:
        root = Path(folder)
        with socket.socket() as listener:
            listener.bind(("127.0.0.1", 0))
            port = listener.getsockname()[1]
        database = root / "state.sqlite"
        config = root / "config.toml"
        config.write_text(f'''log_level = "ERROR"
host = "127.0.0.1"
port = {port}
http_origin = "http://localhost"
timeout_connect = 5
timeout_read = 60
[db]
uri = "sqlite://{database}"
[backends]
''')
        env = {**os.environ, "LLMPROXY_DB_URI": "sqlite://" + str(database)}
        with (root / "server.log").open("wb") as output:
            process = subprocess.Popen([sys.executable, "-m", "llmproxy", "-c", str(config)],
                                       env=env, stdout=output, stderr=output)
            try:
                deadline = time.monotonic() + 20
                while True:
                    if process.poll() is not None:
                        raise AssertionError("Installed server exited before responding")
                    request = urllib.request.Request(f"http://127.0.0.1:{port}/v1/models",
                        headers={"Authorization": "Bearer synthetic-image-smoke-not-a-real-key"})
                    try:
                        urllib.request.urlopen(request, timeout=1).close()
                        raise AssertionError("Unauthenticated models unexpectedly accepted")
                    except urllib.error.HTTPError as error:
                        assert error.code == 401
                        break
                    except (urllib.error.URLError, TimeoutError):
                        if time.monotonic() >= deadline:
                            raise
                        time.sleep(0.1)
                assert database.is_file()
            finally:
                process.terminate()
                try:
                    process.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait(timeout=5)
    print(json.dumps({"status": "PASS", "runtime_uid": os.geteuid(),
        "versions": expected, "installed_cli": True, "http_denies_invalid_key": True,
        "sqlite_created_by_runtime": True, "state_writable": True}), flush=True)


if __name__ == "__main__":
    try:
        smoke()
    except Exception as error:
        print(json.dumps({"status": "FAIL", "stage": STAGE,
                          "exception_type": type(error).__name__,
                          "reason": str(error)[:160] if isinstance(error, AssertionError) else None}), flush=True)
        sys.exit(1)
