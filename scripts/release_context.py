"""Bounded transport of committed, nonsecret Docker inputs to a CGC seed job."""
import base64
import hashlib
from pathlib import Path
import re
import shlex
import subprocess


INPUTS = ("Dockerfile", ".dockerignore", "pyproject.toml", "MANIFEST.in",
          "requirements-runtime.lock", "requirements-build.lock", "scripts", "llmproxy")
MAX_ARCHIVE = 512 * 1024
CHUNK = 32 * 1024


def _revision(value):
    if not isinstance(value, str) or not re.fullmatch(r"[0-9a-f]{40}", value):
        raise ValueError("A full committed source SHA is required")


def proxy_context(repo, revision):
    _revision(revision)
    return subprocess.check_output(["git", "archive", "--format=tar.gz", revision, "--", *INPUTS], cwd=repo)


def seed_job_data(archive, revision, destination="/workspace/llmproxy"):
    _revision(revision)
    if not isinstance(archive, bytes) or not 0 < len(archive) <= MAX_ARCHIVE:
        raise ValueError("Context exceeds the bounded transport budget")
    target = Path(destination)
    if not target.is_absolute():
        raise ValueError("An absolute scratch destination is required")
    encoded = base64.b64encode(archive).decode().rstrip("=")
    chunks = [encoded[i:i + CHUNK] for i in range(0, len(encoded), CHUNK)]
    loader = f'''import base64,hashlib,io,os,pathlib,tarfile
encoded="".join(os.environ[f"AUDIT_CONTEXT_{{i:03}}"] for i in range({len(chunks)}))
raw=base64.b64decode(encoded+"="*(-len(encoded)%4),validate=True)
if hashlib.sha256(raw).hexdigest()!={hashlib.sha256(archive).hexdigest()!r}:
    raise ValueError("Context checksum mismatch")
target=pathlib.Path({str(target)!r})
if target.exists() and any(target.iterdir()):
    raise ValueError("Scratch destination is not empty")
target.mkdir(parents=True,exist_ok=True)
with tarfile.open(fileobj=io.BytesIO(raw),mode="r:gz") as archive:
    archive.extractall(target,filter="data")
(target/".source-revision").write_text({revision!r})
(target/".context-ready").write_text("ready")
print("context-ready")
'''
    command = "python3 -c " + shlex.quote(loader)
    if len(command.encode()) >= 4096:
        raise ValueError("Seed command exceeds the argument budget")
    return {"startup_command": command,
            "environment_data": [f"AUDIT_CONTEXT_{i:03}={chunk}" for i, chunk in enumerate(chunks)]}
