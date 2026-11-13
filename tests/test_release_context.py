import base64
import importlib
import io
import os
from pathlib import Path
import subprocess
import tarfile
import tempfile
import unittest


class TestReleaseContext(unittest.TestCase):
    def module(self):
        try:
            module = importlib.import_module("scripts.release_context")
        except ModuleNotFoundError:
            module = None
        self.assertIsNotNone(module, "Bounded release context transport is missing")
        return module

    def archive(self):
        data = io.BytesIO()
        with tarfile.open(fileobj=data, mode="w:gz") as archive:
            content = b"synthetic context data"
            item = tarfile.TarInfo("example.txt")
            item.size = len(content)
            archive.addfile(item, io.BytesIO(content))
        return data.getvalue()

    def test_context_mount_is_separate_from_docker_build_workdir(self):
        module = self.module()
        config = module.seed_job_data(self.archive(), "e" * 40)
        self.assertIn("/workspace/llmproxy", config["startup_command"])
        self.assertNotIn("/build/llmproxy", config["startup_command"])

    def test_large_context_is_split_in_environment_not_a_single_argument(self):
        module = self.module()
        config = module.seed_job_data(os.urandom(300_000), "a" * 40)
        self.assertLess(len(config["startup_command"].encode()), 4096)
        self.assertGreater(len(config["environment_data"]), 1)
        pieces = []
        for entry in config["environment_data"]:
            self.assertLess(len(entry.encode()), 64 * 1024)
            pieces.append(entry.split("=", 1)[1])
        self.assertEqual(len(base64.b64decode("".join(pieces))), 300_000)

    def test_committed_build_context_excludes_reports_and_credentials(self):
        module = self.module()
        root = Path(__file__).resolve().parents[1]
        revision = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=root, text=True).strip()
        blob = module.proxy_context(root, revision)
        with tarfile.open(fileobj=io.BytesIO(blob), mode="r:gz") as archive:
            names = set(archive.getnames())
        self.assertIn("Dockerfile", names)
        self.assertIn("requirements-runtime.lock", names)
        self.assertFalse(any(name.startswith(("specs/", "reports/", "secrets/", ".git/")) for name in names))

    def test_loader_extracts_verified_archive_and_writes_ready_marker(self):
        module = self.module()
        with tempfile.TemporaryDirectory() as folder:
            target = Path(folder) / "context"
            config = module.seed_job_data(self.archive(), "b" * 40, target)
            env = {**os.environ, **dict(x.split("=", 1) for x in config["environment_data"])}
            result = subprocess.run(["sh", "-c", config["startup_command"]], env=env, capture_output=True)
            self.assertEqual(result.returncode, 0, result.stderr.decode())
            self.assertEqual((target / "example.txt").read_bytes(), b"synthetic context data")
            self.assertTrue((target / ".context-ready").is_file())
            self.assertEqual((target / ".source-revision").read_text(), "b" * 40)

    def test_tampering_or_oversized_context_cannot_be_marked_ready(self):
        module = self.module()
        with self.assertRaises(ValueError):
            module.seed_job_data(b"x" * (1024 * 1024), "c" * 40)
        with tempfile.TemporaryDirectory() as folder:
            target = Path(folder) / "context"
            config = module.seed_job_data(self.archive(), "d" * 40, target)
            env = {**os.environ, **dict(x.split("=", 1) for x in config["environment_data"])}
            env["AUDIT_CONTEXT_000"] = "eA=="
            result = subprocess.run(["sh", "-c", config["startup_command"]], env=env, capture_output=True)
            self.assertNotEqual(result.returncode, 0)
            self.assertFalse((target / ".context-ready").exists())


if __name__ == "__main__":
    unittest.main()
