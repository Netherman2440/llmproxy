"""Static release guardrails; actual build/smoke/scan remain separate gates."""
from pathlib import Path
import re
import tomllib
import unittest


ROOT = Path(__file__).resolve().parents[1]


class TestContainerRelease(unittest.TestCase):
    def setUp(self):
        self.docker = (ROOT / "Dockerfile").read_text()

    def test_python_image_is_pinned_and_matches_supported_runtime(self):
        images = re.findall(r"^FROM\s+(\S+)", self.docker, re.MULTILINE)
        self.assertTrue(images)
        for image in images:
            self.assertRegex(image, r"^python:3\.12[^@]*@sha256:[0-9a-f]{64}$")

    def test_runtime_install_uses_the_same_hashed_lock_as_tests(self):
        self.assertIn("COPY requirements-runtime.lock", self.docker)
        self.assertIn("--require-hashes -r", self.docker)
        self.assertNotIn("apk add", self.docker)
        self.assertNotIn("--break-system-packages", self.docker)
        self.assertNotRegex(self.docker, r"motor==3\.5\.")

    def test_nonroot_runtime_has_persistent_writable_state(self):
        self.assertRegex(self.docker, r"(?m)^USER 10001:10001$")
        self.assertIn("/state", self.docker)
        self.assertIn("LLMPROXY_DB_URI=sqlite:///state/db.sqlite", self.docker)
        self.assertNotIn("WORKDIR /root", self.docker)

    def test_build_context_excludes_secret_and_audit_artifacts(self):
        path = ROOT / ".dockerignore"
        self.assertTrue(path.is_file(), "Build context hygiene file is missing")
        patterns = set(path.read_text().splitlines())
        self.assertTrue({".git", ".superpowers", "secrets", ".env", "reports"} <= patterns)

    def test_repository_ignores_generated_secrets_and_image_artifacts(self):
        patterns = set((ROOT / ".gitignore").read_text().splitlines())
        self.assertTrue({"secrets/", "state/", ".env", "image.tar", "trivy.json"} <= patterns)

    def test_packaging_build_inputs_have_a_hashed_lock(self):
        lock = ROOT / "requirements-build.lock"
        self.assertTrue(lock.is_file(), "Build dependency hashes are missing")
        text = lock.read_text()
        cfg = tomllib.loads((ROOT / "pyproject.toml").read_text())
        for requirement in cfg["build-system"]["requires"]:
            self.assertIn(requirement, text)
        self.assertIn("--hash=sha256:", text)
        self.assertIn("requirements-build.lock", self.docker)


class TestReleaseWorkflows(unittest.TestCase):
    def test_github_verifies_but_never_publishes_public_images(self):
        text = (ROOT / ".github/workflows/image.yml").read_text()
        self.assertIn("pull_request:", text)
        self.assertIn("python -m unittest discover", text)
        self.assertIn("requirements-runtime.lock", text)
        self.assertNotIn("ghcr.io", text)
        self.assertNotRegex(text, r"push:\s*true")
        self.assertIn("contents: read", text)

    def test_github_actions_are_immutable(self):
        text = (ROOT / ".github/workflows/image.yml").read_text()
        actions = re.findall(r"\buses:\s*(\S+)", text)
        self.assertTrue(actions)
        for action in actions:
            self.assertRegex(action, r"^[\w./-]+@[0-9a-f]{40}$")

    def test_gitlab_publication_requires_private_project_and_manual_protected_tag(self):
        text = (ROOT / ".gitlab-ci.yml").read_text()
        self.assertIn('CI_PROJECT_VISIBILITY == "private"', text)
        self.assertIn('CI_COMMIT_REF_PROTECTED == "true"', text)
        self.assertIn("when: manual", text)
        self.assertIn("requirements-runtime.lock", text)
        self.assertIn("python -m unittest discover", text)
        self.assertIn("--exit-code 1", text)
        self.assertIn("--severity HIGH,CRITICAL", text)

    def test_compose_requires_explicit_image_and_persists_sqlite(self):
        text = (ROOT / "compose.yaml").read_text()
        self.assertIn("${LLMPROXY_IMAGE:?", text)
        self.assertIn("127.0.0.1:8080:8080", text)
        self.assertIn("llmproxy-state:/state", text)
        self.assertIn("LLMPROXY_DB_URI: sqlite:///state/db.sqlite", text)
        self.assertNotIn(":master", text)
        self.assertNotIn(":latest", text)


class TestMongoProbeScope(unittest.TestCase):
    def test_only_explicit_audit_namespaces_are_accepted(self):
        from tests.integration import mongo_probe
        check = getattr(mongo_probe, "validate_target", None)
        self.assertIsNotNone(check, "Integration scope validator is missing")
        for namespace in ("izajac", "ci-audit"):
            self.assertEqual(check({"MONGO_HOST": "audit-stt-mongo", "AUDIT_NAMESPACE": namespace}), namespace)

    def test_other_hosts_or_namespaces_are_rejected_before_connection(self):
        from tests.integration import mongo_probe
        check = getattr(mongo_probe, "validate_target", None)
        self.assertIsNotNone(check, "Integration scope validator is missing")
        for env in ({"MONGO_HOST": "production-db", "AUDIT_NAMESPACE": "ci-audit"},
                    {"MONGO_HOST": "audit-stt-mongo", "AUDIT_NAMESPACE": "production"}, {}):
            with self.subTest(env=env), self.assertRaises(ValueError):
                check(env)


if __name__ == "__main__":
    unittest.main()
