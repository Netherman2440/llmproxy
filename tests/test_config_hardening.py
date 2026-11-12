import copy
import logging
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import aiohttp.web

from llmproxy import config
from llmproxy.app import reload_config


BACKEND = {"type": "chat", "url": "http://inference:8000", "model": "model",
           "device": "a5000", "token": ""}


class TestConfigHardening(unittest.TestCase):
    def reject(self, cfg):
        try:
            config.validate(cfg)
        except config.ConfigError as error:
            return str(error)
        except Exception as error:
            self.fail("Malformed config raised non-domain error: " + type(error).__name__)
        self.fail("Malformed configuration was accepted")

    def backend(self, **delta):
        return {"backends": {"model": {**BACKEND, **delta}}}

    def test_top_and_nested_tables_cannot_be_scalars(self):
        for value in (None, False, [], "invalid", 3):
            with self.subTest(root_type=type(value).__name__):
                self.reject(value)
            for key in ("db", "metrics", "rate_limit", "backends", "budget", "files", "provenance", "parsing", "admission"):
                with self.subTest(table=key, value_type=type(value).__name__):
                    self.reject({key: value})

    def test_backend_and_its_nested_tables_cannot_be_scalars(self):
        for value in (None, False, [], "invalid", 3):
            with self.subTest(backend_type=type(value).__name__):
                self.reject({"backends": {"model": value}})
            for key in ("rate_limit", "output_budget", "reasoning"):
                with self.subTest(table=key, value_type=type(value).__name__):
                    self.reject(self.backend(**{key: value}))

    def test_every_timeout_is_positive_finite_and_not_boolean(self):
        for value in (True, False, 0, -1, float("nan"), float("inf"), -float("inf"), "30"):
            for key in ("timeout_connect", "timeout_read", "write_timeout"):
                with self.subTest(key=key, value=value):
                    self.reject({key: value})
            with self.subTest(backend_timeout=value):
                self.reject(self.backend(timeout=value))

    def test_parser_and_response_limits_are_positive_integers(self):
        for key in ("max_multipart_fields", "max_multipart_text_bytes",
                    "max_response_bytes", "max_error_response_bytes"):
            for value in (True, 0, -1, 1.5, "64"):
                with self.subTest(key=key, value=value):
                    self.reject({key: value})

    def test_parsing_admission_limits_and_deadline_are_validated(self):
        for key in ("concurrency", "per_user"):
            for value in (True, 0, -1, 1.5, "4"):
                with self.subTest(key=key, value=value):
                    self.reject({"parsing": {key: value}})
        for value in (None, True, 0, -1, float("inf"), float("nan"), "30"):
            with self.subTest(timeout=value):
                self.reject({"parsing": {"timeout": value}})

    def test_rate_limit_cannot_silently_claim_unsupported_account_or_daily_policy(self):
        for options in ({'rpm': 1, 'scope': 'account'}, {'group_by_owner': True}, {'prompt_tpd': 100}):
            with self.subTest(level='global', options=options):
                self.reject({'rate_limit': options})
            with self.subTest(level='model', options=options):
                self.reject(self.backend(rate_limit=options))

    def test_parser_cannot_silently_claim_account_grouping(self):
        self.reject({'parsing': {'per_user': 1, 'scope': 'account'}})

    def test_shared_admission_profile_cannot_disable_or_misspell_its_safety_bounds(self):
        cases = ({"mode": "other"}, {"mode": True}, {"mode": "shared"},
            {"scope": ""}, {"scope": 123}, {"scope": "x" * 65},
            {"lease_ttl": 0}, {"lease_ttl": True}, {"lease_ttl": float("inf")},
            {"lease_ttl": 3601}, {"io_timeout": None}, {"io_timeout": 0},
            {"io_timeout": float("nan")}, {"lease_ttl": 1, "io_timeout": 0.5})
        for options in cases:
            with self.subTest(options=options):
                self.reject({"admission": options})

    def test_valid_shared_admission_profile_is_accepted(self):
        config.validate({"admission": {"mode": "shared", "scope": "audit-test",
            "lease_ttl": 0.4, "io_timeout": 0.03}})

    def test_backend_urls_have_supported_scheme_host_and_no_credentials(self):
        for value in (None, 12, "", " ", "inference:8000", "ftp://host", "file:///etc/passwd",
                      "http:///v1", "http://user:password@host", "https://host/#fragment",
                      "https://host/?api_key=not-a-real-secret", "http://host/\nother"):
            with self.subTest(url=value):
                self.reject(self.backend(url=value))

    def test_config_errors_do_not_echo_embedded_credentials(self):
        marker = "synthetic-sensitive-marker-not-an-api-key"
        message = self.reject(self.backend(url="https://user:" + marker + "@host"))
        self.assertNotIn(marker, message)

    def test_required_backend_fields_and_scalar_types(self):
        for field in ("url", "device", "token"):
            cfg = self.backend()
            cfg["backends"]["model"].pop(field)
            with self.subTest(missing=field):
                self.reject(cfg)
        for field, value in (("url", False), ("device", 123), ("device", "  "),
                             ("token", []), ("model", True), ("verify_ssl", "false"),
                             ("verify_ssl", 0), ("model", "")):
            with self.subTest(field=field, value=value):
                self.reject(self.backend(**{field: value}))
        for name in ("", " ", "bad\nname", 12):
            with self.subTest(name=name):
                self.reject({"backends": {name: dict(BACKEND)}})

    def test_global_listener_database_and_metrics_fields_are_typed(self):
        for cfg in ({"host": False}, {"host": ""}, {"port": True}, {"port": 0}, {"port": 65536},
                    {"log_level": 2}, {"log_level": "arbitrary"}, {"http_origin": []},
                    {"db": {}}, {"db": {"uri": False}}, {"db": {"uri": "mysql://host"}},
                    {"metrics": {"enabled": "true"}}, {"metrics": {"path": "metrics"}},
                    {"metrics": {"path": "/metrics?token=invalid"}}):
            with self.subTest(cfg=cfg):
                self.reject(cfg)

    def test_huge_timeouts_never_leak_overflow_errors(self):
        huge = 2 ** 4096
        for cfg in (self.backend(type="decision", timeout=huge),
                    self.backend(type="realtime", token="synthetic", heartbeat=huge)):
            with self.subTest(type=cfg["backends"]["model"]["type"]):
                self.reject(cfg)

    def test_malformed_database_host_is_a_domain_error(self):
        self.reject({"db": {"uri": "mongodb://[invalid"}})

    def test_valid_optional_model_and_local_backend_settings_are_preserved(self):
        config.validate(self.backend())
        cfg = self.backend(model=None, verify_ssl=False, timeout=0.25)
        config.validate(cfg)
        config.validate({"db": {"uri": "sqlite:///state/db.sqlite"},
                         "host": "::1", "port": 8080, "log_level": "INFO",
                         "http_origin": "*", "metrics": {"enabled": False, "path": "/metrics"}})
        config.validate({"backends": {"stt": {"type": "realtime", "url": "http://stt:8000",
                         "token": "synthetic-only", "device": "a5000"}}})

    def test_environment_override_does_not_bypass_malformed_database_table(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "config.toml"
            path.write_text("db = true\n")
            with patch.dict(os.environ, {"LLMPROXY_DB_URI": "sqlite:///tmp/synthetic.sqlite"}):
                try:
                    config.load(path)
                except config.ConfigError:
                    pass
                except Exception as error:
                    self.fail("Environment override leaked " + type(error).__name__)
                else:
                    self.fail("Malformed database table was overwritten instead of rejected")

    def test_bad_reload_is_rejected_without_mutating_the_last_good_config(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "config.toml"
            path.write_text("backends = [1, 2]\n")
            old = self.backend()
            old["_path"] = path
            before = copy.deepcopy(old)
            app = aiohttp.web.Application(logger=logging.getLogger("test.reload"))
            app["config"] = old
            try:
                with self.assertLogs("test.reload", level="ERROR"):
                    reload_config(app)
            except Exception as error:
                self.fail("Reload leaked non-domain error: " + type(error).__name__)
            self.assertEqual(app["config"], before)


if __name__ == "__main__":
    unittest.main()
