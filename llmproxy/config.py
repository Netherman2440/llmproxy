import importlib.resources
import math
import os
import re
import sys
import tomllib
from urllib.parse import urlsplit

from . import decision_adapters


class ConfigError(Exception):
    pass


# Every backend must declare one of these as its ``type``. Each endpoint
# handler passes the type it serves to proxy.request(), which rejects a model
# of any other type before the request reaches the backend.
BACKEND_TYPES = ("chat", "embedding", "transcription", "conversion",
    "decision", "realtime")

DECISION_LIMITS = {"max_questions": 16, "max_branches": 64,
    "max_response_bytes": 4 * 1024 * 1024}

# [files] options (positive integers), the limits on polling background file
# conversions; defaults live in files.py.
FILES_OPTIONS = ("max_polls_in_flight", "max_poll_response_bytes")


def _table(value, label):
    if type(value) is not dict:
        raise ConfigError("%s must be a table" % label)


def _text(value, label, *, allow_empty=False):
    if (type(value) is not str or any(ord(char) < 32 or ord(char) == 127 for char in value)
            or (not value.strip() and not (allow_empty and value == ""))):
        raise ConfigError("%s must be a valid nonempty string" % label)


def _positive_finite(value, label):
    try:
        valid = type(value) in (int, float) and value > 0 and math.isfinite(value)
    except OverflowError:
        valid = False
    if not valid:
        raise ConfigError("%s must be finite and positive" % label)


def _backend_url(value, label):
    _text(value, label)
    try:
        url = urlsplit(value)
        valid = (not any(char.isspace() for char in value)
                 and url.scheme in ("http", "https") and bool(url.hostname)
                 and url.username is None and url.password is None
                 and not url.query and not url.fragment
                 and (url.port is None or 0 < url.port <= 65535))
    except (ValueError, UnicodeError):
        valid = False
    if not valid:
        # Never echo a URL: an operator may have embedded a credential in it.
        raise ConfigError("%s requires an HTTP(S) host without credentials, query or fragment" % label)


def _admission_fields(cfg):
    options = cfg.get("admission", {})
    mode = options.get("mode", "local")
    if mode not in ("local", "shared"):
        raise ConfigError("admission.mode must be local or shared")
    if "scope" in options or mode == "shared":
        _text(options.get("scope"), "admission.scope")
        if len(options["scope"]) > 64:
            raise ConfigError("admission.scope is too long")
    ttl, io_timeout = options.get("lease_ttl", 60), options.get("io_timeout", 5)
    _positive_finite(ttl, "admission.lease_ttl")
    _positive_finite(io_timeout, "admission.io_timeout")
    if ttl > 3600 or io_timeout > ttl / 6:
        raise ConfigError("admission requires lease_ttl <=3600 and io_timeout <=lease_ttl/6")


def _realtime_fields(cfg):
    options = cfg.get('realtime', {})
    if options.get('mode', 'legacy') not in ('legacy', 'receipt-v1'):
        raise ConfigError('realtime.mode must be legacy or receipt-v1')
    capacity = options.get('max_sessions', 4)
    if type(capacity) is not int or not 1 <= capacity <= 64:
        raise ConfigError('realtime.max_sessions must be an integer in 1..64')
    for key, default, maximum in (('max_session_seconds', 3600, 3600), ('drain_timeout', 30, 30)):
        value = options.get(key, default)
        _positive_finite(value, 'realtime.' + key)
        if value > maximum:
            raise ConfigError('realtime.' + key + ' exceeds its limit')


def _global_fields(cfg):
    for key in ("db", "metrics", "rate_limit", "backends", "budget", "files", "provenance", "parsing", "admission", "realtime"):
        if key in cfg:
            _table(cfg[key], key)
    _admission_fields(cfg)
    _realtime_fields(cfg)
    parsing = cfg.get("parsing", {})
    for key in ("concurrency", "per_user"):
        if key in parsing and (type(parsing[key]) is not int or parsing[key] <= 0):
            raise ConfigError("parsing.%s must be a positive integer" % key)
    if "timeout" in parsing:
        _positive_finite(parsing["timeout"], "parsing.timeout")
    for key in ("timeout_connect", "timeout_read", "write_timeout"):
        if key in cfg:
            _positive_finite(cfg[key], key)
    for key in ("host", "http_origin"):
        if key in cfg:
            _text(cfg[key], key)
    if "port" in cfg and (type(cfg["port"]) is not int or not 1 <= cfg["port"] <= 65535):
        raise ConfigError("port must be an integer in 1..65535")
    if "log_level" in cfg and cfg["log_level"] not in (
            "DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL", "FATAL", "WARN", "NOTSET"):
        raise ConfigError("log_level must be a known logging level")
    if "db" in cfg:
        uri = cfg["db"].get("uri")
        _text(uri, "db.uri")
        try:
            valid = ((uri.startswith("sqlite://") and bool(uri[9:].strip()))
                     or (uri.startswith("mongodb://") and bool(urlsplit(uri).netloc)))
        except (ValueError, UnicodeError):
            valid = False
        if not valid:
            raise ConfigError("db.uri requires sqlite://path or mongodb://host")
    metrics = cfg.get("metrics", {})
    if set(metrics) - {'enabled', 'path', 'token_env'}:
        raise ConfigError('metrics contains unsupported options; use a credential reference')
    if 'token_env' in metrics:
        value = metrics['token_env']
        if not isinstance(value, str) or not re.fullmatch(r'[A-Za-z_][A-Za-z0-9_]{0,127}', value):
            raise ConfigError('metrics.token_env must name an environment variable')
    if "enabled" in metrics and type(metrics["enabled"]) is not bool:
        raise ConfigError("metrics.enabled must be a boolean")
    if "path" in metrics:
        path = metrics["path"]
        _text(path, "metrics.path")
        if not path.startswith("/") or "?" in path or "#" in path:
            raise ConfigError("metrics.path must be an absolute route without query or fragment")


def _validate_rate_limit(rl, prefix=""):
    """Validate a ``[rate_limit]`` / ``[backends.X.rate_limit]`` block.

    Phase 1 checks ``rpm`` and ``concurrency`` only; phase 2 appends the two
    ``*_tpd`` keys to the same loop. Each, if present, must be a non-negative
    int (``type(v) is not int`` excludes ``bool``). 0 = unlimited, NULL/absent
    = fall through. ``prefix`` labels backend errors.
    """
    _table(rl, prefix + "rate_limit")
    for key in ("rpm", "concurrency"):
        if key in rl:
            value = rl[key]
            if type(value) is not int or value < 0:
                raise ConfigError(
                    "%srate_limit.%s must be a non-negative integer" %
                    (prefix, key))


def validate(cfg):
    _table(cfg, "config")
    _global_fields(cfg)
    _validate_budget_options(cfg.get("budget", {}), "budget", global_options=True)
    for key in ("client_max_size", "max_json_body", "max_multipart_fields", "max_multipart_text_bytes",
                "max_response_bytes", "max_error_response_bytes"):
        if key in cfg:
            value = cfg[key]
            if type(value) is not int or value <= 0:
                raise ConfigError("%s must be a positive integer" % key)

    if "auth_cache_ttl" in cfg:
        value = cfg["auth_cache_ttl"]
        if type(value) is not int or value < 0:
            raise ConfigError(
                "auth_cache_ttl must be a non-negative integer")

    if "provenance" in cfg:
        # Must be a table: `provenance = true` would otherwise only blow up at
        # request time inside cfg.get("provenance", {}).get(...).
        p = cfg["provenance"]
        if type(p) is not dict:
            raise ConfigError("provenance must be a table")
        if "enabled" in p and type(p["enabled"]) is not bool:
            raise ConfigError("provenance.enabled must be a boolean")
        if "generator" in p:
            value = p["generator"]
            if type(value) is not str or not value.strip():
                raise ConfigError(
                    "provenance.generator must be a non-empty string")

    _validate_rate_limit(cfg.get("rate_limit", {}), "")

    files = cfg.get("files", {})
    if type(files) is not dict:
        raise ConfigError("files must be a table")
    if set(files) - set(FILES_OPTIONS):
        raise ConfigError("files contains unknown options: %s" %
            ", ".join(sorted(set(files) - set(FILES_OPTIONS))))
    for key, value in files.items():
        if type(value) is not int or value <= 0:
            raise ConfigError("files.%s must be a positive integer" % key)

    for name, meta in cfg.get("backends", {}).items():
        _text(name, "Backend name")
        _table(meta, 'Backend "%s"' % name)
        # Required: without it the proxy cannot tell which endpoints may reach
        # this backend (e.g. chat completions sent to an embedding model).
        if meta.get("type") not in BACKEND_TYPES:
            raise ConfigError('Backend "%s" type must be one of: %s' %
                (name, ", ".join(BACKEND_TYPES)))

        if meta["type"] == "decision":
            try:
                decision_adapters.get_adapter(meta)
            except ValueError as exc:
                raise ConfigError('Backend "%s" unsupported decision_adapter' % name) from exc
            for key in ("url", "model", "device"):
                if type(meta.get(key)) is not str or not meta[key].strip():
                    raise ConfigError('Backend "%s" %s must be a non-empty string' % (name, key))
            if "token" in meta and type(meta["token"]) is not str:
                raise ConfigError('Backend "%s" token must be a string' % name)
            if "timeout" in meta:
                _positive_finite(meta["timeout"], 'Backend "%s" timeout' % name)
        elif "decision_adapter" in meta:
            raise ConfigError('Backend "%s" decision_adapter requires type decision' % name)
        if meta["type"] == "conversion":
            # Besides authenticating the proxy to the backend, the token keys
            # the signature of background conversion ids (conversion_id.py):
            # with an empty one anybody could mint an id for any job. The
            # device names the billed product; without it a background result
            # would be claimed and then fail to bill.
            for key in ("token", "device"):
                if type(meta.get(key)) is not str or not meta[key].strip():
                    raise ConfigError('Backend "%s" %s must be a non-empty string' % (name, key))
        if meta["type"] == "realtime":
            # The token authenticates the proxy to the WebSocket backend; the
            # device names the billed product (usage is metered per second;
            # billing events follow a pending decision and key off it).
            for key in ("token", "device"):
                if type(meta.get(key)) is not str or not meta[key].strip():
                    raise ConfigError('Backend "%s" %s must be a non-empty string' % (name, key))
            if "heartbeat" in meta:
                # WS keepalive ping interval, seconds; guards long sessions
                # against silent half-open connections.
                _positive_finite(meta["heartbeat"], 'Backend "%s" heartbeat' % name)
        if "decision_limits" in meta:
            limits = meta["decision_limits"]
            if meta["type"] != "decision" or type(limits) is not dict:
                raise ConfigError('Backend "%s" decision_limits requires a decision table' % name)
            if set(limits) - set(DECISION_LIMITS):
                raise ConfigError('Backend "%s" unknown decision_limits option' % name)
            for key, value in limits.items():
                if type(value) is not int or value <= 0:
                    raise ConfigError('Backend "%s" decision_limits.%s must be a positive integer' % (name, key))

        if "max_model_len" in meta:
            value = meta["max_model_len"]
            if type(value) is not int or value <= 0:
                raise ConfigError(
                    'Backend "%s" max_model_len must be a positive integer' %
                    name)

        if "timeout" in meta:
            _positive_finite(meta["timeout"], 'Backend "%s" timeout' % name)

        _validate_rate_limit(meta.get("rate_limit", {}),
            'Backend "%s" ' % name)
        if "output_budget" in meta or "reasoning" in meta:
            if meta["type"] != "chat":
                raise ConfigError('Backend "%s" budgets require type chat' % name)
            _validate_budget_options(meta.get("output_budget", {}),
                'Backend "%s" output_budget' % name)
            _validate_reasoning(meta.get("reasoning", {}), name)
            policy = meta.get("output_budget", {})
            if meta.get("reasoning", {}).get("native_budget") and "output_budget" not in meta:
                raise ConfigError('Backend "%s" native reasoning budgets require output_budget' % name)
            default, ceiling = policy.get("default_max_tokens"), policy.get("ceiling")
            if default is not None and ceiling is not None and default > ceiling:
                raise ConfigError('Backend "%s" default budget exceeds ceiling' % name)
            if ceiling is not None and ceiling > meta.get("max_model_len", ceiling):
                raise ConfigError('Backend "%s" ceiling exceeds max_model_len' % name)
            reserve = meta.get("reasoning", {}).get("reserve_content_tokens")
            if reserve is not None and default is not None and reserve >= default:
                raise ConfigError('Backend "%s" reserve must be below default budget' % name)
        _backend_url(meta.get("url"), 'Backend "%s" url' % name)
        _text(meta.get("device"), 'Backend "%s" device' % name)
        if meta["type"] != "decision":
            _text(meta.get("token"), 'Backend "%s" token' % name, allow_empty=True)
        if meta.get("model") is not None:
            _text(meta["model"], 'Backend "%s" model' % name)
        if "verify_ssl" in meta and type(meta["verify_ssl"]) is not bool:
            raise ConfigError('Backend "%s" verify_ssl must be a boolean' % name)


def _validate_budget_options(options, prefix, global_options=False):
    if type(options) is not dict:
        raise ConfigError("%s must be a table" % prefix)
    flags = ("enabled", "echo_headers", "echo_body") if global_options else ("apply_default", "clamp")
    numbers = () if global_options else ("default_max_tokens", "ceiling", "backend_default_max_tokens")
    allowed = set(flags + numbers + (() if global_options else ("mode",)))
    if set(options) - allowed:
        raise ConfigError("%s contains unknown options: %s" %
            (prefix, ", ".join(sorted(set(options) - allowed))))
    for key in flags:
        if key in options and type(options[key]) is not bool:
            raise ConfigError("%s.%s must be a boolean" % (prefix, key))
    for key in numbers:
        if key in options and (type(options[key]) is not int or options[key] <= 0):
            raise ConfigError("%s.%s must be a positive integer" % (prefix, key))
    if not global_options:
        if options.get("mode", "observe") not in ("observe", "enforce"):
            raise ConfigError("%s.mode must be observe or enforce" % prefix)
        for flag, required in (("apply_default", "default_max_tokens"), ("clamp", "ceiling")):
            if options.get(flag) and required not in options:
                raise ConfigError("%s.%s requires %s" % (prefix, flag, required))


def _validate_reasoning(options, name):
    prefix = 'Backend "%s" reasoning' % name
    if type(options) is not dict:
        raise ConfigError("%s must be a table" % prefix)
    allowed = {"native_budget", "endpoints", "reserve_content_tokens", "effort_budgets", "default_effort"}
    if set(options) - allowed:
        raise ConfigError("%s contains unknown options" % prefix)
    if "native_budget" in options and type(options["native_budget"]) is not bool:
        raise ConfigError("%s.native_budget must be a boolean" % prefix)
    endpoints = options.get("endpoints", ["chat"])
    if (type(endpoints) is not list or not endpoints
            or endpoints != ["chat"]):
        raise ConfigError("%s.endpoints must be [chat]; native reasoning budgets "
            "are not supported on Responses/Messages" % prefix)
    reserve = options.get("reserve_content_tokens")
    if reserve is not None and (type(reserve) is not int or reserve <= 0):
        raise ConfigError("%s.reserve_content_tokens must be a positive integer" % prefix)
    efforts = options.get("effort_budgets", {})
    if type(efforts) is not dict:
        raise ConfigError("%s.effort_budgets must be a table" % prefix)
    for effort, value in efforts.items():
        if (effort not in ("minimal", "low", "medium", "high", "xhigh")
                or type(value) is not int or value <= 0):
            raise ConfigError("%s.effort_budgets requires known efforts and positive integers" % prefix)
    if "default_effort" in options and (type(options["default_effort"]) is not str
            or options["default_effort"] not in efforts):
        raise ConfigError("%s.default_effort requires a matching effort_budgets entry" % prefix)


def load(path=None, create=False):
    choices = [path, os.environ.get("LLMPROXY_CONFIG"), "config.toml"]

    for p in choices:
        if not p:
            continue

        if create:
            if not os.path.lexists(p):
                config = importlib.resources.files("llmproxy") \
                    .joinpath("config.toml").read_bytes()
                try:
                    descriptor = os.open(p, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
                    with os.fdopen(descriptor, "wb") as f:
                        f.write(config)
                    print("Created default config file", file=sys.stderr)
                except OSError as e:
                    print("Failed creating default config file:", e,
                        file=sys.stderr)
            else:
                print("Skipped creating default config because it already exists",
                    file=sys.stderr)

        with open(p, "rb") as f:
            try:
                cfg = tomllib.load(f)
            except tomllib.TOMLDecodeError as e:
                raise ConfigError("Failed parsing config: %s" % e) from e
            print("Loaded config from \"%s\"" % p, file=sys.stderr)
            cfg["_path"] = p

        if db_uri := os.environ.get("LLMPROXY_DB_URI"):
            if "db" not in cfg:
                cfg["db"] = {}
            _table(cfg["db"], "db")
            cfg["db"]["uri"] = db_uri
            print("Loaded database URI from the LLMPROXY_DB_URI env var",
                file=sys.stderr)

        validate(cfg)

        return cfg
