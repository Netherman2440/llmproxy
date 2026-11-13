FROM python:3.12-alpine@sha256:a61cbd76a882645de7f91423329fe43a67eed60d7da24f9e5331414bb3618b20 AS wheel
ENV PIP_DISABLE_PIP_VERSION_CHECK=1 PIP_ROOT_USER_ACTION=ignore
WORKDIR /build
COPY requirements-build.lock /tmp/requirements-build.lock
RUN python -m pip install --no-cache-dir --require-hashes -r /tmp/requirements-build.lock
COPY pyproject.toml MANIFEST.in ./
COPY scripts scripts
COPY llmproxy llmproxy
RUN python -m build --wheel --no-isolation

FROM python:3.12-alpine@sha256:a61cbd76a882645de7f91423329fe43a67eed60d7da24f9e5331414bb3618b20
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 PIP_ROOT_USER_ACTION=ignore \
    LLMPROXY_DB_URI=sqlite:///state/db.sqlite
RUN addgroup -S -g 10001 llmproxy \
    && adduser -S -D -H -u 10001 -G llmproxy -s /sbin/nologin llmproxy \
    && mkdir -p /app /state /etc/llmproxy \
    && chown 10001:10001 /app /state
WORKDIR /app
COPY requirements-runtime.lock /tmp/requirements-runtime.lock
RUN python -m pip install --no-cache-dir --require-hashes -r /tmp/requirements-runtime.lock \
    && rm /tmp/requirements-runtime.lock
COPY --from=wheel /build/dist /tmp/dist
RUN python -m pip install --no-cache-dir --no-deps /tmp/dist/*.whl && rm -rf /tmp/dist
COPY llmproxy/config.toml /etc/llmproxy/config.toml
USER 10001:10001
RUN python -c "import os, aiohttp, aiosqlite, motor.motor_asyncio, pymongo; import llmproxy.db; assert os.geteuid() == 10001"
EXPOSE 8080
ENTRYPOINT ["python", "-m", "llmproxy"]
CMD ["-c", "/etc/llmproxy/config.toml"]
