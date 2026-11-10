FROM python:3.12-slim-bookworm@sha256:0f086103b026048cc50cb5da25ec435c023c5241a83fc44daea593377abc61c4 AS wheel
ENV PIP_DISABLE_PIP_VERSION_CHECK=1 PIP_ROOT_USER_ACTION=ignore
WORKDIR /build
COPY requirements-build.lock /tmp/requirements-build.lock
RUN python -m pip install --no-cache-dir --require-hashes -r /tmp/requirements-build.lock
COPY pyproject.toml MANIFEST.in ./
COPY scripts scripts
COPY llmproxy llmproxy
RUN python -m build --wheel --no-isolation

FROM python:3.12-slim-bookworm@sha256:0f086103b026048cc50cb5da25ec435c023c5241a83fc44daea593377abc61c4
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 PIP_ROOT_USER_ACTION=ignore \
    LLMPROXY_DB_URI=sqlite:///state/db.sqlite
RUN groupadd --gid 10001 llmproxy \
    && useradd --uid 10001 --gid 10001 --no-create-home --shell /usr/sbin/nologin llmproxy \
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
