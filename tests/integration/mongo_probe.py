"""Explicit real-Mongo gate for the isolated CGC audit namespace; never a unit-test skip.

Run inside a CGC job. Credentials are injected through namespaced environment_data
from a protected file reference; no credentials or connection URI are printed.
"""
import asyncio
import datetime
from decimal import Decimal
import hashlib
import importlib.metadata
import json
import os
import sys
import uuid
from urllib.parse import quote


STAGE = "imports"


async def probe():
    global STAGE
    import bson
    from motor.motor_asyncio import AsyncIOMotorClient
    from pymongo.errors import OperationFailure
    from llmproxy.db import MongoDatabase, shutdown_all

    host = os.environ["MONGO_HOST"]
    if host != "audit-stt-mongo" or os.environ["AUDIT_NAMESPACE"] != "izajac":
        raise ValueError("Probe is restricted to the isolated audit resource")
    username = os.environ["MONGO_ADMIN_USERNAME"]
    password = os.environ["MONGO_ADMIN_PASSWORD"]
    options = dict(username=username, password=password, authSource="admin",
                   serverSelectionTimeoutMS=3000, tz_aware=True)
    direct = AsyncIOMotorClient(host, directConnection=True, **options)
    client = None
    try:
        STAGE = "authenticated_direct_connection"
        for attempt in range(30):
            try:
                await direct.admin.command("ping")
                break
            except Exception:
                if attempt == 29:
                    raise
                await asyncio.sleep(1)

        STAGE = "initialize_private_replica_set"
        try:
            await direct.admin.command("replSetGetStatus")
        except OperationFailure as error:
            if error.code != 94:
                raise
            await direct.admin.command("replSetInitiate", {
                "_id": "auditstt", "members": [{"_id": 0, "host": host + ":27017"}]})
        client = AsyncIOMotorClient(host, replicaSet="auditstt", **options)
        STAGE = "primary_ready"
        for attempt in range(30):
            try:
                if (await client.admin.command("hello")).get("isWritablePrimary"):
                    break
            except Exception:
                if attempt == 29:
                    raise
            await asyncio.sleep(1)
        else:
            raise AssertionError("Replica set never became primary")

        STAGE = "real_adapter_auth_expiry"
        # The existing adapter accepts a URI. It stays in process memory only;
        # never persisted in config, passed as a command argument, or logged.
        uri = ("mongodb://" + quote(username, safe="") + ":" + quote(password, safe="")
               + "@" + host + ":27017/?authSource=admin&replicaSet=auditstt")
        adapter = await MongoDatabase.create(uri)
        assert await MongoDatabase.create(uri) is adapter
        now = datetime.datetime.now(datetime.UTC)
        digest = hashlib.sha256(b"synthetic-audit-probe-not-a-real-api-key").hexdigest()
        await client.cgc.rest_users.replace_one({"_id": "audit-probe-user"}, {
            "_id": "audit-probe-user", "namespace": "audit-fixture",
            "org_id": "audit-probe-org", "subscription_level": "STANDARD"}, upsert=True)
        key = {"_id": "audit-probe-key", "user_id": "audit-probe-user",
               "access_level": "LLM", "secret": digest,
               "date_expiry": now + datetime.timedelta(hours=1)}
        await client.cgc.api_keys.replace_one({"_id": key["_id"]}, key, upsert=True)
        users = await adapter.user_list(digest)
        assert len(users) == 1
        await client.cgc.api_keys.update_one({"_id": key["_id"]}, {
            "$set": {"date_expiry": now - datetime.timedelta(minutes=1)}})
        assert await adapter.user_list(digest) == []

        STAGE = "real_billing_record_and_decimal128"
        request_id = "audit-probe-" + uuid.uuid4().hex
        product = "audit-stt/a5000/transcription"
        await adapter.billing_record_add(users[0], now,
                                         {product: Decimal("1.0000625")}, request_id)
        record = await client.cgc.billing_record.find_one({"request_id": request_id})
        assert isinstance(record["_id"], bson.ObjectId)
        assert isinstance(record["resources"][product], bson.decimal128.Decimal128)
        assert record["resources"][product].to_decimal() == Decimal("1.0000625")
        await adapter.close()
        assert await adapter.user_list(digest) == []  # request close keeps the pool usable

        STAGE = "real_transaction_rollback"
        transaction_id = "audit-abort-" + uuid.uuid4().hex
        async with await client.start_session() as session:
            try:
                async with session.start_transaction():
                    await client.cgc.audit_probe_ledger.insert_one({"_id": transaction_id}, session=session)
                    await client.cgc.audit_probe_usage.insert_one({"_id": transaction_id}, session=session)
                    raise RuntimeError("injected rollback")
            except RuntimeError:
                pass
        assert await client.cgc.audit_probe_ledger.find_one({"_id": transaction_id}) is None
        assert await client.cgc.audit_probe_usage.find_one({"_id": transaction_id}) is None

        STAGE = "reject_bad_credentials"
        bad = AsyncIOMotorClient(host, directConnection=True,
            **{**options, "password": password + "-invalid"})
        try:
            try:
                await bad.admin.command("ping")
            except OperationFailure as error:
                assert error.code == 18
            else:
                raise AssertionError("Unauthenticated database access was accepted")
        finally:
            bad.close()
        version = (await client.admin.command("buildInfo"))["version"]
        print(json.dumps({"status": "PASS", "namespace": "izajac", "mongo": version,
            "motor": importlib.metadata.version("motor"),
            "pymongo": importlib.metadata.version("pymongo"), "replica_set": "auditstt",
            "adapter_connection_and_cache": True, "same_day_expiry": True,
            "bson_objectid": True, "exact_decimal128": True,
            "transaction_rollback": True, "bad_auth_rejected": True}), flush=True)
    finally:
        direct.close()
        if client is not None:
            client.close()
        await shutdown_all()


if __name__ == "__main__":
    try:
        asyncio.run(probe())
    except Exception as error:
        # Driver exception text can contain connection details. Emit only
        # bounded diagnostics, never the URI, credential, or full traceback.
        print(json.dumps({"status": "FAIL", "stage": STAGE,
                          "exception_type": type(error).__name__,
                          "code": getattr(error, "code", None)}), flush=True)
        sys.exit(1)
