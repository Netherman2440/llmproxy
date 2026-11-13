"""Real Mongo lease ownership and cross-process gate; audit namespace only."""

import asyncio
import datetime
import json
import multiprocessing
import os
import sys
import uuid
from urllib.parse import quote


STAGE = "scope"


def adapter_uri():
    if (os.environ.get("MONGO_HOST") != "audit-stt-mongo"
            or os.environ.get("AUDIT_NAMESPACE") not in ("izajac", "ci-audit")):
        raise ValueError("Explicit private audit target required")
    return ("mongodb://" + quote(os.environ["MONGO_ADMIN_USERNAME"], safe="") + ":"
        + quote(os.environ["MONGO_ADMIN_PASSWORD"], safe="")
        + "@audit-stt-mongo:27017/?authSource=admin&replicaSet=auditstt")


def race_child(bucket, owner, barrier, release, results):
    async def run():
        from llmproxy.db import MongoDatabase, shutdown_all
        try:
            adapter = await MongoDatabase.create(adapter_uri())
            await asyncio.to_thread(barrier.wait, 5)
            acquired = await adapter.admission_acquire(bucket, owner, 1, 30)
            results.put(acquired)
            await asyncio.to_thread(release.wait, 5)
            if acquired:
                await adapter.admission_release(bucket, owner)
        finally:
            await shutdown_all()
    try:
        asyncio.run(run())
    except Exception as error:
        print(json.dumps({"worker_status": "FAIL", "exception_type": type(error).__name__}), flush=True)
        raise SystemExit(1)


async def probe():
    global STAGE
    from llmproxy.db import MongoDatabase, shutdown_all
    prefix = "audit-admission-" + uuid.uuid4().hex
    adapter = await MongoDatabase.create(adapter_uri())
    collection = adapter.db.cgc.admission_bucket
    try:
        STAGE = "adapter_api"
        for name in ("admission_acquire", "admission_renew", "admission_release"):
            assert callable(getattr(adapter, name, None)), "Missing adapter-owned lease API"
        STAGE = "owner_fencing"
        bucket = prefix + "-owner"
        assert await adapter.admission_acquire(bucket, "old-owner", 1, 30)
        assert not await adapter.admission_release(bucket, "wrong-owner")
        assert not await adapter.admission_acquire(bucket, "new-owner", 1, 30)
        assert await adapter.admission_renew(bucket, "old-owner", 60)
        assert await adapter.admission_release(bucket, "old-owner")
        assert await adapter.admission_acquire(bucket, "new-owner", 1, 30)
        assert not await adapter.admission_release(bucket, "old-owner")
        assert not await adapter.admission_acquire(bucket, "third-owner", 1, 30)
        STAGE = "expired_owner"
        expired = prefix + "-expiry"
        assert await adapter.admission_acquire(expired, "expired-owner", 1, 30)
        await collection.update_one({"_id": expired}, {"$set": {
            "holders.$[].expires_at": datetime.datetime(1970, 1, 1, tzinfo=datetime.UTC)}})
        assert not await adapter.admission_renew(expired, "expired-owner", 30)
        assert await adapter.admission_acquire(expired, "replacement", 1, 30)
        assert not await adapter.admission_release(expired, "expired-owner")
        assert not await adapter.admission_acquire(expired, "third-owner", 1, 30)
        STAGE = "two_process_contention"
        ctx = multiprocessing.get_context("spawn")
        barrier, release, results = ctx.Barrier(2), ctx.Event(), ctx.Queue()
        processes = [ctx.Process(target=race_child,
            args=(prefix + "-race", owner, barrier, release, results))
            for owner in ("process-a", "process-b")]
        try:
            for process in processes:
                process.start()
            answers = [await asyncio.to_thread(results.get, True, 10) for _ in processes]
            assert sorted(answers) == [False, True]
        finally:
            release.set()
            for process in processes:
                await asyncio.to_thread(process.join, 5)
                if process.is_alive():
                    process.terminate()
                    await asyncio.to_thread(process.join, 5)
            results.close()
        assert all(process.exitcode == 0 for process in processes)
        print(json.dumps({"status": "PASS", "namespace": os.environ["AUDIT_NAMESPACE"],
            "owner_fencing": True, "expired_owner_not_revived": True,
            "two_processes_one_winner": True}), flush=True)
    finally:
        # Delete only documents named with this probe's fresh, owned prefix.
        await collection.delete_many({"_id": {"$in": [prefix + suffix for suffix in ("-owner", "-expiry", "-race")]}})
        await shutdown_all()


if __name__ == "__main__":
    try:
        asyncio.run(probe())
    except Exception as error:
        # No driver exception text, URI, credentials or full traceback.
        print(json.dumps({"status": "FAIL", "stage": STAGE,
            "exception_type": type(error).__name__, "code": getattr(error, "code", None)}), flush=True)
        sys.exit(1)
