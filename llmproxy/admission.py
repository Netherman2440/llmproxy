"""Adapter-owned expiring leases, independent from process-local counters."""

import datetime
import math


def validate_lease(bucket, owner, ttl=None, limit=None):
    for name, value, size in (("bucket", bucket, 256), ("owner", owner, 128)):
        if (type(value) is not str or not value or len(value) > size
                or any(ord(char) < 32 or ord(char) == 127 for char in value)):
            raise ValueError("Invalid lease " + name)
    if ttl is not None:
        try:
            valid = type(ttl) in (int, float) and 0 < ttl <= 3600 and math.isfinite(ttl)
        except OverflowError:
            valid = False
        if not valid:
            raise ValueError("Lease lifetime must be finite, positive and at most3600s")
    if limit is not None and (type(limit) is not int or limit <= 0):
        raise ValueError("Lease capacity must be a positive integer")


def _active_holders():
    return {"$filter": {"input": "$holders", "as": "holder",
        "cond": {"$gt": ["$$holder.expires_at", "$$NOW"]}}}


def _has_owner(owner):
    return {"$in": [{"$literal": owner}, {"$map": {"input": _active_holders(),
        "as": "holder", "in": "$$holder.owner"}}]}


def _holder_expiry_stage():
    return {"$set": {"expires_at": {"$ifNull": [{"$max": "$holders.expires_at"}, "$$NOW"]}}}


class MongoAdmission:
    async def _admission_collection(self):
        collection = self.db["cgc"]["admission_bucket"]
        if not getattr(self, "_admission_index_ready", False):
            await collection.create_index("expires_at", expireAfterSeconds=0,
                name="admission_bucket_expiry")
            self._admission_index_ready = True
        return collection

    async def admission_acquire(self, bucket, owner, limit, ttl):
        from pymongo.errors import DuplicateKeyError
        validate_lease(bucket, owner, ttl, limit)
        if ttl is None or limit is None:
            raise ValueError("Lease lifetime and capacity are required")
        collection = await self._admission_collection()
        try:
            await collection.update_one({"_id": bucket}, {"$setOnInsert": {
                "holders": [], "expires_at": datetime.datetime(1970, 1, 1, tzinfo=datetime.UTC)}}, upsert=True)
        except DuplicateKeyError:
            # A competing process initialized the same bucket first.
            pass
        remaining = {"$filter": {"input": _active_holders(), "as": "holder",
            "cond": {"$ne": ["$$holder.owner", {"$literal": owner}]}}}
        holder = {"owner": {"$literal": owner}, "expires_at": {"$add": ["$$NOW", math.ceil(ttl * 1000)]}}
        result = await collection.find_one_and_update({"_id": bucket, "$expr": {"$or": [
            _has_owner(owner), {"$lt": [{"$size": _active_holders()}, limit]}]}},
            [{"$set": {"holders": {"$concatArrays": [remaining, [holder]]}}}, _holder_expiry_stage()],
            projection={"_id": 1})
        return result is not None

    async def admission_renew(self, bucket, owner, ttl):
        validate_lease(bucket, owner, ttl)
        if ttl is None:
            raise ValueError("Lease lifetime is required")
        collection = await self._admission_collection()
        holders = {"$map": {"input": _active_holders(), "as": "holder", "in": {"$cond": [
            {"$eq": ["$$holder.owner", {"$literal": owner}]},
            {"owner": {"$literal": owner}, "expires_at": {"$add": ["$$NOW", math.ceil(ttl * 1000)]}},
            "$$holder"]}}}
        result = await collection.find_one_and_update({"_id": bucket, "$expr": _has_owner(owner)},
            [{"$set": {"holders": holders}}, _holder_expiry_stage()], projection={"_id": 1})
        return result is not None

    async def admission_release(self, bucket, owner):
        validate_lease(bucket, owner)
        collection = await self._admission_collection()
        holders = {"$filter": {"input": _active_holders(), "as": "holder",
            "cond": {"$ne": ["$$holder.owner", {"$literal": owner}]}}}
        result = await collection.find_one_and_update({"_id": bucket, "$expr": _has_owner(owner)},
            [{"$set": {"holders": holders}}, _holder_expiry_stage()], projection={"_id": 1})
        return result is not None


class SqliteAdmission:
    async def _lease_now(self):
        row = await (await self.db.execute(
            "SELECT (julianday('now')-2440587.5)*86400.0 AS now")).fetchone()
        return row["now"]

    async def admission_acquire(self, bucket, owner, limit, ttl):
        validate_lease(bucket, owner, ttl, limit)
        if ttl is None or limit is None:
            raise ValueError("Lease lifetime and capacity are required")
        await self.db.execute("BEGIN IMMEDIATE")
        try:
            now = await self._lease_now()
            await self.db.execute("DELETE FROM admission_lease WHERE bucket=? AND expires_at<=?", (bucket, now))
            await self.db.execute("""DELETE FROM admission_lease WHERE rowid IN
                (SELECT rowid FROM admission_lease WHERE expires_at<=? LIMIT 256)""", (now,))
            existing = await (await self.db.execute(
                "SELECT 1 FROM admission_lease WHERE bucket=? AND owner=?", (bucket, owner))).fetchone()
            count = await (await self.db.execute(
                "SELECT count(*) AS n FROM admission_lease WHERE bucket=?", (bucket,))).fetchone()
            acquired = existing is not None or count["n"] < limit
            if acquired:
                await self.db.execute("""INSERT INTO admission_lease(bucket,owner,expires_at) VALUES(?,?,?)
                    ON CONFLICT(bucket,owner) DO UPDATE SET expires_at=excluded.expires_at""",
                    (bucket, owner, now + ttl))
            await self.db.commit()
            return acquired
        except BaseException:
            await self.db.rollback()
            raise

    async def admission_renew(self, bucket, owner, ttl):
        validate_lease(bucket, owner, ttl)
        if ttl is None:
            raise ValueError("Lease lifetime is required")
        # SQLite's 'now' is stable for the whole statement and evaluated in
        # the worker, not before queue/lock delays. Avoid two extra dispatches
        # and never resurrect a lease that expired after a separate clock read.
        cursor = await self.db.execute("""UPDATE admission_lease
            SET expires_at=(julianday('now')-2440587.5)*86400.0+?
            WHERE bucket=? AND owner=?
                AND expires_at>(julianday('now')-2440587.5)*86400.0""", (ttl, bucket, owner))
        renewed = cursor.rowcount == 1
        await self.db.commit()
        return renewed

    async def admission_release(self, bucket, owner):
        validate_lease(bucket, owner)
        cursor = await self.db.execute("DELETE FROM admission_lease WHERE bucket=? AND owner=?", (bucket, owner))
        released = cursor.rowcount == 1
        await self.db.commit()
        return released
