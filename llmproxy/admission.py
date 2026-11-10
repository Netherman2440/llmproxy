"""Adapter-owned expiring leases, independent from process-local counters."""

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
        now = await self._lease_now()
        cursor = await self.db.execute("""UPDATE admission_lease SET expires_at=?
            WHERE bucket=? AND owner=? AND expires_at>?""", (now + ttl, bucket, owner, now))
        renewed = cursor.rowcount == 1
        await self.db.commit()
        return renewed

    async def admission_release(self, bucket, owner):
        validate_lease(bucket, owner)
        cursor = await self.db.execute("DELETE FROM admission_lease WHERE bucket=? AND owner=?", (bucket, owner))
        released = cursor.rowcount == 1
        await self.db.commit()
        return released
