"""Confirmed conversion claim and financial marker survive retry and restart."""
from .accounting import AccountingConflict, decode, encode
from .delivery import DeliveryUnresolved, product


class SqliteDelivery:
    @staticmethod
    def _delivery_intent(row):
        return {'key': row['delivery_key'], 'binding': decode(row['binding']), 'state': row['state']}

    async def delivery_prepare(self, key, binding, fingerprint):
        await self.db.execute('BEGIN IMMEDIATE')
        try:
            await self.db.execute('''INSERT OR IGNORE INTO conversion_delivery
                (delivery_key,binding,fingerprint) VALUES(?,?,?)''', (key, encode(binding), fingerprint))
            row = await (await self.db.execute('SELECT * FROM conversion_delivery WHERE delivery_key=?', (key,))).fetchone()
            if row is None or row['fingerprint'] != fingerprint:
                raise AccountingConflict('Immutable conversion obligation conflict')
            await self.db.commit()
            return self._delivery_intent(row)
        except BaseException:
            await self.db.rollback()
            raise

    async def delivery_outcome(self, key, fingerprint, claimed):
        await self.db.execute('BEGIN IMMEDIATE')
        try:
            row = await (await self.db.execute('SELECT * FROM conversion_delivery WHERE delivery_key=?', (key,))).fetchone()
            if row is None or row['fingerprint'] != fingerprint:
                raise AccountingConflict('Conversion owner or source conflict')
            if claimed and row['state'] in ('prepared', 'unknown'):
                await self.db.execute("UPDATE conversion_delivery SET state='confirmed' WHERE delivery_key=?", (key,))
            elif not claimed and row['state'] == 'prepared':
                await self.db.execute("UPDATE conversion_delivery SET state='unknown' WHERE delivery_key=?", (key,))
            await self.db.commit()
        except BaseException:
            await self.db.rollback()
            raise

    async def delivery_settle(self, key, fingerprint):
        await self.db.execute('BEGIN IMMEDIATE')
        try:
            row = await (await self.db.execute('SELECT * FROM conversion_delivery WHERE delivery_key=?', (key,))).fetchone()
            if row is None or row['fingerprint'] != fingerprint:
                raise AccountingConflict('Conversion owner or source conflict')
            if row['state'] not in ('confirmed', 'billed'):
                raise DeliveryUnresolved('Conversion claim requires reconciliation')
            binding = decode(row['binding'])
            expected = {'created': binding['created_at'], 'api_key': binding['owner']['id'],
                'product': product(binding), 'quantity': binding['pages'],
                'quantity_exact': str(binding['pages']), 'rid': binding['request_id']}
            if row['state'] == 'billed':
                financial = await (await self.db.execute('SELECT * FROM event_oneoff WHERE id=?',
                    (row['financial_id'],))).fetchone()
                if financial is None or any(financial[name] != value for name, value in expected.items()):
                    raise AccountingConflict('Conversion financial record conflict')
                inserted = False
            else:
                cursor = await self.db.execute('''INSERT INTO event_oneoff
                    (created,api_key,product,quantity,quantity_exact,rid) VALUES(?,?,?,?,?,?)''',
                    tuple(expected[name] for name in ('created', 'api_key', 'product', 'quantity', 'quantity_exact', 'rid')))
                await self.db.execute("UPDATE conversion_delivery SET state='billed',financial_id=? WHERE delivery_key=?",
                    (cursor.lastrowid, key))
                inserted = True
            await self.db.commit()
            return inserted
        except BaseException:
            await self.db.rollback()
            raise

    async def delivery_pending(self, limit, after):
        rows = await (await self.db.execute('''SELECT * FROM conversion_delivery
            WHERE state!='billed' AND delivery_key>? ORDER BY delivery_key LIMIT ?''', (after or '', limit))).fetchall()
        return [self._delivery_intent(row) for row in rows]
