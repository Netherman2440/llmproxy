"""Atomic token finance set and stable HTTP attempt marker under SQLite FULL."""
from .accounting import AccountingConflict, decode, encode


class SqliteAttempts:
    @staticmethod
    def _attempt_intent(row):
        return {'key': row['attempt_id'], 'binding': decode(row['binding']), 'state': row['state'],
            'resources': decode(row['resources']) if row['resources'] is not None else None,
            'observed_at': row['observed_at']}

    async def _attempt_row(self, key, fingerprint):
        rows = await self.db.execute_fetchall('SELECT * FROM http_attempt WHERE attempt_id=?', (key,))
        if not rows or rows[0]['fingerprint'] != fingerprint:
            raise AccountingConflict('HTTP attempt owner or source conflict')
        return rows[0]

    async def attempt_prepare(self, key, binding, fingerprint):
        await self.db.execute('BEGIN IMMEDIATE')
        try:
            await self.db.execute('''INSERT OR IGNORE INTO http_attempt
                (attempt_id,binding,fingerprint) VALUES(?,?,?)''', (key, encode(binding), fingerprint))
            row = await self._attempt_row(key, fingerprint)
            await self.db.commit()
            return self._attempt_intent(row)
        except BaseException:
            await self.db.rollback()
            raise

    async def attempt_confirm(self, key, fingerprint, resources, observed_at):
        await self.db.execute('BEGIN IMMEDIATE')
        try:
            row = await self._attempt_row(key, fingerprint)
            encoded = encode(resources)
            if row['state'] == 'rejected' or row['resources'] not in (None, encoded):
                raise AccountingConflict('Immutable HTTP usage proof conflict')
            if row['state'] in ('prepared', 'unknown'):
                await self.db.execute("UPDATE http_attempt SET state='confirmed',resources=?,observed_at=? WHERE attempt_id=?",
                    (encoded, observed_at, key))
            await self.db.commit()
        except BaseException:
            await self.db.rollback()
            raise

    async def attempt_abandon(self, key, fingerprint, rejected):
        await self.db.execute('BEGIN IMMEDIATE')
        try:
            await self._attempt_row(key, fingerprint)
            await self.db.execute("UPDATE http_attempt SET state=? WHERE attempt_id=? AND state='prepared'",
                ('rejected' if rejected else 'unknown', key))
            await self.db.commit()
        except BaseException:
            await self.db.rollback()
            raise

    async def attempt_settle(self, key, fingerprint):
        await self.db.execute('BEGIN IMMEDIATE')
        try:
            row = await self._attempt_row(key, fingerprint)
            if row['state'] not in ('confirmed', 'billed') or row['resources'] is None:
                raise AccountingConflict('HTTP attempt requires authoritative usage proof')
            binding, resources = decode(row['binding']), decode(row['resources'])
            inserted = row['state'] != 'billed'
            prefix = binding['backend_alias'] + '/' + binding['device']
            # Preserve legacy prompt-before-completion financial ordering.
            for product in (prefix + '/prompt', prefix + '/completion'):
                quantity = resources[product]
                expected = (row['observed_at'], binding['owner']['id'], product,
                    quantity, str(quantity), binding['request_id'])
                if inserted:
                    cursor = await self.db.execute('''INSERT INTO event_oneoff
                        (created,api_key,product,quantity,quantity_exact,rid) VALUES(?,?,?,?,?,?)''', expected)
                    await self.db.execute('INSERT INTO http_attempt_financial VALUES(?,?,?)',
                        (key, product, cursor.lastrowid))
                else:
                    stored = await self.db.execute_fetchall('''SELECT e.* FROM http_attempt_financial m
                        JOIN event_oneoff e ON e.id=m.financial_id WHERE m.attempt_id=? AND m.product=?''', (key, product))
                    if not stored or tuple(stored[0][name] for name in
                            ('created', 'api_key', 'product', 'quantity', 'quantity_exact', 'rid')) != expected:
                        raise AccountingConflict('HTTP financial record conflict')
            await self.db.execute("UPDATE http_attempt SET state='billed' WHERE attempt_id=?", (key,))
            await self.db.commit()
            return inserted
        except BaseException:
            await self.db.rollback()
            raise

    async def attempt_pending(self, limit, after):
        rows = await self.db.execute_fetchall('''SELECT * FROM http_attempt
            WHERE state NOT IN ('billed','rejected') AND attempt_id>? ORDER BY attempt_id LIMIT ?''', (after or '', limit))
        return [self._attempt_intent(row) for row in rows]
