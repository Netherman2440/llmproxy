"""SQLite financial row and receipt marker share one durable transaction."""
import datetime

from .accounting import AccountingConflict, binding_hash, decode, encode, product


class SqliteAccounting:
    async def accounting_bind(self, binding):
        fingerprint = binding_hash(binding)
        await self.db.execute('BEGIN IMMEDIATE')
        try:
            existing = await (await self.db.execute(
                'SELECT * FROM accounting_session WHERE session_id=?', (binding['session_id'],))).fetchone()
            if existing is not None:
                if (existing['binding_hash'] != fingerprint or binding['backend_instance_id'] not in
                        (None, existing['backend_instance_id'])):
                    raise AccountingConflict('Immutable session binding conflict')
            else:
                await self.db.execute('''INSERT INTO accounting_session
                    (session_id,binding,binding_hash,backend_instance_id) VALUES(?,?,?,?)''',
                    (binding['session_id'], encode(binding), fingerprint, binding['backend_instance_id']))
            await self.db.commit()
        except BaseException:
            await self.db.rollback()
            raise

    @staticmethod
    def _binding(row):
        if row is None:
            return None
        result = decode(row['binding'])
        result['backend_instance_id'] = row['backend_instance_id']
        return result

    async def accounting_get(self, session_id):
        row = await (await self.db.execute(
            'SELECT * FROM accounting_session WHERE session_id=?', (session_id,))).fetchone()
        return self._binding(row)

    async def accounting_attach(self, session_id, instance_id):
        cursor = await self.db.execute('''UPDATE accounting_session SET backend_instance_id=?
            WHERE session_id=? AND (backend_instance_id IS NULL OR backend_instance_id=?)''',
            (instance_id, session_id, instance_id))
        if cursor.rowcount != 1:
            await self.db.rollback()
            raise AccountingConflict('Backend identity conflict')
        await self.db.commit()

    async def accounting_record(self, binding, parsed):
        value = parsed['receipt']
        await self.db.execute('BEGIN IMMEDIATE')
        try:
            session = await (await self.db.execute('SELECT * FROM accounting_session WHERE session_id=?',
                (value['session_id'],))).fetchone()
            if (session is None or session['binding_hash'] != binding_hash(binding)
                    or session['backend_instance_id'] != value['backend_instance_id']):
                raise AccountingConflict('Receipt does not match durable binding')
            previous = await (await self.db.execute('SELECT * FROM accounting_receipt WHERE event_id=?',
                (value['event_id'],))).fetchone()
            if previous is not None:
                if previous['fingerprint'] != parsed['fingerprint']:
                    raise AccountingConflict('Receipt payload conflict')
                if previous['status'] == 'billed':
                    financial = await (await self.db.execute('SELECT * FROM event_oneoff WHERE id=?',
                        (previous['billing_event_id'],))).fetchone()
                    expected = {'api_key': binding['owner']['id'], 'product': product(binding),
                        'quantity_exact': str(parsed['quantity']), 'quantity': float(parsed['quantity']),
                        'rid': binding['request_id'], 'created': parsed['completed_time'].isoformat()}
                    if (financial is None or any(financial[name] != expected[name] for name in expected)
                            or previous['quantity_exact'] != expected['quantity_exact']):
                        raise AccountingConflict('Financial record and receipt ledger disagree')
                elif previous['status'] != 'terminal':
                    raise AccountingConflict('Invalid persisted receipt state')
                await self.db.commit()
                return False
            conflict = await (await self.db.execute('''SELECT event_id FROM accounting_receipt WHERE
                (backend_instance_id=? AND cursor=?) OR (session_id=? AND segment_id=?) OR
                (session_id=? AND start_sample<? AND start_sample+sample_count>?) LIMIT 1''',
                (value['backend_instance_id'], value['cursor'], value['session_id'], value['segment_id'],
                 value['session_id'], value['start_sample'] + value['sample_count'], value['start_sample']))).fetchone()
            if conflict is not None:
                raise AccountingConflict('Receipt cursor, segment or sample range conflict')
            quantity = str(parsed['quantity']) if parsed['quantity'] is not None else None
            await self.db.execute('''INSERT INTO accounting_receipt
                (event_id,session_id,backend_instance_id,cursor,segment_id,start_sample,sample_count,
                 fingerprint,receipt,status,quantity_exact) VALUES(?,?,?,?,?,?,?,?,?,'pending',?)''',
                (value['event_id'], value['session_id'], value['backend_instance_id'], value['cursor'],
                 value['segment_id'], value['start_sample'], value['sample_count'], parsed['fingerprint'],
                 encode(value), quantity))
            financial_id = None
            if quantity is not None:
                cursor = await self.db.execute('''INSERT INTO event_oneoff
                    (created,api_key,product,quantity,rid,quantity_exact) VALUES(?,?,?,?,?,?)''',
                    (parsed['completed_time'], binding['owner']['id'], product(binding), float(parsed['quantity']),
                     binding['request_id'], quantity))
                financial_id = cursor.lastrowid
            await self.db.execute('UPDATE accounting_receipt SET status=?,billing_event_id=? WHERE event_id=?',
                ('billed' if financial_id is not None else 'terminal', financial_id, value['event_id']))
            await self.db.commit()
            return financial_id is not None
        except BaseException:
            await self.db.rollback()
            raise

    async def accounting_pending(self, limit, after):
        rows = await (await self.db.execute('''SELECT * FROM accounting_session
            WHERE status!='settled' AND session_id>? ORDER BY session_id LIMIT ?''', (after or '', limit))).fetchall()
        return [self._binding(row) | {'reconcile_cursor': row['reconcile_cursor'],
            'status': row['status'], 'last_error': row['last_error']} for row in rows]

    async def accounting_unacked(self, session_id, limit):
        rows = await (await self.db.execute('''SELECT receipt FROM accounting_receipt
            WHERE session_id=? AND acked_at IS NULL AND status IN ('billed','terminal')
            ORDER BY cursor LIMIT ?''', (session_id, limit))).fetchall()
        return [decode(row['receipt']) for row in rows]

    async def accounting_ack(self, event_id):
        cursor = await self.db.execute('''UPDATE accounting_receipt SET acked_at=COALESCE(acked_at,?)
            WHERE event_id=? AND status IN ('billed','terminal')''',
            (datetime.datetime.now(datetime.UTC).isoformat(), event_id))
        if cursor.rowcount != 1:
            await self.db.rollback()
            raise AccountingConflict('Cannot acknowledge an uncommitted receipt')
        await self.db.commit()

    async def accounting_advance(self, session_id, cursor):
        await self.db.execute('''UPDATE accounting_session SET reconcile_cursor=MAX(reconcile_cursor,?)
            WHERE session_id=?''', (cursor, session_id))
        await self.db.commit()
