"""Standalone-compatible receipt -> stable ObjectId -> financial journal.

A persistent session event fence serializes different receipt ranges. It never
expires while work is unresolved; any worker may resume the SAME event after
a crash. Unique financial IDs make concurrent retries of that event safe.
"""
import asyncio
import copy
import datetime

from .accounting import AccountingConflict, AccountingUnavailable, binding_hash, product


class MongoAccounting:
    async def accounting_prepare(self):
        if getattr(self, '_accounting_ready', False):
            return
        from pymongo import ReadPreference
        from pymongo.read_concern import ReadConcern
        from pymongo.write_concern import WriteConcern
        options = dict(write_concern=WriteConcern(w='majority', j=True),
            read_concern=ReadConcern('majority'), read_preference=ReadPreference.PRIMARY)
        database = self.db['cgc']
        self._accounting_sessions = database.get_collection('llmproxy_accounting_session', **options)
        self._accounting_receipts = database.get_collection('llmproxy_accounting_receipt', **options)
        self._accounting_financial = database.get_collection('billing_record',
            codec_options=self.copt.with_options(tz_aware=True), **options)
        await self._accounting_receipts.create_index(
            [('backend_instance_id', 1), ('cursor', 1)], unique=True, name='receipt_terminal_cursor')
        await self._accounting_receipts.create_index(
            [('session_id', 1), ('segment_id', 1)], unique=True, name='receipt_session_segment')
        await self._accounting_receipts.create_index(
            [('session_id', 1), ('acked_at', 1), ('cursor', 1)], name='receipt_ack_outbox')
        await self._accounting_receipts.create_index(
            [('session_id', 1), ('start_sample', 1)], name='receipt_source_ranges')
        await self._accounting_sessions.create_index([('status', 1), ('_id', 1)], name='accounting_pending')
        self._accounting_ready = True

    async def accounting_bind(self, binding):
        from pymongo.errors import DuplicateKeyError
        await self.accounting_prepare()
        if not {'_namespace', '_user_id', '_org_id', '_tier'} <= binding['owner'].keys():
            raise ValueError('Mongo accounting requires complete historical owner metadata')
        fingerprint = binding_hash(binding)
        try:
            await self._accounting_sessions.update_one({'_id': binding['session_id']}, {'$setOnInsert': {
                'binding': copy.deepcopy(binding), 'binding_hash': fingerprint,
                'backend_instance_id': binding['backend_instance_id'], 'reconcile_cursor': 0,
                'status': 'bound' if binding['backend_instance_id'] is None else 'pending',
                'last_error': None, 'active_event': None}}, upsert=True)
        except DuplicateKeyError:
            pass
        existing = await self._accounting_sessions.find_one({'_id': binding['session_id']},
            projection={'binding_hash': 1, 'backend_instance_id': 1})
        if (existing is None or existing['binding_hash'] != fingerprint
                or binding['backend_instance_id'] not in (None, existing['backend_instance_id'])):
            raise AccountingConflict('Immutable session binding conflict')

    @staticmethod
    def _mongo_binding(row):
        if row is None:
            return None
        result = copy.deepcopy(row['binding'])
        result['backend_instance_id'] = row['backend_instance_id']
        return result

    async def accounting_get(self, session_id):
        await self.accounting_prepare()
        return self._mongo_binding(await self._accounting_sessions.find_one({'_id': session_id}))

    async def accounting_attach(self, session_id, instance_id):
        await self.accounting_prepare()
        result = await self._accounting_sessions.update_one({'_id': session_id,
            'backend_instance_id': {'$in': [None, instance_id]}}, {'$set': {'backend_instance_id': instance_id}})
        if result.matched_count != 1:
            raise AccountingConflict('Backend identity conflict')

    async def _release_event(self, session_id, event_id):
        await self._accounting_sessions.update_one({'_id': session_id, 'active_event': event_id},
            {'$set': {'active_event': None}})

    async def _acquire_event(self, binding, value):
        for _ in range(8):
            result = await self._accounting_sessions.update_one({
                '_id': value['session_id'], 'binding_hash': binding_hash(binding),
                'backend_instance_id': value['backend_instance_id'],
                'active_event': {'$in': [None, value['event_id']]}},
                {'$set': {'active_event': value['event_id']}})
            if result.matched_count == 1:
                return
            session = await self._accounting_sessions.find_one({'_id': value['session_id']},
                projection={'active_event': 1})
            active = session.get('active_event') if session is not None else None
            if active is not None:
                previous = await self._accounting_receipts.find_one({'_id': active}, projection={'status': 1})
                if previous is not None and previous['status'] in ('billed', 'terminal'):
                    await self._release_event(value['session_id'], active)
                    continue
            await asyncio.sleep(0.01)
        # The pending receipt is durable already. Reconciliation resumes the
        # active event first; never steal its fence after a guessed timeout.
        raise AccountingUnavailable('Earlier receipt requires reconciliation')

    async def accounting_record(self, binding, parsed):
        from bson import ObjectId
        from pymongo.errors import DuplicateKeyError
        from .db import _billing_document
        await self.accounting_prepare()
        value = parsed['receipt']
        fingerprint = binding_hash(binding)
        candidate_id = ObjectId()
        try:
            await self._accounting_receipts.update_one({'_id': value['event_id']}, {'$setOnInsert': {
                'session_id': value['session_id'], 'backend_instance_id': value['backend_instance_id'],
                'cursor': value['cursor'], 'segment_id': value['segment_id'],
                'start_sample': value['start_sample'], 'end_sample': value['start_sample'] + value['sample_count'],
                'binding_hash': fingerprint, 'fingerprint': parsed['fingerprint'], 'billing_id': candidate_id,
                'receipt': copy.deepcopy(value), 'quantity_exact': str(parsed['quantity']) if parsed['quantity'] is not None else None,
                'status': 'pending', 'acked_at': None}}, upsert=True)
        except DuplicateKeyError:
            pass
        ledger = await self._accounting_receipts.find_one({'_id': value['event_id']})
        if (ledger is None or ledger['fingerprint'] != parsed['fingerprint'] or ledger['binding_hash'] != fingerprint):
            raise AccountingConflict('Receipt journal identity conflict')
        if ledger['status'] == 'pending':
            await self._accounting_sessions.update_one({'_id': value['session_id'],
                'status': {'$in': ['bound', 'settled']}}, {'$set': {'status': 'pending'}})
            await self._acquire_event(binding, value)
            overlap = await self._accounting_receipts.find_one({'session_id': value['session_id'],
                '_id': {'$ne': value['event_id']}, 'status': {'$in': ['billed', 'terminal']},
                'start_sample': {'$lt': value['start_sample'] + value['sample_count']},
                'end_sample': {'$gt': value['start_sample']}}, projection={'_id': 1})
            if overlap is not None:
                raise AccountingConflict('Overlapping source samples')
        elif ledger['status'] not in ('billed', 'terminal'):
            raise AccountingConflict('Invalid persisted receipt state')
        inserted = False
        if parsed['quantity'] is not None:
            completed = parsed['completed_time']
            completed = completed.replace(microsecond=(completed.microsecond // 1000) * 1000)
            document = _billing_document(binding['owner'], completed,
                {product(binding): parsed['quantity']}, binding['request_id'])
            document['_id'] = ledger['billing_id']
            try:
                await self._accounting_financial.insert_one(document)
                inserted = True
            except DuplicateKeyError:
                pass
            existing = await self._accounting_financial.find_one({'_id': ledger['billing_id']})
            if existing is None or any(existing.get(name) != expected
                    for name, expected in document.items() if name != 'revision_synced'):
                raise AccountingConflict('Financial record and receipt journal disagree')
        await self._accounting_receipts.update_one({'_id': value['event_id'], 'fingerprint': parsed['fingerprint']},
            {'$set': {'status': 'billed' if parsed['quantity'] is not None else 'terminal'}})
        await self._release_event(value['session_id'], value['event_id'])
        return inserted

    async def accounting_pending(self, limit, after):
        await self.accounting_prepare()
        rows = await self._accounting_sessions.find({'status': {'$ne': 'settled'},
            '_id': {'$gt': after or ''}}).sort('_id', 1).limit(limit).to_list(length=limit)
        return [self._mongo_binding(row) | {'reconcile_cursor': row['reconcile_cursor'],
            'status': row['status'], 'last_error': row.get('last_error')} for row in rows]

    async def accounting_unacked(self, session_id, limit):
        await self.accounting_prepare()
        rows = await self._accounting_receipts.find({'session_id': session_id, 'acked_at': None,
            'status': {'$in': ['billed', 'terminal']}}, projection={'receipt': 1}).sort('cursor', 1).limit(limit).to_list(length=limit)
        return [row['receipt'] for row in rows]

    async def accounting_healthy(self):
        await self.accounting_prepare()
        return await self._accounting_sessions.find_one({'status': {'$ne': 'settled'},
            'last_error': {'$ne': None}}, projection={'_id': 1}) is None

    async def accounting_incomplete(self, session_id, limit):
        await self.accounting_prepare()
        session = await self._accounting_sessions.find_one({'_id': session_id}, projection={'active_event': 1})
        active = session.get('active_event') if session is not None else None
        rows = await self._accounting_receipts.find({'session_id': session_id, 'status': 'pending'},
            projection={'receipt': 1}).sort('cursor', 1).limit(limit).to_list(length=limit)
        if active is not None:
            first = await self._accounting_receipts.find_one({'_id': active}, projection={'receipt': 1})
            if first is not None:
                rows = [first] + [row for row in rows if row['_id'] != active]
        return [row['receipt'] for row in rows[:limit]]

    async def accounting_status(self, session_id, status, reason, expected_status=None):
        await self.accounting_prepare()
        query = {'_id': session_id}
        if expected_status is not None:
            query['status'] = expected_status
        if status == 'settled':
            if await self._accounting_receipts.find_one({'session_id': session_id,
                    '$or': [{'acked_at': None}, {'status': 'pending'}]}, projection={'_id': 1}):
                return
            query['active_event'] = None
        await self._accounting_sessions.update_one(query, {'$set': {'status': status, 'last_error': reason}})

    async def accounting_ack(self, event_id):
        await self.accounting_prepare()
        result = await self._accounting_receipts.update_one({'_id': event_id,
            'status': {'$in': ['billed', 'terminal']}}, {'$set': {'acked_at': datetime.datetime.now(datetime.UTC)}})
        if result.matched_count != 1:
            raise AccountingConflict('Cannot acknowledge an uncommitted receipt')

    async def accounting_advance(self, session_id, cursor):
        await self.accounting_prepare()
        await self._accounting_sessions.update_one({'_id': session_id}, {'$max': {'reconcile_cursor': cursor}})
