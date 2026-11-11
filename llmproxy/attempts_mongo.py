"""Stable BSON identity for confirmed HTTP usage, majority+journal replay."""
import copy
import datetime

from .accounting import AccountingConflict


class MongoAttempts:
    async def _prepare_attempt_store(self):
        if getattr(self, '_attempt_ready', False):
            return
        from pymongo import ReadPreference
        from pymongo.read_concern import ReadConcern
        from pymongo.write_concern import WriteConcern
        options = dict(write_concern=WriteConcern(w='majority', j=True),
            read_concern=ReadConcern('majority'), read_preference=ReadPreference.PRIMARY)
        self._attempt_ledger = self.db['cgc'].get_collection('llmproxy_http_attempt', **options)
        self._attempt_financial = self.db['cgc'].get_collection('billing_record',
            codec_options=self.copt.with_options(tz_aware=True), **options)
        await self._attempt_ledger.create_index('financial_id', unique=True, name='http_financial_identity')
        await self._attempt_ledger.create_index([('state', 1), ('_id', 1)], name='http_pending')
        self._attempt_ready = True

    @staticmethod
    def _attempt_intent(row):
        return {'key': row['_id'], 'binding': copy.deepcopy(row['binding']), 'state': row['state'],
            'resources': copy.deepcopy(row.get('resources')), 'observed_at': row.get('observed_at')}

    async def _attempt_row(self, key, fingerprint):
        await self._prepare_attempt_store()
        row = await self._attempt_ledger.find_one({'_id': key})
        if row is None or row['fingerprint'] != fingerprint:
            raise AccountingConflict('HTTP attempt owner or source conflict')
        return row

    async def attempt_prepare(self, key, binding, fingerprint):
        from bson import ObjectId
        from pymongo.errors import DuplicateKeyError
        await self._prepare_attempt_store()
        if not {'_namespace', '_user_id', '_org_id', '_tier'} <= binding['owner'].keys():
            raise ValueError('Mongo HTTP attempt requires complete historical owner metadata')
        try:
            await self._attempt_ledger.update_one({'_id': key}, {'$setOnInsert': {
                'binding': copy.deepcopy(binding), 'fingerprint': fingerprint,
                'financial_id': ObjectId(), 'state': 'prepared', 'resources': None}}, upsert=True)
        except DuplicateKeyError:
            pass
        return self._attempt_intent(await self._attempt_row(key, fingerprint))

    async def attempt_confirm(self, key, fingerprint, resources, observed_at):
        row = await self._attempt_row(key, fingerprint)
        if row['state'] == 'rejected' or row['resources'] not in (None, resources):
            raise AccountingConflict('Immutable HTTP usage proof conflict')
        await self._attempt_ledger.update_one({'_id': key, 'fingerprint': fingerprint,
            'state': {'$in': ['prepared', 'unknown']}, 'resources': None},
            {'$set': {'state': 'confirmed', 'resources': copy.deepcopy(resources), 'observed_at': observed_at}})
        # A competing confirmer must not silently win with another proof.
        row = await self._attempt_row(key, fingerprint)
        if row['resources'] != resources or row['state'] not in ('confirmed', 'billed'):
            raise AccountingConflict('Immutable HTTP usage proof conflict')

    async def attempt_abandon(self, key, fingerprint, rejected):
        await self._attempt_row(key, fingerprint)
        await self._attempt_ledger.update_one({'_id': key, 'fingerprint': fingerprint, 'state': 'prepared'},
            {'$set': {'state': 'rejected' if rejected else 'unknown'}})

    async def attempt_settle(self, key, fingerprint):
        from pymongo.errors import DuplicateKeyError
        from .db import _billing_document
        row = await self._attempt_row(key, fingerprint)
        if row['state'] not in ('confirmed', 'billed') or row['resources'] is None:
            raise AccountingConflict('HTTP attempt requires authoritative usage proof')
        binding = row['binding']
        created = datetime.datetime.fromisoformat(row['observed_at'])
        created = created.replace(microsecond=(created.microsecond // 1000) * 1000)
        document = _billing_document(binding['owner'], created, row['resources'], binding['request_id'])
        document['_id'] = row['financial_id']
        inserted = False
        if row['state'] != 'billed':
            try:
                await self._attempt_financial.insert_one(document)
                inserted = True
            except DuplicateKeyError:
                pass
        existing = await self._attempt_financial.find_one({'_id': row['financial_id']})
        if existing is None or any(existing.get(name) != value
                for name, value in document.items() if name != 'revision_synced'):
            raise AccountingConflict('HTTP financial record conflict')
        await self._attempt_ledger.update_one({'_id': key, 'fingerprint': fingerprint}, {'$set': {'state': 'billed'}})
        return inserted

    async def attempt_pending(self, limit, after):
        await self._prepare_attempt_store()
        rows = await self._attempt_ledger.find({'state': {'$nin': ['billed', 'rejected']},
            '_id': {'$gt': after or ''}}).sort('_id', 1).limit(limit).to_list(length=limit)
        return [self._attempt_intent(row) for row in rows]
