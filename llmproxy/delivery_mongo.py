"""Stable delivery-key to ObjectId mapping; no multi-document transaction."""
import copy
import datetime

from .accounting import AccountingConflict
from .delivery import DeliveryUnresolved, product


class MongoDelivery:
    async def _prepare_delivery_store(self):
        if getattr(self, '_delivery_ready', False):
            return
        from pymongo import ReadPreference
        from pymongo.read_concern import ReadConcern
        from pymongo.write_concern import WriteConcern
        options = dict(write_concern=WriteConcern(w='majority', j=True),
            read_concern=ReadConcern('majority'), read_preference=ReadPreference.PRIMARY)
        self._delivery_ledger = self.db['cgc'].get_collection('llmproxy_conversion_delivery', **options)
        self._delivery_financial = self.db['cgc'].get_collection('billing_record',
            codec_options=self.copt.with_options(tz_aware=True), **options)
        await self._delivery_ledger.create_index('financial_id', unique=True, name='delivery_financial_identity')
        await self._delivery_ledger.create_index([('state', 1), ('_id', 1)], name='pending_delivery')
        self._delivery_ready = True

    @staticmethod
    def _delivery_intent(row):
        return {'key': row['_id'], 'binding': copy.deepcopy(row['binding']), 'state': row['state']}

    async def delivery_prepare(self, key, binding, fingerprint):
        from bson import ObjectId
        from pymongo.errors import DuplicateKeyError
        await self._prepare_delivery_store()
        if not {'_namespace', '_user_id', '_org_id', '_tier'} <= binding['owner'].keys():
            raise ValueError('Mongo delivery requires complete historical owner metadata')
        try:
            await self._delivery_ledger.update_one({'_id': key}, {'$setOnInsert': {
                'binding': copy.deepcopy(binding), 'fingerprint': fingerprint,
                'financial_id': ObjectId(), 'state': 'prepared'}}, upsert=True)
        except DuplicateKeyError:
            pass
        row = await self._delivery_ledger.find_one({'_id': key})
        if row is None or row['fingerprint'] != fingerprint:
            raise AccountingConflict('Immutable conversion obligation conflict')
        return self._delivery_intent(row)

    async def delivery_outcome(self, key, fingerprint, claimed):
        await self._prepare_delivery_store()
        row = await self._delivery_ledger.find_one({'_id': key})
        if row is None or row['fingerprint'] != fingerprint:
            raise AccountingConflict('Conversion owner or source conflict')
        if claimed:
            await self._delivery_ledger.update_one({'_id': key, 'fingerprint': fingerprint,
                'state': {'$in': ['prepared', 'unknown']}}, {'$set': {'state': 'confirmed'}})
        else:
            await self._delivery_ledger.update_one({'_id': key, 'fingerprint': fingerprint,
                'state': 'prepared'}, {'$set': {'state': 'unknown'}})

    async def delivery_settle(self, key, fingerprint):
        from pymongo.errors import DuplicateKeyError
        from .db import _billing_document
        await self._prepare_delivery_store()
        row = await self._delivery_ledger.find_one({'_id': key})
        if row is None or row['fingerprint'] != fingerprint:
            raise AccountingConflict('Conversion owner or source conflict')
        if row['state'] not in ('confirmed', 'billed'):
            raise DeliveryUnresolved('Conversion claim requires reconciliation')
        binding = row['binding']
        created = datetime.datetime.fromisoformat(binding['created_at'])
        created = created.replace(microsecond=(created.microsecond // 1000) * 1000)
        document = _billing_document(binding['owner'], created,
            {product(binding): binding['pages']}, binding['request_id'])
        document['_id'] = row['financial_id']
        inserted = False
        if row['state'] != 'billed':
            try:
                await self._delivery_financial.insert_one(document)
                inserted = True
            except DuplicateKeyError:
                pass
        existing = await self._delivery_financial.find_one({'_id': row['financial_id']})
        if existing is None or any(existing.get(name) != value
                for name, value in document.items() if name != 'revision_synced'):
            raise AccountingConflict('Conversion financial record conflict')
        await self._delivery_ledger.update_one({'_id': key, 'fingerprint': fingerprint}, {'$set': {'state': 'billed'}})
        return inserted

    async def delivery_pending(self, limit, after):
        await self._prepare_delivery_store()
        rows = await self._delivery_ledger.find({'state': {'$ne': 'billed'},
            '_id': {'$gt': after or ''}}).sort('_id', 1).limit(limit).to_list(length=limit)
        return [self._delivery_intent(row) for row in rows]
