"""Real isolated Mongo conversion journal gate; synthetic owner IDs only."""
import asyncio
import json
import multiprocessing
import os
import sys
import uuid
from urllib.parse import quote

STAGE = 'scope'


def uri():
    assert os.environ['AUDIT_NAMESPACE'] == 'izajac'
    assert os.environ['MONGO_HOST'] == 'audit-stt-mongo'
    return ('mongodb://' + quote(os.environ['MONGO_ADMIN_USERNAME'], safe='') + ':'
        + quote(os.environ['MONGO_ADMIN_PASSWORD'], safe='')
        + '@audit-stt-mongo:27017/?authSource=admin&replicaSet=auditstt')


def writer(intent, barrier, answers, crash=False):
    async def run():
        from llmproxy import delivery
        from llmproxy.db import MongoDatabase, shutdown_all
        db = await MongoDatabase.create(uri())
        try:
            if crash:
                await db._prepare_delivery_store()
                original = db._delivery_financial
                class CrashAfterWrite:
                    async def insert_one(self, document):
                        await original.insert_one(document)
                        os._exit(94)
                    def __getattr__(self, name):
                        return getattr(original, name)
                db._delivery_financial = CrashAfterWrite()
            else:
                await asyncio.to_thread(barrier.wait, 5)
            result = await delivery.settle_delivery(db, intent, {'delivery_claimed': True})
            if answers is not None:
                answers.put(result)
        finally:
            await shutdown_all()
    try:
        asyncio.run(run())
    except Exception as error:
        print(json.dumps({'worker': 'FAIL', 'exception_type': type(error).__name__}), flush=True)
        raise SystemExit(1)


async def probe():
    global STAGE
    from bson import ObjectId
    from pymongo.errors import AutoReconnect
    from llmproxy import delivery
    from llmproxy.db import MongoDatabase, shutdown_all
    db = await MongoDatabase.create(uri())
    owner = {'id': ObjectId(), '_user_id': ObjectId(), '_org_id': 'audit-fixture-org',
        '_namespace': 'audit-fixture', '_tier': 'STANDARD'}
    def binding():
        source = uuid.uuid4().hex
        return {'owner': owner, 'backend_alias': 'audit-conversion', 'backend_url': 'http://audit-fixture',
            'device': 'none', 'job_id': source, 'request_id': source,
            'created_at': '2026-06-01T00:00:00.123456+00:00'}
    finance = db.db.cgc.billing_record
    try:
        STAGE = 'adapter_api'
        assert callable(getattr(db, 'delivery_prepare', None)), 'Missing Mongo delivery adapter'
        STAGE = 'first_write_and_duplicate'
        first = await delivery.prepare_delivery(db, binding(), 5)
        assert await delivery.settle_delivery(db, first, {'delivery_claimed': True})
        assert not await delivery.settle_delivery(db, first, {'delivery_claimed': False})
        row = await finance.find_one({'api_key_id': owner['id']})
        assert isinstance(row['_id'], ObjectId)
        assert row['resources'] == {'audit-conversion/none/conversion': 5}
        STAGE = 'unknown_is_not_free_or_speculatively_billed'
        unknown = await delivery.prepare_delivery(db, binding(), 7)
        try:
            await delivery.settle_delivery(db, unknown, {'delivery_claimed': False})
        except delivery.DeliveryUnresolved:
            pass
        else:
            raise AssertionError('Unknown claim was treated as success')
        STAGE = 'actual_insert_with_lost_reply'
        second = await delivery.prepare_delivery(db, binding(), 6)
        original = db._delivery_financial
        class LoseReply:
            async def insert_one(self, document):
                await original.insert_one(document)
                raise AutoReconnect('injected lost insert reply')
            def __getattr__(self, name):
                return getattr(original, name)
        db._delivery_financial = LoseReply()
        try:
            try:
                await delivery.settle_delivery(db, second, {'delivery_claimed': True})
            except delivery.AccountingUnavailable:
                pass
            else:
                raise AssertionError('Ambiguous insert not surfaced')
        finally:
            db._delivery_financial = original
        assert not await delivery.settle_delivery(db, second)
        STAGE = 'two_processes_one_bill'
        third = await delivery.prepare_delivery(db, binding(), 8)
        ctx = multiprocessing.get_context('spawn')
        barrier, answers = ctx.Barrier(2), ctx.Queue()
        processes = [ctx.Process(target=writer, args=(third, barrier, answers)) for _ in range(2)]
        try:
            for process in processes:
                process.start()
            results = [await asyncio.to_thread(answers.get, True, 12) for _ in processes]
            assert sorted(results) == [False, True]
        finally:
            for process in processes:
                await asyncio.to_thread(process.join, 5)
                if process.is_alive():
                    process.terminate()
                    await asyncio.to_thread(process.join, 5)
            answers.close()
        assert all(process.exitcode == 0 for process in processes)
        STAGE = 'process_crash_after_financial_insert'
        fourth = await delivery.prepare_delivery(db, binding(), 9)
        process = ctx.Process(target=writer, args=(fourth, None, None, True))
        process.start()
        await asyncio.to_thread(process.join, 12)
        if process.is_alive():
            process.terminate()
            await asyncio.to_thread(process.join, 5)
        assert process.exitcode == 94
        assert not await delivery.settle_delivery(db, fourth)
        assert await finance.count_documents({'api_key_id': owner['id']}) == 4
        STAGE = 'missing_previously_billed_record_is_not_recreated'
        mapped = await db._delivery_ledger.find_one({'_id': first['key']})
        await finance.delete_one({'_id': mapped['financial_id']})
        try:
            await delivery.settle_delivery(db, first, {'delivery_claimed': False})
        except delivery.AccountingConflict:
            pass
        else:
            raise AssertionError('Missing historical financial record was silently recreated')
        assert await finance.count_documents({'api_key_id': owner['id']}) == 3
        print(json.dumps({'status': 'PASS', 'namespace': 'izajac', 'objectid': True,
            'one_bill_per_source': True, 'unknown_not_delivered': True,
            'lost_reply_recovered': True, 'two_processes_one_bill': True,
            'post_insert_crash_recovered': True, 'missing_historical_bill_not_recreated': True,
            'customer_charges': False}), flush=True)
    finally:
        await finance.delete_many({'api_key_id': owner['id']})
        await db.db.cgc.llmproxy_conversion_delivery.delete_many({'binding.owner.id': owner['id']})
        await shutdown_all()


if __name__ == '__main__':
    try:
        asyncio.run(probe())
    except Exception as error:
        print(json.dumps({'status': 'FAIL', 'stage': STAGE, 'exception_type': type(error).__name__}), flush=True)
        sys.exit(1)
