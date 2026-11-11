"""Real, isolated Mongo receipt accounting fault gate; never a skipped host test."""
import asyncio
import copy
import datetime
from decimal import Decimal
import json
import multiprocessing
import os
import sys
import uuid
from urllib.parse import quote


STAGE = 'scope'


def uri():
    if (os.environ.get('MONGO_HOST') != 'audit-stt-mongo'
            or os.environ.get('AUDIT_NAMESPACE') not in ('izajac', 'ci-audit')):
        raise ValueError('Private audit Mongo required')
    return ('mongodb://' + quote(os.environ['MONGO_ADMIN_USERNAME'], safe='') + ':'
        + quote(os.environ['MONGO_ADMIN_PASSWORD'], safe='')
        + '@audit-stt-mongo:27017/?authSource=admin&replicaSet=auditstt')


def scenario(owner):
    from tests.accounting_fixtures import binding, receipt
    sid, instance = uuid.uuid4().hex, uuid.uuid4().hex
    session = binding() | {'session_id': sid, 'backend_instance_id': instance,
        'owner': copy.deepcopy(owner), 'backend_alias': 'audit-accounting',
        'request_id': 'audit-accounting-' + sid}
    event = receipt() | {'session_id': sid, 'backend_instance_id': instance,
        'event_id': instance + ':' + sid + ':seg_1', 'sample_count': 16001,
        'usage': {'type': 'duration', 'seconds': 1.0000625}}
    return session, event


def writer(session, event, barrier, results, crash=False):
    async def run():
        from llmproxy.accounting import record_receipt
        from llmproxy.db import MongoDatabase, shutdown_all
        adapter = await MongoDatabase.create(uri())
        try:
            if crash:
                await adapter.accounting_prepare()
                original = adapter._accounting_financial

                class CrashAfterInsert:
                    async def insert_one(self, document):
                        await original.insert_one(document)
                        os._exit(93)

                    def __getattr__(self, name):
                        return getattr(original, name)
                adapter._accounting_financial = CrashAfterInsert()
            else:
                await asyncio.to_thread(barrier.wait, 5)
            result = await record_receipt(adapter, session, event)
            if results is not None:
                results.put(result)
        finally:
            await shutdown_all()
    try:
        asyncio.run(run())
    except Exception as error:
        print(json.dumps({'worker_status': 'FAIL', 'exception_type': type(error).__name__}), flush=True)
        raise SystemExit(1)


async def probe():
    global STAGE
    import bson
    from pymongo.errors import AutoReconnect
    from llmproxy import accounting
    from llmproxy.db import MongoDatabase, shutdown_all
    adapter = await MongoDatabase.create(uri())
    sessions, events = [], []
    owner = {'id': bson.ObjectId(), '_user_id': bson.ObjectId(), '_namespace': 'audit-fixture',
        '_org_id': 'audit-fixture-org', '_tier': 'STANDARD'}
    financial = adapter.db.cgc.billing_record
    try:
        STAGE = 'adapter_api'
        assert callable(getattr(adapter, 'accounting_bind', None)), 'Missing durable Mongo accounting API'
        STAGE = 'first_bill_and_exact_decimal'
        session, event = scenario(owner)
        sessions.append(session['session_id'])
        events.append(event['event_id'])
        await accounting.bind_session(adapter, session)
        assert await accounting.record_receipt(adapter, session, event)
        assert not await accounting.record_receipt(adapter, session, event)
        stored = await financial.find_one({'request_id': session['request_id']})
        assert isinstance(stored['_id'], bson.ObjectId)
        assert stored['api_key_id'] == owner['id'] and stored['user_id'] == owner['_user_id']
        amount = stored['resources']['audit-accounting/a5000/transcription']
        assert isinstance(amount, bson.decimal128.Decimal128)
        assert amount.to_decimal() == Decimal('1.0000625')
        expected_time = datetime.datetime(2026, 6, 1, 0, 0, 2, 123000, datetime.UTC)
        assert stored['created_at'] == expected_time
        STAGE = 'lost_insert_reply'
        session2, event2 = scenario(owner)
        sessions.append(session2['session_id'])
        events.append(event2['event_id'])
        await accounting.bind_session(adapter, session2)
        await adapter.accounting_prepare()
        original = adapter._accounting_financial

        class LostReply:
            async def insert_one(self, document):
                await original.insert_one(document)
                raise AutoReconnect('injected ambiguous insert reply')

            def __getattr__(self, name):
                return getattr(original, name)
        adapter._accounting_financial = LostReply()
        try:
            try:
                await accounting.record_receipt(adapter, session2, event2)
            except accounting.AccountingUnavailable:
                pass
            else:
                raise AssertionError('Ambiguous insert outcome was hidden')
        finally:
            adapter._accounting_financial = original
        assert not await accounting.record_receipt(adapter, session2, event2)
        assert await financial.count_documents({'request_id': session2['request_id']}) == 1
        STAGE = 'two_process_same_receipt'
        session3, event3 = scenario(owner)
        sessions.append(session3['session_id'])
        events.append(event3['event_id'])
        await accounting.bind_session(adapter, session3)
        ctx = multiprocessing.get_context('spawn')
        barrier, results = ctx.Barrier(2), ctx.Queue()
        processes = [ctx.Process(target=writer, args=(session3, event3, barrier, results)) for _ in range(2)]
        try:
            for process in processes:
                process.start()
            answers = [await asyncio.to_thread(results.get, True, 12) for _ in processes]
            assert sorted(answers) == [False, True]
        finally:
            for process in processes:
                await asyncio.to_thread(process.join, 5)
                if process.is_alive():
                    process.terminate()
                    await asyncio.to_thread(process.join, 5)
            results.close()
        assert all(process.exitcode == 0 for process in processes)
        assert await financial.count_documents({'request_id': session3['request_id']}) == 1
        STAGE = 'process_death_after_financial_insert'
        session4, event4 = scenario(owner)
        sessions.append(session4['session_id'])
        events.append(event4['event_id'])
        await accounting.bind_session(adapter, session4)
        process = ctx.Process(target=writer, args=(session4, event4, None, None, True))
        process.start()
        await asyncio.to_thread(process.join, 12)
        if process.is_alive():
            process.terminate()
            await asyncio.to_thread(process.join, 5)
        assert process.exitcode == 93
        pending = await accounting.incomplete_receipts(adapter, session4['session_id'])
        assert [item['event_id'] for item in pending] == [event4['event_id']]
        assert not await accounting.record_receipt(adapter, session4, event4)
        assert await financial.count_documents({'request_id': session4['request_id']}) == 1
        STAGE = 'ack_outbox_and_session_state'
        assert len(await accounting.unacked_receipts(adapter, session4['session_id'])) == 1
        await accounting.set_session_status(adapter, session4['session_id'], 'settled')
        pending_sessions = await accounting.pending_sessions(adapter)
        assert session4['session_id'] in [item['session_id'] for item in pending_sessions]
        await accounting.mark_acked(adapter, event4['event_id'])
        await accounting.advance_cursor(adapter, session4['session_id'], 1)
        await accounting.set_session_status(adapter, session4['session_id'], 'settled')
        assert await accounting.unacked_receipts(adapter, session4['session_id']) == []
        assert session4['session_id'] not in [item['session_id'] for item in await accounting.pending_sessions(adapter)]
        STAGE = 'recovery_health_state'
        assert await accounting.healthy(adapter)
        await accounting.set_session_status(adapter, session3['session_id'], 'retry', 'backend_unavailable')
        assert not await accounting.healthy(adapter)
        await accounting.set_session_status(adapter, session3['session_id'], 'pending')
        assert await accounting.healthy(adapter)
        STAGE = 'conflicting_payload_and_overlap'
        changed = copy.deepcopy(event)
        changed.update(sample_count=16000, usage={'type': 'duration', 'seconds': 1})
        try:
            await accounting.record_receipt(adapter, session, changed)
        except accounting.AccountingConflict:
            pass
        else:
            raise AssertionError('Conflicting receipt was accepted')
        overlap = copy.deepcopy(event)
        overlap.update(segment_id='seg_2', event_id=event['event_id'].replace('seg_1', 'seg_2'), cursor=2)
        try:
            await accounting.record_receipt(adapter, session, overlap)
        except accounting.AccountingConflict:
            pass
        else:
            raise AssertionError('Overlapping source samples were billed')
        assert await financial.count_documents({'api_key_id': owner['id']}) == 4
        print(json.dumps({'status': 'PASS', 'namespace': os.environ['AUDIT_NAMESPACE'],
            'bson_objectid': True, 'exact_decimal128': True, 'completion_time_ms': True,
            'ambiguous_insert_retry': True, 'two_processes_one_bill': True,
            'crash_after_insert_retry': True, 'conflicts_and_overlap_rejected': True,
            'pending_recovery_and_ack_outbox': True}), flush=True)
    finally:
        # Only this probe's fresh IDs; never delete arbitrary namespace data.
        await financial.delete_many({'api_key_id': owner['id']})
        await adapter.db.cgc.llmproxy_accounting_receipt.delete_many({'_id': {'$in': events}})
        await adapter.db.cgc.llmproxy_accounting_session.delete_many({'_id': {'$in': sessions}})
        await shutdown_all()


if __name__ == '__main__':
    try:
        asyncio.run(probe())
    except Exception as error:
        print(json.dumps({'status': 'FAIL', 'stage': STAGE,
            'exception_type': type(error).__name__, 'code': getattr(error, 'code', None)}), flush=True)
        sys.exit(1)
