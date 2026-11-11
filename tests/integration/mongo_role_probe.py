"""Cold-store runtime role gate on a disposable DB in the isolated audit Mongo."""
import asyncio
import datetime
import decimal
import hashlib
import json
import os
import re
import secrets
import sys
import uuid
from urllib.parse import quote

STAGE = 'scope'


def uri(username, password):
    assert os.environ['AUDIT_NAMESPACE'] == 'izajac'
    assert os.environ['MONGO_HOST'] == 'audit-stt-mongo'
    return ('mongodb://' + quote(username, safe='') + ':' + quote(password, safe='')
        + '@audit-stt-mongo:27017/?authSource=admin&replicaSet=auditstt')


class ScopedClient:
    """Test-only namespace routing; operations/authorization use the real driver."""
    def __init__(self, client, database):
        assert re.fullmatch(r'audit_llmproxy_[0-9a-f]{32}', database)
        self.client, self.database = client, database

    def __getitem__(self, name):
        assert name == 'cgc'
        return self.client[self.database]

    def __getattr__(self, name):
        return getattr(self.client, name)


async def probe():
    global STAGE
    from bson import ObjectId
    from pymongo.errors import OperationFailure
    from llmproxy import accounting, delivery
    from llmproxy.db import MongoDatabase
    from llmproxy.mongo_roles import runtime_privileges
    from tests.accounting_fixtures import binding, completed_event

    nonce = uuid.uuid4().hex
    database = 'audit_llmproxy_' + nonce
    username = 'audit_runtime_' + nonce
    role = 'audit_runtime_' + nonce
    password = secrets.token_urlsafe(36)
    admin = await MongoDatabase._connect(uri(os.environ['MONGO_ADMIN_USERNAME'], os.environ['MONGO_ADMIN_PASSWORD']))
    runtime_client = None
    owned = False
    try:
        STAGE = 'isolated_ownership'
        assert (await admin.db.admin.command('hello')).get('setName') == 'auditstt'
        assert database not in await admin.db.list_database_names()
        owned = True
        print(json.dumps({'owned_fixture_database': database, 'owned_fixture_user': username,
            'owned_fixture_role': role, 'credentials_logged': False}), flush=True)
        STAGE = 'role_contract'
        privileges = runtime_privileges(database=database, receipt_v1=True, shared_admission=True)
        await admin.db.admin.command('createRole', role, privileges=privileges, roles=[])
        await admin.db.admin.command('createUser', username, pwd=password, roles=[{'role': role, 'db': 'admin'}])
        seed = admin.db[database]
        user_id, key_id, org_id = ObjectId(), ObjectId(), ObjectId()
        secret_hash = hashlib.sha256(b'synthetic-role-fixture-credential').hexdigest()
        await seed.rest_users.insert_one({'_id': user_id, 'namespace': 'audit-fixture',
            'org_id': org_id, 'subscription_level': 'STANDARD'})
        await seed.api_keys.insert_one({'_id': key_id, 'access_level': 'LLM',
            'user_id': user_id, 'secret': secret_hash, 'date_expiry': None, 'comment': 'fixture-private-comment'})
        runtime = await MongoDatabase._connect(uri(username, password))
        runtime_client = runtime.db
        runtime.db = ScopedClient(runtime_client, database)
        STAGE = 'authentication_reads'
        [user] = await runtime.user_list(secret_hash)
        assert user['id'] == key_id and user['_user_id'] == user_id
        assert user['_org_id'] == str(org_id)
        STAGE = 'legacy_financial_insert'
        await runtime.billing_record_add(user, datetime.datetime.now(datetime.UTC),
            {'audit/fixture/duration': decimal.Decimal('1.25')}, nonce)
        STAGE = 'cold_shared_admission_and_indexes'
        assert await runtime.admission_acquire('role-fixture', 'owner-a', 1, 10)
        assert not await runtime.admission_acquire('role-fixture', 'owner-b', 1, 10)
        assert await runtime.admission_renew('role-fixture', 'owner-a', 10)
        assert await runtime.admission_release('role-fixture', 'owner-a')
        STAGE = 'cold_receipt_journal_and_indexes'
        value = binding()
        value['owner'] = user
        await accounting.bind_session(runtime, value)
        assert await accounting.record_receipt(runtime, value, completed_event())
        assert not await accounting.record_receipt(runtime, value, completed_event())
        event_id = completed_event()['receipt']['event_id']
        await accounting.mark_acked(runtime, event_id)
        await accounting.advance_cursor(runtime, value['session_id'], 1)
        await accounting.set_session_status(runtime, value['session_id'], 'settled')
        assert await accounting.healthy(runtime)
        STAGE = 'cold_conversion_journal_and_indexes'
        intent = await delivery.prepare_delivery(runtime, {'owner': user, 'backend_alias': 'audit-conversion',
            'backend_url': 'http://audit-fixture', 'device': 'none', 'job_id': nonce,
            'request_id': nonce, 'created_at': '2026-06-01T00:00:00.123456+00:00'}, 7)
        assert await delivery.settle_delivery(runtime, intent, {'delivery_claimed': True})
        assert not await delivery.settle_delivery(runtime, intent)
        assert await delivery.pending_deliveries(runtime) == []
        assert await seed.billing_record.count_documents({}) == 3
        STAGE = 'implicit_own_user_info_is_not_admin_access'
        own = await runtime_client.admin.command('usersInfo', username)
        assert len(own['users']) == 1 and own['users'][0]['user'] == username
        STAGE = 'denied_out_of_contract_operations'
        scoped = runtime.db['cgc']
        operations = [lambda: scoped.api_keys.insert_one({'_id': ObjectId()}),
            lambda: scoped.rest_users.update_one({'_id': user_id}, {'$set': {'namespace': 'forbidden'}}),
            lambda: scoped.billing_record.update_one({'request_id': nonce}, {'$set': {'tier': 'forbidden'}}),
            lambda: scoped.billing_record.delete_one({}),
            lambda: scoped.drop_collection('billing_record'),
            lambda: runtime_client[database + '_foreign'].records.find_one({}),
            lambda: runtime_client.admin.command('usersInfo', os.environ['MONGO_ADMIN_USERNAME']),
            lambda: runtime_client.admin.command('createRole', role + '_forbidden', privileges=[], roles=[])]
        for index, operation in enumerate(operations):
            STAGE = 'denied_operation_' + str(index)
            try:
                await operation()
            except OperationFailure as error:
                assert error.code == 13
            else:
                raise AssertionError('Runtime role permits forbidden operation')
        assert await seed.billing_record.count_documents({}) == 3
        assert (await seed.rest_users.find_one({'_id': user_id}))['namespace'] == 'audit-fixture'
        print(json.dumps({'status': 'PASS', 'cold_journals': True, 'real_auth_reads': True,
            'legacy_receipt_delivery_billing': True, 'shared_admission': True,
            'forbidden_operations_denied': len(operations), 'customer_charges': False}), flush=True)
    finally:
        if runtime_client is not None:
            runtime_client.close()
        if owned:
            for command, name in (('dropUser', username), ('dropRole', role)):
                try:
                    await admin.db.admin.command(command, name)
                except OperationFailure as error:
                    if error.code not in (11, 31):  # UserNotFound / RoleNotFound
                        raise
            await admin.db.drop_database(database)
            print(json.dumps({'owned_fixture_database_removed': True,
                'owned_fixture_user_and_role_removed': True}), flush=True)
        admin.db.close()


if __name__ == '__main__':
    try:
        asyncio.run(probe())
    except Exception as error:
        print(json.dumps({'status': 'FAIL', 'stage': STAGE, 'exception_type': type(error).__name__}), flush=True)
        sys.exit(1)
