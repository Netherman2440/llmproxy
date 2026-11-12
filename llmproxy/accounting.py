"""Durable, credential-free session ownership and receipt accounting facade."""
import copy
import hashlib
import json
from urllib.parse import urlsplit

from .products import product as canonical_product
from .usage import ID, parse_receipt, timestamp


class AccountingConflict(ValueError):
    pass


class AccountingUnavailable(RuntimeError):
    pass


def _text(value, name, maximum=256, empty=False):
    if (type(value) is not str or len(value) > maximum or (not value and not empty)
            or any(ord(char) < 32 or ord(char) == 127 for char in value)):
        raise ValueError('Invalid accounting ' + name)
    return value


def _identifier(value):
    if type(value) is str:
        return _text(value, 'owner identifier')
    try:
        from bson import ObjectId
        if type(value) is ObjectId:
            return value
    except ImportError:
        pass
    raise ValueError('Invalid accounting owner identifier')


def _json_id(value):
    try:
        from bson import ObjectId
        if type(value) is ObjectId:
            return {'$oid': str(value)}
    except ImportError:
        pass
    raise ValueError('Unsupported accounting data type')


def encode(value):
    return json.dumps(value, default=_json_id, sort_keys=True, separators=(',', ':'), allow_nan=False)


def decode(value):
    def object_hook(item):
        if set(item) == {'$oid'}:
            from bson import ObjectId
            return ObjectId(item['$oid'])
        return item
    return json.loads(value, object_hook=object_hook)


def clean_owner(value):
    if type(value) is not dict:
        raise ValueError('Invalid accounting owner')
    owner = {'id': _identifier(value.get('id'))}
    if '_user_id' in value:
        owner['_user_id'] = _identifier(value['_user_id'])
    for name in ('_namespace', '_org_id', '_tier'):
        if name in value:
            owner[name] = _text(value[name], name, empty=True)
    return owner


def clean_binding(value):
    if type(value) is not dict or type(value.get('owner')) is not dict:
        raise ValueError('Invalid accounting binding')
    sid, instance = value.get('session_id'), value.get('backend_instance_id')
    if type(sid) is not str or not ID.fullmatch(sid):
        raise ValueError('Invalid accounting session identifier')
    if instance is not None and (type(instance) is not str or not ID.fullmatch(instance)):
        raise ValueError('Invalid accounting backend identity')
    result = {'session_id': sid, 'backend_instance_id': instance, 'owner': clean_owner(value['owner'])}
    for name in ('request_id', 'backend_alias', 'backend_url', 'model_id', 'device'):
        result[name] = _text(value.get(name), name, maximum=512)
    if any('/' in result[name] for name in ('backend_alias', 'device')):
        raise ValueError('Invalid accounting product')
    url = urlsplit(result['backend_url'])
    if (url.scheme not in ('http', 'https') or not url.hostname or url.username is not None
            or url.password is not None or url.query or url.fragment):
        raise ValueError('Invalid accounting backend URL')
    result['created_at'] = timestamp(value.get('created_at')).isoformat(timespec='microseconds')
    limit = value.get('max_segment_samples', 448000)
    if type(limit) is not int or not 1 <= limit <= 16000 * 3600:
        raise ValueError('Invalid accounting segment limit')
    result['max_segment_samples'] = limit
    return result


def binding_hash(value):
    business = clean_binding(value)
    business.pop('backend_instance_id')
    return hashlib.sha256(encode(business).encode()).hexdigest()


def product(binding):
    return canonical_product(binding['backend_alias'], binding['device'], 'transcription')


async def _call(awaitable):
    try:
        return await awaitable
    except (AccountingConflict, ValueError):
        raise
    except Exception:
        raise AccountingUnavailable('Durable accounting storage unavailable') from None


async def bind_session(db, binding):
    await _call(db.accounting_bind(clean_binding(binding)))


async def get_binding(db, session_id):
    return await _call(db.accounting_get(session_id))


async def attach_instance(db, session_id, instance_id):
    for value in (session_id, instance_id):
        if type(value) is not str or not ID.fullmatch(value):
            raise ValueError('Invalid accounting identity')
    await _call(db.accounting_attach(session_id, instance_id))


async def record_receipt(db, binding, event):
    durable = await get_binding(db, binding['session_id'])
    if durable is None or binding_hash(durable) != binding_hash(binding):
        raise AccountingConflict('Receipt owner does not match durable session')
    if binding.get('backend_instance_id') not in (None, durable.get('backend_instance_id')):
        raise AccountingConflict('Receipt backend does not match durable session')
    parsed = parse_receipt(event, durable)
    return await _call(db.accounting_record(copy.deepcopy(durable), parsed))


async def pending_sessions(db, limit=100, after=None):
    if type(limit) is not int or not 1 <= limit <= 100:
        raise ValueError('Invalid accounting page limit')
    return await _call(db.accounting_pending(limit, after))


async def unacked_receipts(db, session_id, limit=100):
    if type(limit) is not int or not 1 <= limit <= 100:
        raise ValueError('Invalid accounting receipt page limit')
    return await _call(db.accounting_unacked(session_id, limit))


async def healthy(db):
    return await _call(db.accounting_healthy())


async def incomplete_receipts(db, session_id, limit=100):
    if type(limit) is not int or not 1 <= limit <= 100:
        raise ValueError('Invalid accounting receipt page limit')
    return await _call(db.accounting_incomplete(session_id, limit))


async def set_session_status(db, session_id, status, reason=None, *, expected_status=None):
    if status not in ('pending', 'retry', 'blocked', 'settled') or reason not in (
            None, 'backend_unavailable', 'storage_unavailable', 'contract', 'retention_gap', 'route_missing'):
        raise ValueError('Invalid accounting session status')
    if expected_status not in (None, 'bound'):
        raise ValueError('Invalid accounting expected status')
    await _call(db.accounting_status(session_id, status, reason, expected_status))


async def mark_acked(db, event_id):
    await _call(db.accounting_ack(event_id))


async def advance_cursor(db, session_id, cursor):
    if type(cursor) is not int or not 0 <= cursor < 2**63:
        raise ValueError('Invalid reconciliation cursor')
    await _call(db.accounting_advance(session_id, cursor))
