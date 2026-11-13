"""Strict receipt-v1 validation; samples, not client numbers, define quantity."""
import copy
import datetime
from decimal import Decimal, localcontext
import hashlib
import json
import re


COMPLETED_EVENT = 'conversation.item.input_audio_transcription.completed'
ID = re.compile(r'[a-f0-9]{32}')
SEGMENT = re.compile(r'seg_[1-9][0-9]{0,9}')
TIMESTAMP = re.compile(r'\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d(?:\.\d{1,6})?(?:Z|[+-]\d\d:\d\d)')
FIELDS = {'event_id', 'session_id', 'segment_id', 'start_sample', 'sample_count', 'state',
    'committed_at', 'protocol_version', 'backend_instance_id', 'sample_rate', 'cursor',
    'terminal_at', 'completed_at', 'provenance'}


def timestamp(value):
    if type(value) is not str or len(value) > 40 or not TIMESTAMP.fullmatch(value):
        raise ValueError('Invalid receipt timestamp')
    try:
        result = datetime.datetime.fromisoformat(value.replace('Z', '+00:00')).astimezone(datetime.UTC)
    except (ValueError, OverflowError):
        raise ValueError('Invalid receipt timestamp') from None
    if result > datetime.datetime.now(datetime.UTC) + datetime.timedelta(minutes=1):
        raise ValueError('Receipt timestamp is in the future')
    return result


def _integer(value, minimum, maximum, name):
    if type(value) is not int or not minimum <= value <= maximum:
        raise ValueError('Invalid receipt ' + name)


def _usage(value, quantity):
    if type(value) is not dict or set(value) != {'type', 'seconds'} or value['type'] != 'duration':
        raise ValueError('Invalid receipt usage')
    seconds = value['seconds']
    if type(seconds) not in (int, float, Decimal):
        raise ValueError('Invalid receipt seconds')
    try:
        decoded = Decimal(str(seconds))
    except (ValueError, ArithmeticError):
        raise ValueError('Invalid receipt seconds') from None
    if not decoded.is_finite() or decoded != quantity:
        raise ValueError('Receipt seconds do not match samples')


def parse_receipt(event, binding):
    if type(event) is not dict or type(binding) is not dict:
        raise ValueError('Receipt and binding must be objects')
    wrapped = 'receipt' in event
    value = event.get('receipt') if wrapped else event
    if type(value) is not dict or not FIELDS <= value.keys() or set(value) - FIELDS - {'usage'}:
        raise ValueError('Invalid receipt fields')
    for field in ('session_id', 'backend_instance_id'):
        identity = value[field]
        if type(identity) is not str or not ID.fullmatch(identity) or identity != binding.get(field):
            raise ValueError('Receipt identity does not match binding')
    segment = value['segment_id']
    if type(segment) is not str or not SEGMENT.fullmatch(segment):
        raise ValueError('Invalid receipt segment identifier')
    if value['event_id'] != value['backend_instance_id'] + ':' + value['session_id'] + ':' + segment:
        raise ValueError('Invalid receipt event identifier')
    if value['protocol_version'] != 'receipt-v1' or value['state'] not in ('completed', 'failed', 'interrupted'):
        raise ValueError('Unsupported receipt protocol or state')
    if type(value['sample_rate']) is not int or value['sample_rate'] != 16000:
        raise ValueError('Invalid receipt sample rate')
    maximum = binding.get('max_segment_samples', 448000)
    _integer(maximum, 1, 16000 * 3600, 'sample limit')
    _integer(value['sample_count'], 1, maximum, 'sample count')
    _integer(value['start_sample'], 0, 2**63 - 1 - value['sample_count'], 'sample range')
    _integer(value['cursor'], 1, 2**63 - 1, 'cursor')
    committed, terminal = timestamp(value['committed_at']), timestamp(value['terminal_at'])
    if terminal < committed:
        raise ValueError('Receipt terminal time precedes commit')
    provenance = value['provenance']
    if (type(provenance) is not dict or not {'code_revision', 'model_id'} <= provenance.keys()
            or set(provenance) - {'code_revision', 'model_id', 'model_revision', 'model_sha256'}
            or any(type(part) is not str or not part or len(part) > 512 for part in provenance.values())
            or provenance['model_id'] != binding.get('model_id')
            or len(json.dumps(provenance)) > 2048):
        raise ValueError('Invalid receipt provenance')
    if 'model_sha256' in provenance and not re.fullmatch(r'[a-f0-9]{64}', provenance['model_sha256']):
        raise ValueError('Invalid model digest')
    result = copy.deepcopy(value)
    quantity, completed = None, None
    if value['state'] == 'completed':
        completed = timestamp(value['completed_at'])
        if completed != terminal:
            raise ValueError('Receipt completion timestamps disagree')
        with localcontext() as context:
            context.prec = 40
            quantity = Decimal(value['sample_count']) / Decimal(16000)
        _usage(value.get('usage'), quantity)
        result['usage'] = {'type': 'duration', 'seconds': float(quantity)}
    elif value['completed_at'] is not None or value.get('usage') is not None:
        raise ValueError('Failed or interrupted work cannot have completed usage')
    if wrapped:
        if (event.get('type') != COMPLETED_EVENT or value['state'] != 'completed'
                or event.get('item_id') != segment):
            raise ValueError('Completed frame does not match receipt')
        _usage(event.get('usage'), quantity)
    for name, date in (('committed_at', committed), ('terminal_at', terminal), ('completed_at', completed)):
        result[name] = date.isoformat(timespec='microseconds') if date is not None else None
    canonical = copy.deepcopy(result)
    if quantity is not None:
        canonical['usage']['seconds'] = str(quantity)
    encoded = json.dumps(canonical, sort_keys=True, separators=(',', ':'), allow_nan=False)
    if len(encoded.encode()) > 8192:
        raise ValueError('Receipt metadata exceeds byte limit')
    return {'receipt': result, 'quantity': quantity, 'completed_time': completed,
        'fingerprint': hashlib.sha256(encoded.encode()).hexdigest()}
