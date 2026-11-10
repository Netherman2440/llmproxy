"""Hand-authored receipt-v1 fixtures; no credentials or transcript storage."""
import copy


SESSION = '1' * 32
INSTANCE = '2' * 32
MODEL = 'nvidia/parakeet-tdt-0.6b-v3'


def binding():
    return {'session_id': SESSION, 'request_id': 'fixture-request',
        'owner': {'id': 'fixture-key', '_user_id': 'fixture-user',
            '_namespace': 'audit-fixture', '_org_id': 'fixture-org', '_tier': 'STANDARD'},
        'backend_alias': 'parakeet', 'backend_url': 'http://private-stt:8000',
        'model_id': MODEL, 'device': 'a5000', 'max_segment_samples': 448000,
        'created_at': '2026-06-01T00:00:00.000000+00:00', 'backend_instance_id': INSTANCE}


def receipt():
    return {'event_id': INSTANCE + ':' + SESSION + ':seg_1',
        'session_id': SESSION, 'segment_id': 'seg_1', 'start_sample': 0,
        'sample_count': 16000, 'state': 'completed',
        'committed_at': '2026-06-01T00:00:01.000000+00:00',
        'protocol_version': 'receipt-v1', 'backend_instance_id': INSTANCE,
        'sample_rate': 16000, 'cursor': 1,
        'terminal_at': '2026-06-01T00:00:02.123456+00:00',
        'completed_at': '2026-06-01T00:00:02.123456+00:00',
        'provenance': {'code_revision': 'fixture-revision', 'model_id': MODEL},
        'usage': {'type': 'duration', 'seconds': 1.0}}


def completed_event():
    value = receipt()
    return {'type': 'conversation.item.input_audio_transcription.completed',
        'item_id': 'seg_1', 'transcript': 'not persisted in the accounting ledger',
        'usage': copy.deepcopy(value['usage']), 'receipt': value}
