"""Versioned least-privilege runtime contract; provisioning is an admin action."""
import argparse
import json
import re


def runtime_privileges(database='cgc', *, receipt_v1=False, shared_admission=False):
    if not isinstance(database, str) or not re.fullmatch(r'[A-Za-z0-9_][A-Za-z0-9_-]{0,62}', database):
        raise ValueError('Explicit database name required, no wildcards')
    if type(receipt_v1) is not bool or type(shared_admission) is not bool:
        raise ValueError('Runtime profiles must be boolean')
    collections = {'api_keys': ['find'], 'rest_users': ['find'],
        'billing_record': ['find', 'insert']}
    # Delivery recovery runs regardless of the currently configured backends:
    # removing a backend must not erase its retained financial obligations.
    journals = ['llmproxy_conversion_delivery', 'llmproxy_http_attempt']
    if receipt_v1:
        journals += ['llmproxy_accounting_session', 'llmproxy_accounting_receipt']
    if shared_admission:
        journals += ['admission_bucket']
    for name in journals:
        collections[name] = ['find', 'insert', 'update', 'createIndex']
    return [{'resource': {'db': database, 'collection': name}, 'actions': actions}
        for name, actions in collections.items()]


def main():
    parser = argparse.ArgumentParser(description='Print Mongo runtime privileges; never provision or print credentials')
    parser.add_argument('--database', default='cgc')
    parser.add_argument('--receipt-v1', action='store_true')
    parser.add_argument('--shared-admission', action='store_true')
    args = parser.parse_args()
    print(json.dumps(runtime_privileges(args.database, receipt_v1=args.receipt_v1,
        shared_admission=args.shared_admission), indent=2))


if __name__ == '__main__':
    main()
