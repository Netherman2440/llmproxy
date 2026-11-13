import importlib
import importlib.util
import unittest


class TestMongoRuntimePrivileges(unittest.TestCase):
    def setUp(self):
        self.assertIsNotNone(importlib.util.find_spec('llmproxy.mongo_roles'), 'Versioned minimal role contract missing')
        self.roles = importlib.import_module('llmproxy.mongo_roles')

    def actions(self, privileges):
        return {row['resource']['collection']: set(row['actions']) for row in privileges}

    def test_default_profile_has_exact_read_auth_and_append_verify_finance_privileges(self):
        privileges = self.roles.runtime_privileges()
        actions = self.actions(privileges)
        self.assertEqual(actions['api_keys'], {'find'})
        self.assertEqual(actions['rest_users'], {'find'})
        self.assertEqual(actions['billing_record'], {'find', 'insert'})
        self.assertEqual(set(actions), {'api_keys', 'rest_users', 'billing_record', 'llmproxy_conversion_delivery', 'llmproxy_http_attempt'})
        self.assertTrue(all(row['resource']['db'] == 'cgc' for row in privileges))

    def test_optional_profiles_add_only_their_own_journals(self):
        actions = self.actions(self.roles.runtime_privileges(receipt_v1=True, shared_admission=True))
        self.assertEqual(set(actions), {'api_keys', 'rest_users', 'billing_record',
            'llmproxy_conversion_delivery', 'llmproxy_http_attempt', 'llmproxy_accounting_session',
            'llmproxy_accounting_receipt', 'admission_bucket'})
        for name in ('llmproxy_conversion_delivery', 'llmproxy_http_attempt', 'llmproxy_accounting_session',
                     'llmproxy_accounting_receipt', 'admission_bucket'):
            self.assertEqual(actions[name], {'find', 'insert', 'update', 'createIndex'})
        forbidden = {'remove', 'dropCollection', 'dropDatabase', 'updateUser', 'grantRole', 'anyAction'}
        self.assertFalse(any(forbidden & value for value in actions.values()))

    def test_disposable_database_scope_is_explicit_and_never_a_wildcard(self):
        rows = self.roles.runtime_privileges(database='audit_llmproxy_fixture', receipt_v1=True)
        self.assertTrue(all(row['resource']['db'] == 'audit_llmproxy_fixture' for row in rows))
        for value in ('', None, '*', 'other/db', 'admin.$cmd', 'a' * 64):
            with self.subTest(value=value), self.assertRaises(ValueError):
                self.roles.runtime_privileges(database=value)
        for value in ('yes', 1, None):
            with self.subTest(flag=value), self.assertRaises(ValueError):
                self.roles.runtime_privileges(receipt_v1=value)
