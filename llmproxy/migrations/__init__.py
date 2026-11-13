"""Add-only SQLite migrations, serialized with an immediate transaction."""

import importlib.resources
import sqlite3


class SchemaVersionError(ValueError):
    pass


async def _execute_script(connection, script):
    # executescript implicitly commits a pre-existing transaction. Execute
    # complete statements separately so the version and DDL commit together.
    statement = ""
    for line in script.splitlines(keepends=True):
        statement += line
        if sqlite3.complete_statement(statement):
            await connection.execute(statement)
            statement = ""
    if statement.strip():
        raise SchemaVersionError("Incomplete migration statement")


async def _validate_schema(connection):
    columns = await connection.execute_fetchall('PRAGMA table_info(admission_lease)')
    actual = [(row[1], row[2], row[3], row[5]) for row in columns]
    if actual != [('bucket', 'TEXT', 1, 1), ('owner', 'TEXT', 1, 2), ('expires_at', 'REAL', 1, 0)]:
        raise SchemaVersionError('Incompatible admission lease schema')
    indexes = await connection.execute_fetchall('PRAGMA index_list(api_key)')
    if not any(row[1] == 'api_key_type_secret' for row in indexes):
        raise SchemaVersionError('Missing exact credential lookup index')
    for table, required in (
            ('accounting_session', {'session_id', 'binding', 'binding_hash', 'backend_instance_id',
                'reconcile_cursor', 'status', 'last_error'}),
            ('accounting_receipt', {'event_id', 'session_id', 'backend_instance_id', 'cursor',
                'segment_id', 'start_sample', 'sample_count', 'fingerprint', 'receipt',
                'status', 'billing_event_id', 'quantity_exact', 'acked_at'}),
            ('conversion_delivery', {'delivery_key', 'binding', 'fingerprint', 'state', 'financial_id'}),
            ('http_attempt', {'attempt_id', 'binding', 'fingerprint', 'state', 'resources', 'observed_at'}),
            ('http_attempt_financial', {'attempt_id', 'product', 'financial_id'})):
        columns = await connection.execute_fetchall('PRAGMA table_info(' + table + ')')
        if {row[1] for row in columns} != required or not any(row[5] == 1 for row in columns):
            raise SchemaVersionError('Incompatible accounting schema')


async def upgrade_sqlite(connection):
    current, = (await connection.execute_fetchall('PRAGMA user_version'))[0]
    if current == 6:
        # Current-schema connections need only bounded read-only validation;
        # authentication/admission must not compete for the financial writer.
        await _validate_schema(connection)
        return
    if not 0 <= current <= 6:
        raise SchemaVersionError('Unsupported database schema version')
    await connection.execute("BEGIN IMMEDIATE")
    try:
        # Execute and consume bounded metadata on the worker thread. A cursor
        # retained by a cancelled auth-watch/schema-read can hold a SQLite
        # read lock even after connection.close(), blocking financial COMMIT.
        version, = (await connection.execute_fetchall('PRAGMA user_version'))[0]
        if not 0 <= version <= 5:
            raise SchemaVersionError("Unsupported database schema version")
        if version == 0:
            schema = importlib.resources.files("llmproxy").joinpath("schema.sql").read_text()
            await _execute_script(connection, schema)
            await connection.execute("PRAGMA user_version=1")
            version = 1
        if version == 1:
            migration = importlib.resources.files(__package__).joinpath("002-admission.sql").read_text()
            await _execute_script(connection, migration)
            await connection.execute("PRAGMA user_version=2")
            version = 2
        if version == 2:
            migration = importlib.resources.files(__package__).joinpath("003-accounting.sql").read_text()
            await _execute_script(connection, migration)
            await connection.execute("PRAGMA user_version=3")
            version = 3
        if version == 3:
            migration = importlib.resources.files(__package__).joinpath('004-delivery.sql').read_text()
            await _execute_script(connection, migration)
            await connection.execute('PRAGMA user_version=4')
            version = 4
        if version == 4:
            migration = importlib.resources.files(__package__).joinpath('005-http-attempt.sql').read_text()
            await _execute_script(connection, migration)
            await connection.execute('PRAGMA user_version=5')
            version = 5
        if version == 5:
            migration = importlib.resources.files(__package__).joinpath('006-auth-index.sql').read_text()
            await _execute_script(connection, migration)
            await connection.execute('PRAGMA user_version=6')
        await _validate_schema(connection)
        await connection.commit()
    except BaseException:
        await connection.rollback()
        raise
