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


async def upgrade_sqlite(connection):
    await connection.execute("BEGIN IMMEDIATE")
    try:
        version, = await (await connection.execute("PRAGMA user_version")).fetchone()
        if not 0 <= version <= 3:
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
        columns = await (await connection.execute("PRAGMA table_info(admission_lease)")).fetchall()
        actual = [(row[1], row[2], row[3], row[5]) for row in columns]
        if actual != [("bucket", "TEXT", 1, 1), ("owner", "TEXT", 1, 2), ("expires_at", "REAL", 1, 0)]:
            raise SchemaVersionError("Incompatible admission lease schema")
        for table, required in (
                ('accounting_session', {'session_id', 'binding', 'binding_hash', 'backend_instance_id',
                    'reconcile_cursor', 'status', 'last_error'}),
                ('accounting_receipt', {'event_id', 'session_id', 'backend_instance_id', 'cursor',
                    'segment_id', 'start_sample', 'sample_count', 'fingerprint', 'receipt',
                    'status', 'billing_event_id', 'quantity_exact', 'acked_at'})):
            columns = await (await connection.execute('PRAGMA table_info(' + table + ')')).fetchall()
            if {row[1] for row in columns} != required or not any(row[5] == 1 for row in columns):
                raise SchemaVersionError('Incompatible accounting schema')
        await connection.commit()
    except BaseException:
        await connection.rollback()
        raise
