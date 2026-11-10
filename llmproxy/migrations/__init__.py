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
        if not 0 <= version <= 2:
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
        columns = await (await connection.execute("PRAGMA table_info(admission_lease)")).fetchall()
        actual = [(row[1], row[2], row[3], row[5]) for row in columns]
        if actual != [("bucket", "TEXT", 1, 1), ("owner", "TEXT", 1, 2), ("expires_at", "REAL", 1, 0)]:
            raise SchemaVersionError("Incompatible admission lease schema")
        await connection.commit()
    except BaseException:
        await connection.rollback()
        raise
