"""Shared PostgreSQL connection for Part 3 (Serve).

Defaults match the postgres service in docker-compose.yml; override with the
usual PG* environment variables.
"""
import os
from pathlib import Path

import psycopg

SCHEMA = Path(__file__).with_name("schema.sql")


def connect() -> psycopg.Connection:
    return psycopg.connect(
        host=os.environ.get("PGHOST", "localhost"),
        port=int(os.environ.get("PGPORT", "5432")),
        dbname=os.environ.get("PGDATABASE", "divvy"),
        user=os.environ.get("PGUSER", "divvy"),
        password=os.environ.get("PGPASSWORD", "divvy"),
        # Autocommit, so every `with conn.transaction()` block is a real
        # transaction that commits when it ends. Without it, the first plain
        # query opens a transaction that is never committed: later blocks
        # become savepoints inside it, and closing the connection rolls back
        # everything.
        autocommit=True,
    )


def apply_schema(conn: psycopg.Connection) -> None:
    with conn.transaction():
        conn.execute(SCHEMA.read_text())
