"""
OLTP Database — Azure SQL via pyodbc.

Cloud replacement for the local SQLite db.py.
Uses the same table schema but connects to Azure SQL Database.
Connection string is read from environment variable SQL_CONNECTION_STRING.
"""

import os
import logging
import pyodbc

logger = logging.getLogger(__name__)

_connection_pool = None


def get_connection() -> pyodbc.Connection:
    """Return a pyodbc connection to Azure SQL.

    Reuses the module-level connection if still alive, otherwise creates a new one.
    Azure Functions recycles workers, so this keeps connections efficient within
    a single invocation batch.
    """
    global _connection_pool
    conn_str = os.environ["SQL_CONNECTION_STRING"]

    if _connection_pool is not None:
        try:
            _connection_pool.execute("SELECT 1")
            return _connection_pool
        except Exception:
            _connection_pool = None

    _connection_pool = pyodbc.connect(conn_str, autocommit=False)
    _connection_pool.setdecoding(pyodbc.SQL_CHAR, encoding='utf-8')
    _connection_pool.setdecoding(pyodbc.SQL_WCHAR, encoding='utf-8')
    logger.info("Connected to Azure SQL")
    return _connection_pool


def execute_query(query: str, params: tuple = (), commit: bool = False):
    """Execute a single query and return the cursor."""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute(query, params)
    if commit:
        conn.commit()
    return cursor


def execute_many(query: str, param_list: list, commit: bool = True):
    """Execute a parameterized query for multiple rows."""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.executemany(query, param_list)
    if commit:
        conn.commit()
    return cursor


def fetchone(query: str, params: tuple = ()) -> dict:
    """Execute and fetch one row as a dict, or None."""
    cursor = execute_query(query, params)
    columns = [col[0] for col in cursor.description] if cursor.description else []
    row = cursor.fetchone()
    if row is None:
        return None
    return dict(zip(columns, row))


def fetchall(query: str, params: tuple = ()) -> list:
    """Execute and fetch all rows as a list of dicts."""
    cursor = execute_query(query, params)
    columns = [col[0] for col in cursor.description] if cursor.description else []
    return [dict(zip(columns, row)) for row in cursor.fetchall()]


def commit():
    """Commit the current transaction."""
    conn = get_connection()
    conn.commit()
