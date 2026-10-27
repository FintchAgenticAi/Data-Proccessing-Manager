from __future__ import annotations

import re
import sqlite3
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterator

from app.config import DATABASE_PATH, DATABASE_URL

SQLITE_SCHEMA = """
CREATE TABLE IF NOT EXISTS supplier (
    supplier_id INTEGER PRIMARY KEY AUTOINCREMENT, supplier_name TEXT NOT NULL UNIQUE,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE TABLE IF NOT EXISTS data_batch (
    data_batch_id INTEGER PRIMARY KEY AUTOINCREMENT, supplier_id INTEGER NOT NULL REFERENCES supplier(supplier_id),
    batch_name TEXT NOT NULL, country TEXT NOT NULL, year INTEGER NOT NULL, version INTEGER NOT NULL,
    csv_filename TEXT NOT NULL, created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(supplier_id, batch_name, country, year, version)
);
CREATE TABLE IF NOT EXISTS raw_kegal_data (
    raw_record_id INTEGER PRIMARY KEY AUTOINCREMENT, data_batch_id INTEGER NOT NULL REFERENCES data_batch(data_batch_id),
    `row_number` INTEGER NOT NULL, data_json TEXT NOT NULL, parent_raw_record_id INTEGER REFERENCES raw_kegal_data(raw_record_id),
    processing_status TEXT NOT NULL DEFAULT 'received', transformed_data_json TEXT,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE TABLE IF NOT EXISTS quarantine_data (
    quarantine_id INTEGER PRIMARY KEY AUTOINCREMENT, raw_record_id INTEGER NOT NULL REFERENCES raw_kegal_data(raw_record_id),
    status TEXT NOT NULL, reason TEXT NOT NULL, created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE TABLE IF NOT EXISTS error_reason (
    error_id INTEGER PRIMARY KEY AUTOINCREMENT, quarantine_id INTEGER NOT NULL REFERENCES quarantine_data(quarantine_id),
    error_reason TEXT NOT NULL, created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE TABLE IF NOT EXISTS quarantine_export (
    export_id INTEGER PRIMARY KEY AUTOINCREMENT, error_id INTEGER NOT NULL REFERENCES error_reason(error_id),
    file_name TEXT NOT NULL, exported_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP, status TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS clean_data (
    clean_data_id INTEGER PRIMARY KEY AUTOINCREMENT, raw_record_id INTEGER NOT NULL REFERENCES raw_kegal_data(raw_record_id),
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE TABLE IF NOT EXISTS execution_results (
    execution_result_id INTEGER PRIMARY KEY AUTOINCREMENT, data_batch_id INTEGER NOT NULL REFERENCES data_batch(data_batch_id),
    result_status TEXT NOT NULL, processed_at TEXT, processing_count INTEGER NOT NULL DEFAULT 0,
    clean_count INTEGER NOT NULL DEFAULT 0, quarantine_count INTEGER NOT NULL DEFAULT 0,
    processing_status TEXT NOT NULL, started_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP, completed_at TEXT
);
CREATE TABLE IF NOT EXISTS rule_selected (
    selected_rule_id INTEGER PRIMARY KEY AUTOINCREMENT, supplier_id INTEGER NOT NULL REFERENCES supplier(supplier_id),
    rule_type TEXT NOT NULL, attribute_name TEXT NOT NULL, created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE TABLE IF NOT EXISTS rule_for_column (
    rule_for_column_id INTEGER PRIMARY KEY AUTOINCREMENT, selected_rule_id INTEGER NOT NULL REFERENCES rule_selected(selected_rule_id),
    rule_id TEXT NOT NULL, rule_order INTEGER NOT NULL, condition_type TEXT NOT NULL, created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE TABLE IF NOT EXISTS pipeline_execution (
    pipeline_execution_id INTEGER PRIMARY KEY AUTOINCREMENT, data_batch_id INTEGER NOT NULL REFERENCES data_batch(data_batch_id),
    status TEXT NOT NULL, started_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP, completed_at TEXT, error_message TEXT
);
CREATE TABLE IF NOT EXISTS service_call (
    service_call_id INTEGER PRIMARY KEY AUTOINCREMENT, pipeline_execution_id INTEGER NOT NULL REFERENCES pipeline_execution(pipeline_execution_id),
    service_name TEXT NOT NULL, stage_name TEXT NOT NULL, request_json TEXT NOT NULL, response_json TEXT,
    status TEXT NOT NULL, created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);
"""

MYSQL_SCHEMA = (
    SQLITE_SCHEMA.replace("INTEGER PRIMARY KEY AUTOINCREMENT", "BIGINT PRIMARY KEY AUTO_INCREMENT")
    .replace("created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP", "created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP")
    .replace("started_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP", "started_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP")
    .replace("completed_at TEXT", "completed_at DATETIME")
    .replace("processed_at TEXT", "processed_at DATETIME")
    .replace("exported_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP", "exported_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP")
    .replace("processing_status TEXT NOT NULL DEFAULT 'received'", "processing_status VARCHAR(50) NOT NULL DEFAULT 'received'")
)


class MySQLConnection:
    def __init__(self, connection: Any):
        self._connection = connection

    def execute(self, sql: str, parameters: tuple[Any, ...] = ()) -> Any:
        sql = sql.replace("?", "%s")
        sql = sql.replace("INSERT OR IGNORE", "INSERT IGNORE")
        sql = re.sub(
            r"ON CONFLICT\(([^)]+)\) DO UPDATE SET ([^ ]+)=excluded\.([^\s,]+)",
            r"ON DUPLICATE KEY UPDATE \2=VALUES(\3)",
            sql,
        )
        if sql.lower().startswith("insert into supplier") and "on duplicate key update" in sql.lower():
            sql = re.sub(
                r"ON DUPLICATE KEY UPDATE .+$",
                "ON DUPLICATE KEY UPDATE supplier_id=LAST_INSERT_ID(supplier_id), supplier_name=VALUES(supplier_name)",
                sql,
                flags=re.IGNORECASE,
            )
        returning = re.search(r"\s+RETURNING\s+([A-Za-z_][A-Za-z0-9_]*)\s*$", sql, re.IGNORECASE)
        if returning:
            sql = sql[: returning.start()]
        cursor = self._connection.cursor()
        cursor.execute(sql, parameters)
        if returning:
            value = cursor.lastrowid
            if returning.group(1).lower() == "data_batch_id" and "data_batch_id" in sql.split("VALUES", 1)[0]:
                value = parameters[0]
            return _ReturningResult(value, returning.group(1))
        return cursor

    def executescript(self, script: str) -> None:
        for statement in script.split(";"):
            statement = statement.strip()
            if statement:
                self.execute(statement)

    def commit(self) -> None:
        self._connection.commit()

    def rollback(self) -> None:
        self._connection.rollback()

    def close(self) -> None:
        self._connection.close()


class _ReturningResult:
    def __init__(self, value: int, column: str):
        self.value = value
        self.column = column

    def fetchone(self) -> dict[str, int]:
        return {self.column: self.value}


def _mysql_connection() -> MySQLConnection:
    import pymysql

    parsed = DATABASE_URL.removeprefix("mysql+pymysql://").removeprefix("mysql://")
    credentials, host_and_db = parsed.split("@", 1)
    username, password = credentials.split(":", 1)
    host_port, database = host_and_db.split("/", 1)
    host, _, port = host_port.partition(":")
    connection = pymysql.connect(
        host=host,
        port=int(port or 3306),
        user=username,
        password=password,
        database=database,
        charset="utf8mb4",
        cursorclass=pymysql.cursors.DictCursor,
        autocommit=False,
    )
    return MySQLConnection(connection)


def initialize_database(database_path: Path = DATABASE_PATH) -> None:
    if DATABASE_URL:
        db = _mysql_connection()
        try:
            db.executescript(MYSQL_SCHEMA)
            db.commit()
        finally:
            db.close()
        return
    database_path.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(database_path) as connection:
        connection.executescript(SQLITE_SCHEMA)
        columns = {row[1] for row in connection.execute("PRAGMA table_info(raw_kegal_data)")}
        if "processing_status" not in columns:
            connection.execute("ALTER TABLE raw_kegal_data ADD COLUMN processing_status TEXT NOT NULL DEFAULT 'received'")
        if "transformed_data_json" not in columns:
            connection.execute("ALTER TABLE raw_kegal_data ADD COLUMN transformed_data_json TEXT")


@contextmanager
def connection(database_path: Path = DATABASE_PATH) -> Iterator[Any]:
    if DATABASE_URL:
        db = _mysql_connection()
    else:
        initialize_database(database_path)
        db = sqlite3.connect(database_path)
        db.row_factory = sqlite3.Row
    try:
        yield db
        db.commit()
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()
