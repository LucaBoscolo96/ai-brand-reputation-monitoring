import sqlite3
from pathlib import Path
from typing import Iterable


def get_conn(db_path: str) -> sqlite3.Connection:
	Path(db_path).parent.mkdir(parents=True, exist_ok=True)
	conn = sqlite3.connect(db_path)
	conn.row_factory = sqlite3.Row
	return conn


def exec_one(conn: sqlite3.Connection, sql: str, params: tuple = ()) -> None:
	cur = conn.cursor()
	cur.execute(sql, params)
	conn.commit()


def exec_many(conn: sqlite3.Connection, sql: str, rows: Iterable[tuple]) -> None:
	cur = conn.cursor()
	cur.executemany(sql, rows)
	conn.commit()
