import os
import re
import sqlite3
from pathlib import Path
from typing import Iterable, Any

try:
	import psycopg2
	from psycopg2.extras import RealDictCursor
except ImportError:
	psycopg2 = None  # optional if using sqlite only

POSTGRES_URL = os.getenv("POSTGRES_URL", "").strip()


def is_remote() -> bool:
	return bool(POSTGRES_URL)


def _patch_pg_cursor(conn):
	# default cursor returns tuples; we want dict-like rows for dict(r)
	conn.autocommit = True
	_conn_cursor = conn.cursor

	def _cursor(*args, **kwargs):
		return _conn_cursor(cursor_factory=RealDictCursor, *args, **kwargs)

	# psycopg2 connection.cursor is read-only; just wrap when called
	conn._dict_cursor = _cursor  # type: ignore
	return conn


def _adapt_sql(sql: str, remote: bool) -> str:
	if not remote:
		return sql
	s = sql
	# placeholder conversion
	s = s.replace("?", "%s")
	# DDL tweaks
	s = s.replace("INTEGER PRIMARY KEY AUTOINCREMENT", "SERIAL PRIMARY KEY")
	s = s.replace("INTEGER PRIMARY KEY", "SERIAL PRIMARY KEY")
	s = s.replace("DEFAULT (datetime('now'))", "DEFAULT CURRENT_TIMESTAMP")
	s = re.sub(r"datetime\\('now'\\)", "CURRENT_TIMESTAMP", s)
	return s


def get_conn(db_path: str):
	if is_remote():
		if psycopg2 is None:
			raise RuntimeError("psycopg2-binary not installed; add it to requirements.txt")
		# Ensure sslmode=require for Neon/Supabase if not provided
		url = POSTGRES_URL
		if "sslmode" not in url:
			if "?" in url:
				url += "&sslmode=require"
			else:
				url += "?sslmode=require"
		conn = psycopg2.connect(url)
		return _patch_pg_cursor(conn)

	# local sqlite fallback
	Path(db_path).parent.mkdir(parents=True, exist_ok=True)
	conn = sqlite3.connect(db_path)
	conn.row_factory = sqlite3.Row
	return conn


def exec_one(conn: Any, sql: str, params: tuple = ()) -> None:
	remote = is_remote()
	sql = _adapt_sql(sql, remote)
	cur = conn._dict_cursor() if remote and hasattr(conn, "_dict_cursor") else conn.cursor()
	cur.execute(sql, params)
	if hasattr(conn, "commit") and not remote:
		conn.commit()


def exec_many(conn: Any, sql: str, rows: Iterable[tuple]) -> None:
	remote = is_remote()
	sql = _adapt_sql(sql, remote)
	cur = conn._dict_cursor() if remote and hasattr(conn, "_dict_cursor") else conn.cursor()
	if hasattr(cur, "executemany"):
		cur.executemany(sql, rows)
	else:
		for row in rows:
			cur.execute(sql, row)
	if hasattr(conn, "commit") and not remote:
		conn.commit()
