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
	# SQLite datetime('now','-7 days') -> Postgres NOW() - INTERVAL '7 days'
	s = re.sub(
		r"datetime\('now','-([0-9]+)\s+days?'\)",
		r"NOW() - INTERVAL '\1 days'",
		s,
		flags=re.IGNORECASE,
	)
	# Cast published_at (with optional table alias) to timestamp for comparisons
	s = re.sub(
		r"(\b[\w\.]*published_at\b)\s*>=",
		r"CAST(\1 AS TIMESTAMP) >=",
		s,
		flags=re.IGNORECASE,
	)
	s = re.sub(
		r"(\b[\w\.]*published_at\b)\s*<=",
		r"CAST(\1 AS TIMESTAMP) <=",
		s,
		flags=re.IGNORECASE,
	)
	return s


class ProxyCursor:
	def __init__(self, cur):
		self._cur = cur

	def execute(self, sql, params=()):
		sql = _adapt_sql(sql, True)
		return self._cur.execute(sql, params)

	def executemany(self, sql, seq):
		sql = _adapt_sql(sql, True)
		return self._cur.executemany(sql, seq)

	def __getattr__(self, name):
		return getattr(self._cur, name)


class ProxyConn:
	def __init__(self, conn):
		self._conn = conn
		self.autocommit = True

	def cursor(self, *args, **kwargs):
		# ensure we don't pass duplicate cursor_factory
		kwargs.pop("cursor_factory", None)
		cur = self._conn.cursor(cursor_factory=RealDictCursor, *args, **kwargs)
		return ProxyCursor(cur)

	def __getattr__(self, name):
		return getattr(self._conn, name)


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
		conn.autocommit = True
		return ProxyConn(conn)

	# local sqlite fallback
	Path(db_path).parent.mkdir(parents=True, exist_ok=True)
	conn = sqlite3.connect(db_path)
	conn.row_factory = sqlite3.Row
	return conn


def exec_one(conn: Any, sql: str, params: tuple = ()) -> None:
	remote = is_remote()
	sql = _adapt_sql(sql, remote)
	cur = conn.cursor(cursor_factory=RealDictCursor) if remote else conn.cursor()
	cur.execute(sql, params)
	if hasattr(conn, "commit") and not remote:
		conn.commit()


def exec_many(conn: Any, sql: str, rows: Iterable[tuple]) -> None:
	remote = is_remote()
	sql = _adapt_sql(sql, remote)
	cur = conn.cursor(cursor_factory=RealDictCursor) if remote else conn.cursor()
	if hasattr(cur, "executemany"):
		cur.executemany(sql, rows)
	else:
		for row in rows:
			cur.execute(sql, row)
	if hasattr(conn, "commit") and not remote:
		conn.commit()
