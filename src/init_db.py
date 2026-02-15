try:
	from dotenv import load_dotenv
except ImportError:
	def load_dotenv(*args, **kwargs):
		# fallback: silently skip if python-dotenv not installed
		return False
from db import get_conn, exec_one
from db import is_remote


DDL = """
CREATE TABLE IF NOT EXISTS items_raw (
	id INTEGER PRIMARY KEY AUTOINCREMENT,

	source TEXT NOT NULL,
	source_item_id TEXT NOT NULL,
	brand TEXT NOT NULL,

	title TEXT NOT NULL,
	url TEXT NOT NULL,

	content TEXT,
	metadata_json TEXT,

	published_at TEXT NOT NULL,  -- mandatory
	created_at TEXT NOT NULL DEFAULT (datetime('now')),

	UNIQUE(source, source_item_id)
);

CREATE INDEX IF NOT EXISTS idx_items_raw_url ON items_raw(url);

CREATE TABLE IF NOT EXISTS items_orient (
	id INTEGER PRIMARY KEY AUTOINCREMENT,
	raw_item_id INTEGER,
	brand TEXT NOT NULL,
	orient_json TEXT,
	created_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS items_decide (
	id INTEGER PRIMARY KEY AUTOINCREMENT,
	raw_item_id INTEGER,
	orient_id INTEGER,
	brand TEXT NOT NULL,
	decide_json TEXT,
	created_at TEXT NOT NULL DEFAULT (datetime('now'))
);
"""


def main():
	import yaml
	from pathlib import Path

	load_dotenv()  # load POSTGRES_URL if present in .env

	with open("config.yaml", "r", encoding="utf-8") as f:
		cfg = yaml.safe_load(f)

	db_path = cfg["storage"]["db_path"]
	if not is_remote():
		Path(db_path).parent.mkdir(parents=True, exist_ok=True)

	conn = get_conn(db_path)
	if is_remote():
		print("DB target: Postgres (POSTGRES_URL detected, sslmode=require)")
	else:
		print(f"DB target: SQLite ({db_path})")

	for stmt in DDL.strip().split(";"):
		stmt = stmt.strip()
		if stmt:
			exec_one(conn, stmt + ";")
	conn.close()
	print(f"DB initialized: {db_path}")


if __name__ == "__main__":
	main()
