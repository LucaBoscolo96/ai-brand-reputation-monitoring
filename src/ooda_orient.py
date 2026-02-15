import json
import os
from datetime import datetime
from typing import Dict, List
from concurrent.futures import ThreadPoolExecutor, as_completed

import yaml
from dotenv import load_dotenv
from openai import OpenAI
from openai import APIError, AuthenticationError, RateLimitError
from db import get_conn

MODEL = "gpt-5-mini"  # economico


def get_brand(cfg: Dict) -> str:
	return os.getenv("BRAND", cfg.get("project", {}).get("brand", "")).strip()


def fetch_latest_items(db_path: str, limit: int = 0) -> List[Dict]:
	conn = get_conn(db_path)
	cur = conn.cursor()
	sql = """
		SELECT r.id, r.title, r.url, r.content, r.metadata_json, r.brand
		FROM items_raw r
		LEFT JOIN items_orient o ON o.raw_item_id = r.id
		WHERE o.id IS NULL
		ORDER BY r.id DESC
	"""
	if limit and limit > 0:
		sql += " LIMIT ?"
		cur.execute(sql, (limit,))
	else:
		cur.execute(sql)
	rows_raw = [dict(r) for r in cur.fetchall()]
	conn.close()
	# Dedup by normalized title (keep most recent first because ordered DESC)
	seen = set()
	rows = []
	for r in rows_raw:
		title_raw = (r.get("title") or "").strip()
		title_norm = title_raw.lower()
		# try to strip trailing " - Source" to dedup same story across outlets
		if " - " in title_norm:
			title_norm = title_norm.rsplit(" - ", 1)[0]
		if title_norm in seen:
			continue
		seen.add(title_norm)
		rows.append(r)
	return rows


def api_smoke_test(client: OpenAI, brand: str) -> None:
	# test minimo: una completion piccola
	resp = client.chat.completions.create(
		model=MODEL,
		messages=[{"role": "user", "content": "Reply with a JSON object {\"ok\": true}"}],
		response_format={"type": "json_object"},
	)
	print(f"API smoke test OK for brand: {brand}")


def orient_item(client: OpenAI, item: Dict, brand: str) -> Dict:
	prompt = f"""
You are an AI Early Warning Analyst for brand reputation crises.

Brand under monitoring: {brand}

Analyze the following news item and assess its reputational risk.

Return ONLY valid JSON with these fields:
- claim_summary (1 sentence)
- narrative_category (one of: supply_chain, cultural_controversy, financial, fake_news, other)
- reputational_risk (low/medium/high)
- severity (0-100)
- confidence (0-1)
- verification_steps (list of 3 bullets)

NEWS TITLE: {item['title']}
NEWS SNIPPET: {item['content']}
URL: {item['url']}
"""

	response = client.chat.completions.create(
		model=MODEL,
		messages=[{"role": "user", "content": prompt}],
		response_format={"type": "json_object"},
		timeout=30,
	)

	return json.loads(response.choices[0].message.content)


def main():
	load_dotenv()
	api_key = os.getenv("OPENAI_API_KEY", "").strip()

	if not api_key:
		raise RuntimeError("Missing OPENAI_API_KEY in .env")

	client = OpenAI(api_key=api_key)

	with open("config.yaml", "r", encoding="utf-8") as f:
		cfg = yaml.safe_load(f)

	db_path = cfg["storage"]["db_path"]
	brand = get_brand(cfg) or "the brand"
	items = fetch_latest_items(db_path, limit=0)  # 0 = no limit

	# ✅ Smoke test per capire subito se key/model/rete sono ok
	try:
		api_smoke_test(client, brand)
	except AuthenticationError as e:
		print("\nAUTH ERROR: controlla OPENAI_API_KEY in .env")
		raise
	except RateLimitError as e:
		print("\nRATE LIMIT / QUOTA: potresti non avere credito o hai superato limiti.")
		raise
	except APIError as e:
		print("\nOPENAI API ERROR:", str(e))
		raise
	except Exception as e:
		print("\nGENERIC ERROR during smoke test:", repr(e))
		raise

	print(f"\nRunning ORIENT on {len(items)} items...\n")

	conn = get_conn(db_path)
	cur = conn.cursor()
	cur.execute("""
	CREATE TABLE IF NOT EXISTS items_orient (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		raw_item_id INTEGER,
		orient_json TEXT,
		created_at TEXT NOT NULL DEFAULT (datetime('now'))
	)
	""")

	for item in items:
		pass  # replaced by threaded approach below

	results = []
	max_workers = 20  # send up to 20 calls in parallelo
	with ThreadPoolExecutor(max_workers=max_workers) as ex:
		futures = {ex.submit(orient_item, client, item, brand): item for item in items}
		for fut in as_completed(futures):
			item = futures[fut]
			try:
				orient = fut.result()
				results.append((item, orient))
			except Exception as e:
				print("\nFAILED on item:", item["id"], item["title"])
				print("ERROR:", repr(e))

	# inserimento in DB (sequenziale)
	for item, orient in results:
		cur.execute("""
			INSERT INTO items_orient (raw_item_id, brand, orient_json)
			VALUES (?, ?, ?)
		""", (item["id"], item.get("brand", ""), json.dumps(orient, ensure_ascii=False)))

		print("----")
		print(item["title"])
		print("→", orient.get("reputational_risk"), "| severity:", orient.get("severity"))

	conn.commit()
	conn.close()
	print("\nDone. ORIENT outputs saved in DB table: items_orient")


if __name__ == "__main__":
	main()
