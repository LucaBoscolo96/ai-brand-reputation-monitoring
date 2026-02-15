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


def orient_batch(client: OpenAI, brand: str, batch: List[Dict]) -> List[Dict]:
	"""
	Batch more items in a single call; response keeps item_id to re-map.
	"""
	parts = []
	for item in batch:
		parts.append(
			{
				"item_id": item["id"],
				"title": (item.get("title") or "")[:500],
				"snippet": (item.get("content") or "")[:1000],
				"url": item.get("url") or "",
			}
		)

	prompt = {
		"instruction": "For each item return a JSON object with key 'items' (array) and include all fields; keep item_id to match inputs.",
		"brand": brand,
		"items": parts,
		"schema": {
			"item_id": "int (echo input)",
			"claim_summary": "string, 1 sentence",
			"narrative_category": "supply_chain|cultural_controversy|financial|fake_news|other",
			"reputational_risk": "low|medium|high",
			"severity": "0-100",
			"confidence": "0-1",
			"verification_steps": "list of 3 bullets",
		},
	}

	prompt_text = json.dumps(prompt, ensure_ascii=False)

	resp = client.chat.completions.create(
		model=MODEL,
		messages=[
			{
				"role": "user",
				"content": (
					"Return ONLY JSON. Respond with a single JSON object containing key 'items'. "
					"Do not add prose or explanations.\n\n"
					+ prompt_text
				),
			}
		],
		response_format={"type": "json_object"},
		timeout=45,
	)

	payload = json.loads(resp.choices[0].message.content)
	return payload.get("items", [])


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

	print(f"\nRunning ORIENT on {len(items)} items (batched)...\n")

	conn = get_conn(db_path)
	cur = conn.cursor()
	cur.execute("""
	CREATE TABLE IF NOT EXISTS items_orient (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		raw_item_id INTEGER,
		brand TEXT,
		orient_json TEXT,
		created_at TEXT NOT NULL DEFAULT (datetime('now'))
	)
	""")

	chunk_size = 5
	batches = [items[i:i + chunk_size] for i in range(0, len(items), chunk_size)]

	results = []
	max_workers = max(1, min(6, len(batches)))  # parallelize batches cautiously
	with ThreadPoolExecutor(max_workers=max_workers) as ex:
		futures = {ex.submit(orient_batch, client, brand, batch): batch for batch in batches}
		for fut in as_completed(futures):
			batch = futures[fut]
			try:
				out_items = fut.result()
				results.append((batch, out_items))
			except Exception as e:
				print("\nFAILED batch containing ids:", [it.get("id") for it in batch])
				print("ERROR:", repr(e))

	for batch, out_items in results:
		id_map = {it["id"]: it for it in batch}
		for orient in out_items:
			item_id = orient.get("item_id")
			if item_id not in id_map:
				continue
			item = id_map[item_id]
			cur.execute("""
				INSERT INTO items_orient (raw_item_id, brand, orient_json)
				VALUES (?, ?, ?)
			""", (item["id"], item.get("brand", ""), json.dumps(orient, ensure_ascii=False)))

			print("----")
			print(item.get("title"))
			print("->", orient.get("reputational_risk"), "| severity:", orient.get("severity"))

	conn.commit()
	conn.close()
	print("\nDone. ORIENT outputs saved in DB table: items_orient")


if __name__ == "__main__":
	main()
