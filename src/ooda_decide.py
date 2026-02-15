import json
import os
from typing import Dict, List, Optional

import yaml
from dotenv import load_dotenv
from openai import OpenAI
from openai import APIError, AuthenticationError, RateLimitError

from db import get_conn

# Modello "cheap demo"
MODEL = "gpt-5-mini"


INTENT_ENUM = ["THREAT", "DEFENSE", "NEUTRAL", "OPPORTUNITY", "NOISE"]


def get_brand(cfg: Dict) -> str:
	return os.getenv("BRAND", cfg.get("project", {}).get("brand", "")).strip()


def ensure_decide_table(conn) -> None:
	cur = conn.cursor()
	cur.execute("""
	CREATE TABLE IF NOT EXISTS items_decide (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		raw_item_id INTEGER,
		orient_id INTEGER,
		brand TEXT NOT NULL,
		decide_json TEXT,
		created_at TEXT NOT NULL DEFAULT (datetime('now'))
	)
	""")
	conn.commit()


def fetch_recent_orient_with_raw(db_path: str, brand: str, limit: int = 10) -> List[Dict]:
	"""
	Prende gli ultimi orient (max 30) entro 7 giorni, arricchiti con title/url/snippet da items_raw.
	"""
	conn = get_conn(db_path)
	cur = conn.cursor()

	cur.execute("""
		SELECT
			o.id AS orient_id,
			o.raw_item_id AS raw_item_id,
			o.orient_json AS orient_json,
			o.created_at AS orient_created_at,
			r.title AS title,
			r.url AS url,
			r.content AS content,
			r.metadata_json AS raw_metadata_json
		FROM items_orient o
		LEFT JOIN items_raw r
			ON r.id = o.raw_item_id
		WHERE r.published_at >= datetime('now','-7 days')
		AND o.brand = ?
		ORDER BY COALESCE(CAST(json_extract(o.orient_json, '$.severity') AS REAL), 0) DESC, o.id DESC
		LIMIT ?
	""", (brand, limit))

	rows = [dict(r) for r in cur.fetchall()]
	conn.close()
	return rows


def already_decided(conn, orient_id: int) -> bool:
	cur = conn.cursor()
	cur.execute("SELECT 1 FROM items_decide WHERE orient_id=? LIMIT 1", (orient_id,))
	return cur.fetchone() is not None


def api_smoke_test(client: OpenAI, brand: str) -> None:
	resp = client.chat.completions.create(
		model=MODEL,
		messages=[{"role": "user", "content": f'Reply with a JSON object {{"ok": true, "brand": "{brand}"}}'}],
		response_format={"type": "json_object"},
		timeout=30,
	)
	print("API smoke test OK for brand:", brand)


def build_decide_prompt(brand: str, raw: Dict, orient: Dict) -> str:
	"""
	DECIDE: deve capire l'INTENTO dell'articolo:
	- THREAT: brand accusato / colpevole / scandalo
	- DEFENSE: enforcement/azioni contro il fake, brand vittima o parte della soluzione
	- OPPORTUNITY: news positiva (acquisizione, premio, partnership)
	- NEUTRAL: citazione informativa
	- NOISE: gossip/irrilevante

	E poi dare azione coerente (no escalation inutile).
	"""
	title = (raw.get("title") or "").strip()
	snippet = (raw.get("content") or "").strip()
	url = (raw.get("url") or "").strip()

	claim_summary = orient.get("claim_summary", "")
	narr_cat = orient.get("narrative_category", "")
	sev = orient.get("severity", 0)
	rep_risk = orient.get("reputational_risk", "")

	return f"""
You are an AI Early Warning Advisor for brand reputation.
You MUST follow the OODA Loop framework.

We are now in the DECIDE phase.

Brand: {brand}

Inputs:
- ORIENT claim_summary: {claim_summary}
- ORIENT narrative_category: {narr_cat}
- ORIENT reputational_risk: {rep_risk}
- ORIENT severity: {sev}

News item:
- Title: {title}
- Snippet: {snippet}
- URL: {url}

Critical instruction:
Do NOT assume that negative keywords automatically mean a crisis for the brand.
You must interpret the INTENT / FRAMING of the article.

Classify the framing into ONE of:
- THREAT (brand accused or harmed; scandal, wrongdoing, boycott, investigation targeting brand)
- DEFENSE (enforcement already happening; seizures/crackdowns against fakes; brand is victim or acting)
- OPPORTUNITY (positive strategic news: acquisition, partnership, award, growth)
- NEUTRAL (informational mention; no clear threat or opportunity)
- NOISE (gossip/low relevance; no business impact)

Then decide the correct action:
- THREAT: escalate to crisis response; propose immediate steps
- DEFENSE: monitor + consider reputation reinforcement messaging ("brand fighting counterfeits")
- OPPORTUNITY: suggest reputational leverage / comms angle
- NEUTRAL: log & monitor only
- NOISE: ignore / deprioritize

Return ONLY valid JSON with these fields:
{{
  "intent_framing": "THREAT|DEFENSE|OPPORTUNITY|NEUTRAL|NOISE",
  "recommended_action": "one sentence",
  "urgency": "low|medium|high",
  "escalation_team": ["PR","Legal","Security","Exec"]  // can be empty
  "rationale": "2-3 sentences explaining WHY this framing and action",
  "no_regret_move": "one concrete step that is safe in most cases"
}}

Make sure intent_framing matches exactly one of the allowed values.
""".strip()


def decide_one(client: OpenAI, brand: str, raw: Dict, orient: Dict) -> Dict:
	prompt = build_decide_prompt(brand, raw, orient)

	resp = client.chat.completions.create(
		model=MODEL,
		messages=[{"role": "user", "content": prompt}],
		response_format={"type": "json_object"},
		timeout=45,
	)

	obj = json.loads(resp.choices[0].message.content)

	# Hard validation / normalization (evita output fuori enum)
	intent = str(obj.get("intent_framing", "")).strip().upper()
	if intent not in INTENT_ENUM:
		# fallback soft: se non valido, metti NEUTRAL
		intent = "NEUTRAL"
	obj["intent_framing"] = intent

	urg = str(obj.get("urgency", "")).strip().lower()
	if urg not in ["low", "medium", "high"]:
		obj["urgency"] = "low"

	team = obj.get("escalation_team", [])
	if not isinstance(team, list):
		obj["escalation_team"] = []

	return obj


def main():
	load_dotenv()
	api_key = os.getenv("OPENAI_API_KEY", "").strip()
	if not api_key:
		raise RuntimeError("Missing OPENAI_API_KEY in .env")

	client = OpenAI(api_key=api_key)

	with open("config.yaml", "r", encoding="utf-8") as f:
		cfg = yaml.safe_load(f)

	brand = get_brand(cfg)
	db_path = cfg["storage"]["db_path"]

	# Smoke test (così capisci subito se rete/key ok)
	try:
		api_smoke_test(client, brand)
	except AuthenticationError:
		print("AUTH ERROR: OPENAI_API_KEY invalid/missing permissions.")
		raise
	except RateLimitError:
		print("RATE LIMIT / QUOTA: check billing/credits.")
		raise
	except APIError as e:
		print("OPENAI API ERROR:", str(e))
		raise

	records = fetch_recent_orient_with_raw(db_path, brand, limit=30)
	if not records:
		print("No ORIENT records found. Run ORIENT first.")
		return

	conn = get_conn(db_path)
	ensure_decide_table(conn)
	cur = conn.cursor()

	done = 0
	skipped = 0

	print(f"\nRunning DECIDE for brand: {brand}")
	print(f"Scanning last {len(records)} ORIENT items...\n")

	# Pre-filtra ciò che è già deciso per evitare chiamate inutili
	to_process = []
	for rec in records:
		orient_id = rec.get("orient_id")
		raw_item_id = rec.get("raw_item_id")
		if orient_id is None or raw_item_id is None:
			continue
		if already_decided(conn, int(orient_id)):
			skipped += 1
			continue
		try:
			orient = json.loads(rec.get("orient_json") or "{}")
		except Exception:
			orient = {}
		raw = {
			"title": rec.get("title"),
			"url": rec.get("url"),
			"content": rec.get("content"),
		}
		to_process.append((orient_id, raw_item_id, raw, orient))

	# Decidi in parallelo (fino a ~30 worker)
	results = []
	max_workers = max(1, min(30, len(to_process)))
	from concurrent.futures import ThreadPoolExecutor, as_completed
	with ThreadPoolExecutor(max_workers=max_workers) as ex:
		futures = {
			ex.submit(decide_one, client, brand, raw, orient): (orient_id, raw_item_id, raw)
			for (orient_id, raw_item_id, raw, orient) in to_process
		}
		for fut in as_completed(futures):
			orient_id, raw_item_id, raw = futures[fut]
			try:
				decide = fut.result()
				results.append((orient_id, raw_item_id, raw, decide))
			except Exception as e:
				print("FAILED DECIDE on:", raw.get("title"))
				print("ERROR:", repr(e))

	# Inserimento sequenziale (evita write race sul DB)
	for orient_id, raw_item_id, raw, decide in results:
		cur.execute("""
			INSERT INTO items_decide (raw_item_id, orient_id, brand, decide_json)
			VALUES (?, ?, ?, ?)
		""", (int(raw_item_id), int(orient_id), brand, json.dumps(decide, ensure_ascii=False)))

		done += 1

		print("----")
		print(raw.get("title"))
		print("intent:", decide.get("intent_framing"), "| urgency:", decide.get("urgency"))
		print("action:", decide.get("recommended_action"))
		print("team:", decide.get("escalation_team"))
		print()

	conn.commit()
	conn.close()

	print(f"Done. DECIDE saved to DB table: items_decide")
	print(f"New decisions: {done} | skipped (already decided): {skipped}")


if __name__ == "__main__":
	main()
