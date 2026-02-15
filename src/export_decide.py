import csv
import json
import os
from datetime import datetime

import yaml
from db import get_conn


def main():
	# Load config
	with open("config.yaml", "r", encoding="utf-8") as f:
		cfg = yaml.safe_load(f)

	db_path = cfg["storage"]["db_path"]

	conn = get_conn(db_path)
	cur = conn.cursor()

	# Join decide + raw (per avere titolo/url/snippet in export)
	cur.execute("""
		SELECT
			d.id AS decide_id,
			d.raw_item_id AS raw_item_id,
			d.orient_id AS orient_id,
			d.decide_json AS decide_json,
			d.created_at AS decided_at,
			r.title AS title,
			r.url AS url,
			r.content AS snippet
		FROM items_decide d
		LEFT JOIN items_raw r
			ON r.id = d.raw_item_id
		ORDER BY d.id DESC
		LIMIT 500
	""")

	rows = []
	for r in cur.fetchall():
		obj = json.loads(r["decide_json"])

		rows.append({
			"decide_id": r["decide_id"],
			"orient_id": r["orient_id"],
			"raw_item_id": r["raw_item_id"],
			"title": r["title"],
			"url": r["url"],
			"snippet": (r["snippet"] or "")[:400].replace("\n", " ").strip(),
			"intent_framing": obj.get("intent_framing"),
			"recommended_action": obj.get("recommended_action"),
			"urgency": obj.get("urgency"),
			"escalation_team": ", ".join(obj.get("escalation_team", [])) if isinstance(obj.get("escalation_team"), list) else "",
			"rationale": obj.get("rationale"),
			"no_regret_move": obj.get("no_regret_move"),
			"decided_at": r["decided_at"],
		})

	conn.close()

	if not rows:
		print("No DECIDE items found.")
		return

	ts = datetime.now().strftime("%Y%m%d_%H%M%S")
	out_dir = os.getenv("RUN_DIR", "outputs")
	os.makedirs(out_dir, exist_ok=True)
	out_json = os.path.join(out_dir, f"decide_{ts}.json")
	out_csv = os.path.join(out_dir, f"decide_{ts}.csv")

	# JSON
	with open(out_json, "w", encoding="utf-8") as f:
		json.dump(rows, f, ensure_ascii=False, indent=2)

	# CSV
	with open(out_csv, "w", encoding="utf-8", newline="") as f:
		w = csv.DictWriter(f, fieldnames=rows[0].keys())
		w.writeheader()
		for row in rows:
			w.writerow(row)

	print("Exported DECIDE results:")
	print("-", out_json)
	print("-", out_csv)


if __name__ == "__main__":
	main()
