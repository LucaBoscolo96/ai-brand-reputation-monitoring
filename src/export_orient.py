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

	# Fetch latest ORIENT analyses
	cur.execute("""
		SELECT id, raw_item_id, orient_json, created_at
		FROM items_orient
		ORDER BY id DESC
		LIMIT 200
	""")

	rows = []
	for r in cur.fetchall():
		obj = json.loads(r["orient_json"])
		rows.append({
			"orient_id": r["id"],
			"raw_item_id": r["raw_item_id"],
			"claim_summary": obj.get("claim_summary"),
			"narrative_category": obj.get("narrative_category"),
			"reputational_risk": obj.get("reputational_risk"),
			"severity": obj.get("severity"),
			"confidence": obj.get("confidence"),
			"verification_steps": " | ".join(obj.get("verification_steps", [])),
			"created_at": r["created_at"],
		})

	conn.close()

	if not rows:
		print("No ORIENT items found.")
		return

	# Output filenames
	ts = datetime.now().strftime("%Y%m%d_%H%M%S")
	out_dir = os.getenv("RUN_DIR", "outputs")
	os.makedirs(out_dir, exist_ok=True)
	out_json = os.path.join(out_dir, f"orient_{ts}.json")
	out_csv = os.path.join(out_dir, f"orient_{ts}.csv")

	# Save JSON
	with open(out_json, "w", encoding="utf-8") as f:
		json.dump(rows, f, ensure_ascii=False, indent=2)

	# Save CSV
	with open(out_csv, "w", encoding="utf-8", newline="") as f:
		w = csv.DictWriter(f, fieldnames=rows[0].keys())
		w.writeheader()
		for r in rows:
			w.writerow(r)

	print("Exported ORIENT results:")
	print("-", out_json)
	print("-", out_csv)


if __name__ == "__main__":
	main()
