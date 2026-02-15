import csv
import json
from datetime import datetime
import os

import yaml
from db import get_conn


def main():
	with open("config.yaml", "r", encoding="utf-8") as f:
		cfg = yaml.safe_load(f)

	db_path = cfg["storage"]["db_path"]
	conn = get_conn(db_path)
	cur = conn.cursor()

	cur.execute("""
		SELECT id, source, source_item_id, title, url, published_at, content, metadata_json, created_at
		FROM items_raw
		ORDER BY id DESC
		LIMIT 200
	""")
	rows = [dict(r) for r in cur.fetchall()]
	conn.close()

	ts = datetime.now().strftime("%Y%m%d_%H%M%S")

	out_dir = os.getenv("RUN_DIR", "outputs")
	os.makedirs(out_dir, exist_ok=True)  # ensure destination folder exists

	out_json = os.path.join(out_dir, f"raw_{ts}.json")
	out_csv = os.path.join(out_dir, f"raw_{ts}.csv")

	with open(out_json, "w", encoding="utf-8") as f:
		json.dump(rows, f, ensure_ascii=False, indent=2)

	with open(out_csv, "w", encoding="utf-8", newline="") as f:
		w = csv.DictWriter(f, fieldnames=rows[0].keys() if rows else ["empty"])
		w.writeheader()
		for r in rows:
			w.writerow(r)

	print(f"Exported:\n- {out_json}\n- {out_csv}")


if __name__ == "__main__":
	main()
