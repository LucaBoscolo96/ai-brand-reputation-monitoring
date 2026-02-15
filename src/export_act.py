import json
import os
from datetime import datetime

import yaml
from db import get_conn


def main():
	with open("config.yaml", "r", encoding="utf-8") as f:
		cfg = yaml.safe_load(f)

	db_path = cfg["storage"]["db_path"]

	conn = get_conn(db_path)
	cur = conn.cursor()

	cur.execute("""
		SELECT name FROM sqlite_master
		WHERE type='table' AND name='runs_act'
	""")
	if not cur.fetchone():
		print("Table runs_act not found. Run: python src/ooda_act.py")
		conn.close()
		return

	cur.execute("""
		SELECT id, brand, act_json, created_at
		FROM runs_act
		ORDER BY id DESC
		LIMIT 1
	""")
	row = cur.fetchone()
	conn.close()

	if not row:
		print("No ACT runs found. Run: python src/ooda_act.py")
		return

	obj = json.loads(row["act_json"])
	ts = datetime.now().strftime("%Y%m%d_%H%M%S")
	out_dir = os.getenv("RUN_DIR", "outputs")
	os.makedirs(out_dir, exist_ok=True)
	out = os.path.join(out_dir, f"act_latest_{ts}.json")
	with open(out, "w", encoding="utf-8") as f:
		json.dump(obj, f, ensure_ascii=False, indent=2)

	print("Exported latest ACT run:")
	print("-", out)


if __name__ == "__main__":
	main()
