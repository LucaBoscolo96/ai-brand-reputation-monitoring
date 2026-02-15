import argparse
import sys
import os
import glob
from datetime import datetime
from pathlib import Path

try:
	sys.stdout.reconfigure(encoding="utf-8")
	sys.stderr.reconfigure(encoding="utf-8")
except Exception:
	pass

# Importa i "main" dei tuoi script
from init_db import main as init_db_main
from collect_rss import main as collect_rss_main
from export_raw import main as export_raw_main
from ooda_orient import main as ooda_orient_main
from ooda_decide import main as ooda_decide_main
from export_decide import main as export_decide_main
from ooda_act import main as ooda_act_main
from export_orient import main as export_orient_main
# from export_act import main as export_act_main  # optional


def main():
	run_ts = datetime.now().strftime("%Y%m%d_%H%M%S")
	run_dir = os.path.join("runs", run_ts)
	os.makedirs(run_dir, exist_ok=True)
	os.environ["RUN_DIR"] = run_dir
	print("Run folder:", run_dir)

	# brand dinamico
	brand = os.getenv("BRAND", "").strip()
	if not brand:
		brand = Path("config.yaml").read_text(encoding="utf-8")
		# semplice fallback: non parsare yaml qui per restare light; sarà usato nei singoli script
		brand = ""
	print("BRAND_USED:", os.getenv("BRAND", "(env not set)"))

	parser = argparse.ArgumentParser(
		description="AI Brand Reputation Monitoring - Orchestrator (local)"
	)
	parser.add_argument(
		"--skip-init",
		action="store_true",
		help="Skip DB initialization",
	)
	parser.add_argument(
		"--skip-collect",
		action="store_true",
		help="Skip RSS collection",
	)
	parser.add_argument(
		"--skip-export-raw",
		action="store_true",
		help="Skip raw export",
	)
	parser.add_argument(
		"--skip-orient",
		action="store_true",
		help="Skip ORIENT (OpenAI)",
	)

	args = parser.parse_args()

	print("\n==============================")
	print(" AI Brand Reputation Monitoring")
	print(" Orchestrator (local)")
	print("==============================\n")

	if not args.skip_init:
		print("--- STEP 1/6: INIT DB ---")
		init_db_main()
	else:
		print("--- STEP 1/6: INIT DB (skipped) ---")

	if not args.skip_collect:
		print("\n--- STEP 2/6: COLLECT RSS ---")
		n = collect_rss_main()
		if n == 0:
			print("\n⚠️ No new RSS items collected.")
			print("Pipeline stopped (nothing to analyze).")
			return
	else:
		print("\n--- STEP 2/6: COLLECT RSS (skipped) ---")

	if not args.skip_export_raw:
		print("\n--- STEP 3/6: EXPORT RAW ---")
		export_raw_main()
	else:
		print("\n--- STEP 3/6: EXPORT RAW (skipped) ---")

	if not args.skip_orient:
		print("\n--- STEP 4/6: ORIENT (AI) ---")
		ooda_orient_main()

		print("\n--- EXTRA: EXPORT ORIENT ---")
		export_orient_main()
	else:
		print("\n--- STEP 4/6: ORIENT (skipped) ---")

	print("\n--- STEP 5/6: DECIDE (AI) ---")
	ooda_decide_main()

	print("\n--- EXTRA: EXPORT DECIDE ---")
	export_decide_main()

	print("\n--- STEP 6/6: ACT (AGGREGATED) ---")
	ooda_act_main()

	# print("\n--- EXTRA: EXPORT ACT (latest) ---")
	# export_act_main()

	# Save path of latest XLSX report inside RUN_DIR
	run_dir = os.environ.get("RUN_DIR") or str(Path("runs") / "UNKNOWN_RUN")
	xlsx_files = glob.glob(os.path.join(run_dir, "*.xlsx"))
	report_path = max(xlsx_files, key=os.path.getmtime) if xlsx_files else ""

	with open(os.path.join(run_dir, "last_report_path.txt"), "w", encoding="utf-8") as f:
		f.write(report_path)

	print("LAST_REPORT_PATH:", report_path)

	print("\n✅ Pipeline completed.")
	print("Open this folder:", os.environ["RUN_DIR"])


if __name__ == "__main__":
	main()
