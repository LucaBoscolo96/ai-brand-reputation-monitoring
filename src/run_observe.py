import subprocess
import sys


def run(cmd):
	print("\n>", " ".join(cmd))
	subprocess.check_call(cmd)


def main():
	run([sys.executable, "src/init_db.py"])
	run([sys.executable, "src/collect_rss.py"])
	run([sys.executable, "src/export_raw.py"])
	print("\nDone. Check outputs/ and data/ooda.db")


if __name__ == "__main__":
	main()
