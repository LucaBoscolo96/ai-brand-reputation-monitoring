import json
import os
import time
import urllib.parse
from datetime import datetime, timedelta, timezone
from typing import Dict, List
from concurrent.futures import ThreadPoolExecutor, as_completed

import feedparser
import requests
import yaml

from db import get_conn

UA = "Mozilla/5.0 (compatible; BrandMonitorBot/1.0; +https://example.com)"

def get_brand(cfg: Dict) -> str:
	return os.getenv("BRAND", cfg.get("project", {}).get("brand", "")).strip()


def _parse_entry_datetime(entry):
	"""
	Return datetime UTC if available, otherwise None.
	"""
	for key in ["published_parsed", "updated_parsed"]:
		st = entry.get(key)
		if st:
			return datetime.fromtimestamp(time.mktime(st), tz=timezone.utc)
	return None


def _fetch_feed(feed_url: str, timeout_sec: int = 6):
	headers = {"User-Agent": "AI-Brand-Reputation-Monitor/1.0 (+local demo)"}
	r = requests.get(feed_url, headers=headers, timeout=timeout_sec)
	r.raise_for_status()
	return feedparser.parse(r.content, agent=UA)


def collect_rss(cfg: Dict) -> int:
	db_path = cfg["storage"]["db_path"]
	days_back = int(cfg.get("observe", {}).get("days_back", 10))
	cutoff = datetime.now(timezone.utc) - timedelta(days=days_back)

	# Compat: supporta sia observe.rss.feeds (config attuale) sia observe.rss_feeds
	feeds: List[str] = []
	if "observe" in cfg:
		if isinstance(cfg["observe"], dict):
			feeds = (
				cfg["observe"].get("rss_feeds")
				or cfg["observe"].get("rss", {}).get("feeds", [])
			)
	feeds = feeds or []

	# sostituisci placeholder [BRAND] con il brand runtime
	brand_raw = get_brand(cfg)
	if brand_raw:
		brand_q = urllib.parse.quote_plus(brand_raw)
		feeds = [f.replace("[BRAND]", brand_q) for f in feeds]

	if not feeds:
		print("No RSS feeds configured (observe.rss.feeds).")
		return 0

	new_items = 0
	parsed_feeds: List[tuple[str, list]] = []

	# fetch feeds in parallel
	with ThreadPoolExecutor(max_workers=10) as ex:
		futures = {ex.submit(_fetch_feed, feed_url, 6): feed_url for feed_url in feeds}
		for fut in as_completed(futures):
			feed_url = futures[fut]
			try:
				parsed = fut.result()
				parsed_feeds.append((feed_url, parsed.entries))
			except Exception as e:
				print(f"RSS skip (error/timeout): {feed_url} | {type(e).__name__}: {e}")
				continue

	conn = get_conn(db_path)
	cur = conn.cursor()

	for feed_url, entries in parsed_feeds:
		for entry in entries:
			title = (entry.get("title") or "").strip()
			link = (entry.get("link") or "").strip()

			if not title or not link:
				# skip silently to reduce log noise
				continue

			source_item_id = (entry.get("id") or entry.get("guid") or link).strip()

			# --- Keyword filtering (mandatory) ---
			title_txt = entry.get("title", "")
			snippet_txt = entry.get("summary", "")
			text = (title_txt + " " + snippet_txt).lower()

			brand = get_brand(cfg).lower()
			brand_terms = [brand] + cfg["observe"]["keywords"]["brand_terms"]

			if brand and brand not in text:
				# skip silently to reduce log noise
				continue

			brand_matches = sum(1 for t in brand_terms if t.lower() in text)

			# Rule: must contain the brand
			if brand_matches < 1:
				# skip silently
				continue

			# Data pubblicazione (mandatory)
			published_dt = _parse_entry_datetime(entry)
			if published_dt is None:
				# skip silently
				continue  # skip if no published date

			if published_dt < cutoff:
				# skip silently
				continue  # older than cutoff

			metadata = {
				"feed_url": feed_url,
				"published": entry.get("published"),
				"updated": entry.get("updated"),
				"tags": [t.get("term") for t in entry.get("tags", []) if isinstance(t, dict)],
			}

			snippet = (entry.get("summary") or entry.get("description") or "").strip()
			if snippet:
				snippet = " ".join(snippet.split())

			# dedup
			cur.execute(
				"SELECT 1 FROM items_raw WHERE source=? AND source_item_id=? LIMIT 1",
				("rss", source_item_id),
			)
			if cur.fetchone():
				# skip silently duplicates
				continue

			cur.execute(
				"""
				INSERT INTO items_raw (
					source, source_item_id, brand,
					title, url, content,
					metadata_json,
					published_at
				)
				VALUES (?, ?, ?, ?, ?, ?, ?, ?)
				""",
				(
					"rss",
					source_item_id,
					brand_raw or brand,
					title,
					link,
					snippet,
					json.dumps(metadata, ensure_ascii=False),
					published_dt.strftime("%Y-%m-%d %H:%M:%S"),
				),
			)

			print(f"RSS OK: {title} | {feed_url}")
			new_items += 1

	conn.commit()
	conn.close()
	return new_items


def main():
	with open("config.yaml", "r", encoding="utf-8") as f:
		cfg = yaml.safe_load(f)

	n = collect_rss(cfg)
	print(f"RSS collected: {n} new items")


if __name__ == "__main__":
	main()
