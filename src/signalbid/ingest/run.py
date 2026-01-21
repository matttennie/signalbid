import argparse
import hashlib
import json
from datetime import datetime
from pathlib import Path

import yaml

from signalbid.ingest.fetch import MultiSourceFetcher
from signalbid.score.engine import OieScorer

DATA_DIR = Path("data")
RAW_DIR = DATA_DIR / "raw"
PROC_DIR = DATA_DIR / "processed"
HISTORY_FILE = PROC_DIR / "opportunities.ndjson"


def stable_id(item: dict) -> str:
    key = "|".join(
        [
            item.get("source_id", ""),
            item.get("canonical_url", ""),
            item.get("title", ""),
            str(item.get("deadline", "")),
        ]
    )
    return hashlib.sha256(key.encode("utf-8")).hexdigest()


def load_existing_ids():
    ids = set()
    if HISTORY_FILE.exists():
        with open(HISTORY_FILE) as f:
            for line in f:
                try:
                    obj = json.loads(line)
                    ids.add(obj["id"])
                except Exception:
                    continue
    return ids


def append_history(items):
    PROC_DIR.mkdir(parents=True, exist_ok=True)
    with open(HISTORY_FILE, "a") as f:
        for it in items:
            f.write(json.dumps(it) + "\n")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--sources", required=True)
    args = parser.parse_args()

    RAW_DIR.mkdir(parents=True, exist_ok=True)

    with open(args.sources) as f:
        config = yaml.safe_load(f)

    existing_ids = load_existing_ids()
    scorer = OieScorer(prompt_path="src/signalbid/score/prompts/score_v1.txt")

    fetcher = MultiSourceFetcher(config)
    new_records = []
    failures = []

    for source in config.get("sources", []):
        try:
            items = fetcher.fetch_source(source)
            for item in items:
                item["source_id"] = source["id"]
                item["fetched_at"] = datetime.utcnow().isoformat() + "Z"
                item["id"] = stable_id(item)

                if item["id"] in existing_ids:
                    continue

                scored = scorer.process(item)
                new_records.append(scored)

        except Exception as e:
            failures.append(
                {
                    "source_id": source.get("id"),
                    "error": str(e),
                }
            )

    if new_records:
        append_history(new_records)

    if failures:
        print("WARN: source failures encountered")
        for f in failures:
            print(f)


if __name__ == "__main__":
    main()
