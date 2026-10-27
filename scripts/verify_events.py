#!/usr/bin/env python3
"""Compare a generated JSONL fixture with FileStream JSONL output by event ID."""
import argparse
from collections import Counter
import json
from pathlib import Path


def reconcile(expected_path, actual_path):
    expected = [json.loads(line) for line in Path(expected_path).read_text().splitlines() if line]
    wanted = {event["event_id"]: event for event in expected}
    if len(wanted) != len(expected):
        raise ValueError("The generated fixture contains duplicate event IDs")
    runs = {event["demo_run_id"] for event in expected}
    actual = []
    malformed = []
    if Path(actual_path).exists():
        for number, line in enumerate(Path(actual_path).read_text().splitlines(), 1):
            if not line:
                continue
            try:
                event = json.loads(line)
                if event.get("demo_run_id") in runs:
                    actual.append(event)
            except (ValueError, AttributeError):
                malformed.append(number)
    counts = Counter(event.get("event_id") for event in actual)
    missing = sorted(set(wanted) - set(counts))
    unexpected = sorted(str(key) for key in set(counts) - set(wanted))
    mismatched = sorted({event.get("event_id") for event in actual
                         if event.get("event_id") in wanted and event != wanted[event["event_id"]]})
    return {"run_ids": sorted(runs), "produced": len(expected), "received_total": len(actual),
            "received_unique": len(counts), "missing_ids": missing,
            "duplicate_records": sum(count - 1 for count in counts.values()),
            "unexpected_ids": unexpected, "changed_event_ids": mismatched,
            "malformed_output_lines": malformed,
            "passed": not (missing or unexpected or mismatched or malformed)}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("expected")
    parser.add_argument("actual")
    args = parser.parse_args()
    result = reconcile(args.expected, args.actual)
    print(json.dumps(result, indent=2, ensure_ascii=False))
    raise SystemExit(0 if result["passed"] else 1)
