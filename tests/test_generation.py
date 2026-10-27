"""Behavioral checks for event counts, pacing and incompatible settings."""
from datetime import datetime
import json
from pathlib import Path
import subprocess
import sys
import time
import unittest

ROOT = Path(__file__).resolve().parents[1]
GENERATOR = ROOT / "scripts/generate_logs.py"


class GeneratorTest(unittest.TestCase):
    def generate(self, *args):
        started = time.monotonic()
        result = subprocess.run([sys.executable, str(GENERATOR), *args],
                                capture_output=True, text=True, encoding="utf-8", timeout=10)
        elapsed = time.monotonic() - started
        self.assertEqual(result.returncode, 0, result.stderr)
        events = [json.loads(line) for line in result.stdout.splitlines()]
        self.assertEqual(len({event["event_id"] for event in events}), len(events))
        return events, elapsed

    def assert_spread(self, events, minimum_span):
        times = [datetime.fromisoformat(event["timestamp"].replace("Z", "+00:00"))
                 for event in events]
        self.assertGreaterEqual((times[-1] - times[0]).total_seconds(), minimum_span)

    def test_default_count_and_severities(self):
        events, _ = self.generate()
        self.assertEqual(len(events), 100)
        self.assertEqual({event["level"] for event in events}, {"INFO", "WARN", "ERROR"})

    def test_rate_and_seconds(self):
        events, elapsed = self.generate("--events-per-minute", "600", "--duration-seconds", "0.5")
        self.assertEqual(len(events), 5)
        self.assertGreaterEqual(elapsed, 0.48)
        self.assert_spread(events, 0.35)

    def test_count_and_minutes(self):
        events, elapsed = self.generate("--count", "6", "--duration-minutes", "0.01")
        self.assertEqual(len(events), 6)
        self.assertGreaterEqual(elapsed, 0.58)
        self.assert_spread(events, 0.45)

    def test_count_and_rate(self):
        events, elapsed = self.generate("--count", "3", "--events-per-minute", "600")
        self.assertEqual(len(events), 3)
        self.assertGreaterEqual(elapsed, 0.28)
        self.assert_spread(events, 0.17)

    def test_fractional_event_budget_rounds_down(self):
        events, elapsed = self.generate("--events-per-minute", "900", "--duration-seconds", "0.1")
        self.assertEqual(len(events), 1)
        self.assertGreaterEqual(elapsed, 0.08)

    def test_legacy_interval(self):
        events, elapsed = self.generate("--count", "3", "--interval-ms", "50")
        self.assertEqual(len(events), 3)
        self.assertGreaterEqual(elapsed, 0.13)
        self.assert_spread(events, 0.08)

    def test_invalid_settings_emit_no_events(self):
        cases = [
            ["--events-per-minute", "0", "--duration-seconds", "1"],
            ["--events-per-minute", "nan", "--duration-seconds", "1"],
            ["--events-per-minute", "Infinity", "--duration-seconds", "1"],
            ["--count", "-1"],
            ["--duration-minutes", "0", "--count", "1"],
            ["--events-per-minute", "60"],
            ["--duration-seconds", "1"],
            ["--count", "1", "--events-per-minute", "60", "--duration-seconds", "1"],
            ["--count", "0", "--duration-seconds", "1"],
            ["--events-per-minute", "1", "--duration-seconds", "0.1"],
            ["--count", "1", "--duration-seconds", "1", "--interval-ms", "1"],
        ]
        for args in cases:
            with self.subTest(args=args):
                result = subprocess.run([sys.executable, str(GENERATOR), *args],
                                        capture_output=True, text=True, timeout=5)
                self.assertEqual(result.returncode, 2)
                self.assertEqual(result.stdout, "")


if __name__ == "__main__":
    unittest.main()
