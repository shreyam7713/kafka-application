#!/usr/bin/env python3
"""Build Kafka 4.1.2 using a trusted, cached Java 17+ Linux amd64 image."""
import argparse
import json
from pathlib import Path
import shutil
import subprocess

root = Path(__file__).resolve().parents[1]
parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("--base-image", required=True)
args = parser.parse_args()
cfg = json.loads((root / "config/runtime.local.json").read_text())
context = Path(cfg["runtime"]) / "docker-build"
context.mkdir(parents=True, exist_ok=True)
shutil.copytree(cfg["kafka_home"], context / "kafka", dirs_exist_ok=True,
                ignore=shutil.ignore_patterns("logs"))
shutil.copy2(root / "scripts/broker-entrypoint.sh", context / "broker-entrypoint.sh")
subprocess.run(["docker", "build", "--platform", "linux/amd64", "--pull=false",
                "--build-arg", "BASE_IMAGE=" + args.base_image,
                "-f", str(root / "Dockerfile"), "-t", "shreyam-hackathon-kafka:4.1.2",
                str(context)], check=True)
