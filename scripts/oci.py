#!/usr/bin/env python3
"""Build and manage the OCI path alongside the existing FileStream sink."""
import argparse
from configparser import ConfigParser
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import time
from urllib.parse import urlsplit
import urllib.error
import uuid
import xml.etree.ElementTree as ET
from zipfile import ZipFile

import lab
from generate_logs import add_timing_arguments, generation_plan
from verify_events import reconcile

OCI_NAME = "oci-la-cloud"
OCI_CLASS = "com.oracle.observability.kafka.connect.OracleObservabilitySinkConnector"
ROOT = lab.ROOT


def write_report(name, data):
    (ROOT / "evidence").mkdir(exist_ok=True)
    (ROOT / "evidence" / name).write_text(json.dumps(data, indent=2) + "\n")


def local_config_path():
    return ROOT / "config/oci-sink.local.json"


def init_config():
    destination = local_config_path()
    if not destination.exists():
        shutil.copy2(ROOT / "config/oci-sink.json", destination)
        destination.chmod(0o600)
    return destination


def configuration_errors(payload, check_auth=True):
    errors = []
    config = payload.get("config", {})
    if payload.get("name") != OCI_NAME:
        errors.append(f"name must be {OCI_NAME}; the existing FileStream connector is reserved")
    if config.get("connector.class") != OCI_CLASS:
        errors.append("connector.class must identify the upstream OCI plugin")
    if config.get("topics") != lab.TOPIC or str(config.get("tasks.max")) != "1":
        errors.append(f"use topic {lab.TOPIC} and tasks.max=1 for this lab")
    if config.get("consumer.override.group.id", "connect-" + OCI_NAME) != "connect-" + OCI_NAME:
        errors.append("the OCI path must use its own consumer group connect-" + OCI_NAME)
    for name in ["key.converter", "value.converter"]:
        if config.get(name) != "org.apache.kafka.connect.storage.StringConverter":
            errors.append(f"{name} must be StringConverter")
    required = ["oci.region", "oci.namespace", "oci.config.file", "oci.config.profile",
                "oci.la.log.group.id", "oci.la.log.source.name"]
    for name in required:
        value = str(config.get(name, "")).strip()
        if not value or "REPLACE" in value.upper():
            errors.append(f"provide a verified value for {name}")
    if config.get("oci.auth.type") != "config_file":
        errors.append("oci.auth.type must be config_file")
    if check_auth and not errors:
        auth = Path(config["oci.config.file"]).expanduser()
        if not auth.is_absolute() or not auth.is_file():
            errors.append("oci.config.file must identify an existing absolute profile-file path")
        else:
            profiles = ConfigParser(interpolation=None)
            profiles.read(auth)
            profile = config["oci.config.profile"]
            if profile != "DEFAULT" and not profiles.has_section(profile):
                errors.append("the requested OCI profile is absent from the profile file")
            else:
                for key in ["tenancy", "user", "fingerprint", "key_file"]:
                    if not profiles[profile].get(key):
                        errors.append(f"the OCI profile requires {key}")
                key_file = profiles[profile].get("key_file", "")
                if key_file and not Path(key_file).expanduser().is_file():
                    errors.append("the OCI profile's key_file does not exist")
    return errors


def load_config():
    path = init_config()
    payload = json.loads(path.read_text())
    auth_path = payload.get("config", {}).get("oci.config.file")
    if isinstance(auth_path, str) and auth_path.strip():
        expanded = Path(auth_path).expanduser()
        if expanded.is_absolute():
            payload["config"]["oci.config.file"] = str(expanded.resolve())
    errors = configuration_errors(payload)
    if errors:
        raise RuntimeError("OCI configuration is not ready: " + "; ".join(errors))
    return payload


def source_directory():
    pin = json.loads((ROOT / "config/oci-upstream.json").read_text())
    source = ROOT / pin["directory"]
    if not source.exists():
        source.parent.mkdir(exist_ok=True)
        subprocess.run(["git", "clone", "--depth", "1", "--branch", pin["branch"],
                        pin["repository"], str(source)], check=True)
    revision = subprocess.check_output(["git", "-C", str(source), "rev-parse", "HEAD"], text=True).strip()
    dirty = subprocess.check_output(["git", "-C", str(source), "status", "--porcelain"], text=True)
    if revision != pin["revision"] or dirty:
        raise RuntimeError("Upstream checkout must be clean and match config/oci-upstream.json; local changes were retained")
    return source, pin


def maven_settings(runtime):
    proxy = urlsplit(os.environ.get("https_proxy", os.environ.get("HTTPS_PROXY", "")))
    root = ET.Element("settings", xmlns="http://maven.apache.org/SETTINGS/1.2.0")
    if proxy.hostname:
        node = ET.SubElement(ET.SubElement(root, "proxies"), "proxy")
        for key, value in [("id", "lab-proxy"), ("active", "true"), ("protocol", proxy.scheme),
                           ("host", proxy.hostname), ("port", str(proxy.port or 80)),
                           ("nonProxyHosts", "localhost|127.0.0.1")]:
            ET.SubElement(node, key).text = value
    path = runtime / "maven-settings.xml"
    ET.ElementTree(root).write(path, encoding="utf-8", xml_declaration=True)
    path.chmod(0o600)
    return path


def build_plugin():
    source, pin = source_directory()
    cfg = lab.settings()
    runtime = Path(cfg["runtime"])
    cache = Path(cfg.get("maven_repository", str(runtime / "maven-repository")))
    subprocess.run(["mvn", "-B", "-ntp", "-s", str(maven_settings(runtime)),
                    "-Dmaven.repo.local=" + str(cache), "-f", str(source / "pom.xml"),
                    "clean", "verify"], env=lab.env(), check=True)
    archives = list((source / "target").glob("*-plugin.zip"))
    if len(archives) != 1:
        raise RuntimeError("Expected one upstream plugin ZIP after verification")
    totals = dict(tests=0, failures=0, errors=0, skipped=0)
    for pattern in ["surefire-reports/TEST-*.xml", "failsafe-reports/TEST-*.xml"]:
        for report in (source / "target").glob(pattern):
            suite = ET.parse(report).getroot()
            for key in totals:
                totals[key] += int(suite.attrib.get(key, 0))
    archive = archives[0]
    manifest = {"upstream_repository": pin["repository"], "upstream_revision": pin["revision"],
                "archive": str(archive), "sha256": hashlib.sha256(archive.read_bytes()).hexdigest(),
                "verification": totals, "built_utc": datetime.now(timezone.utc).isoformat()}
    write_report("oci-build.json", manifest)
    init_config()
    return manifest


def prepare_distribution(archive, directory):
    with ZipFile(archive) as zip_file:
        for entry in zip_file.infolist():
            relative = Path(entry.filename)
            if relative.is_absolute() or ".." in relative.parts:
                raise RuntimeError("Plugin archive contains an unsafe path")
            if (entry.external_attr >> 16) & 0o170000 == 0o120000:
                raise RuntimeError("Plugin archive contains a symbolic link")
        jars = [entry.filename for entry in zip_file.infolist() if entry.filename.endswith(".jar")]
        if len(jars) < 3 or any(Path(name).name.startswith(("connect-api-", "kafka-clients-", "slf4j-api-")) for name in jars):
            raise RuntimeError("Plugin runtime dependencies do not match the worker isolation contract")
        zip_file.extractall(directory)
    roots = [path for path in Path(directory).iterdir() if path.is_dir() and (path / "lib").is_dir()]
    if len(roots) != 1:
        raise RuntimeError("Expected one plugin distribution containing lib/")
    return roots[0], len(jars)


def installed_classes():
    return {plugin["class"] for plugin in lab.api("/connector-plugins")}


def validate_schema(config):
    body = dict(config, name=OCI_NAME)
    response = lab.api(f"/connector-plugins/{OCI_CLASS}/config/validate", "PUT", body)
    if response["error_count"]:
        messages = [message for item in response.get("configs", [])
                    for message in item.get("value", {}).get("errors", [])]
        raise RuntimeError("Connect rejected the OCI configuration: " + "; ".join(messages))
    return response


def install_plugin(skip_build=False):
    manifest = json.loads((ROOT / "evidence/oci-build.json").read_text()) if skip_build else build_plugin()
    archive = Path(manifest["archive"])
    pin = json.loads((ROOT / "config/oci-upstream.json").read_text())
    if manifest["upstream_revision"] != pin["revision"] or hashlib.sha256(archive.read_bytes()).hexdigest() != manifest["sha256"]:
        raise RuntimeError("Build manifest or archive does not match the pinned source")
    runtime = Path(lab.settings()["runtime"])
    destination = runtime / "plugins/oracle-observability"
    backup = runtime / "plugin-backups" / ("oracle-observability-" + uuid.uuid4().hex[:12])
    with tempfile.TemporaryDirectory(dir=runtime) as temporary:
        distribution, jar_count = prepare_distribution(archive, temporary)
        worker_running = False
        try:
            lab.api("/")
            worker_running = True
        except OSError:
            pass
        if worker_running:
            lab.stop_connect()
        changed = False
        try:
            if destination.exists():
                backup.parent.mkdir(exist_ok=True)
                destination.rename(backup)
            shutil.move(str(distribution), destination)
            changed = True
            lab.start_connect()
            if OCI_CLASS not in installed_classes():
                raise RuntimeError("OCI plugin was not discovered after restarting Connect")
            validate_schema(json.loads((ROOT / "config/oci-sink.json").read_text())["config"])
        except BaseException:
            if changed:
                try:
                    lab.stop_connect()
                except (OSError, RuntimeError):
                    pass
                shutil.rmtree(destination)
            if backup.exists():
                backup.rename(destination)
            if worker_running:
                lab.start_connect()
            raise
    manifest.update(installed_directory=str(destination), runtime_jars=jar_count,
                    plugin_discovered=True, schema_validated=True)
    write_report("oci-installation.json", manifest)
    init_config()
    return manifest


def connector_status(name):
    try:
        return lab.api(f"/connectors/{name}/status")
    except urllib.error.HTTPError as error:
        if error.code == 404:
            error.close()
            return {"name": name, "state": "NOT_REGISTERED"}
        raise


def wait_connector(name):
    def ready(status):
        states = [status.get("connector", {}).get("state")] + [task["state"] for task in status.get("tasks", [])]
        if "FAILED" in states:
            raise RuntimeError(f"{name} failed; inspect Connect logs for the request or authentication error")
        return status["connector"]["state"] == "RUNNING" and bool(status["tasks"]) and all(
            task["state"] == "RUNNING" for task in status["tasks"])
    return lab.wait_api(f"/connectors/{name}/status", ready)


def topic_offsets(earliest=False):
    result = lab.kafka("kafka-get-offsets", "--bootstrap-server", lab.BROKER, "--topic", lab.TOPIC,
                       "--time", "-2" if earliest else "-1", capture_output=True, text=True, timeout=30)
    offsets = {}
    for line in result.stdout.splitlines():
        topic, partition, offset = line.rsplit(":", 2)
        if topic == lab.TOPIC:
            offsets[int(partition)] = int(offset)
    if not offsets:
        raise RuntimeError("The lab Kafka topic has no readable partitions")
    return offsets


def enable_oci(start_at="latest"):
    payload = load_config()  # Refuse placeholders before contacting Connect or starting an SDK task.
    if OCI_CLASS not in installed_classes():
        raise RuntimeError("Install the OCI plugin first with scripts/oci.py install")
    validate_schema(payload["config"])
    try:
        existing = lab.api(f"/connectors/{OCI_NAME}/config")
    except urllib.error.HTTPError as error:
        if error.code != 404:
            raise
        error.close()
        offsets = topic_offsets(earliest=start_at == "earliest")
        lab.api("/connectors", "POST", dict(payload, initial_state="STOPPED"))
        lab.wait_api(f"/connectors/{OCI_NAME}/status", lambda value:
                     value["connector"]["state"] == "STOPPED" and not value["tasks"])
        lab.api(f"/connectors/{OCI_NAME}/offsets", "PATCH", {"offsets": [
            {"partition": {"kafka_topic": lab.TOPIC, "kafka_partition": partition},
             "offset": {"kafka_offset": offset}} for partition, offset in offsets.items()]})
    else:
        if existing.get("connector.class") != OCI_CLASS:
            raise RuntimeError("OCI connector name is occupied by another connector class")
        status = connector_status(OCI_NAME)
        changed = any(existing.get(key) != value for key, value in payload["config"].items())
        failed = status.get("connector", {}).get("state") == "FAILED" or any(
            task["state"] == "FAILED" for task in status.get("tasks", []))
        if changed or failed:
            lab.api(f"/connectors/{OCI_NAME}/stop", "PUT")
            lab.wait_api(f"/connectors/{OCI_NAME}/status", lambda value:
                         value["connector"]["state"] == "STOPPED" and not value["tasks"])
        if changed:
            lab.api(f"/connectors/{OCI_NAME}/config", "PUT", payload["config"])
        # Recover a creation interrupted before its initial offset PATCH. A group
        # with committed offsets is always retained, including while stopped.
        status = connector_status(OCI_NAME)
        if status.get("connector", {}).get("state") == "STOPPED":
            committed = lab.api(f"/connectors/{OCI_NAME}/offsets")["offsets"]
            if not committed:
                offsets = topic_offsets(earliest=start_at == "earliest")
                lab.api(f"/connectors/{OCI_NAME}/offsets", "PATCH", {"offsets": [
                    {"partition": {"kafka_topic": lab.TOPIC, "kafka_partition": partition},
                     "offset": {"kafka_offset": offset}} for partition, offset in offsets.items()]})
    lab.api(f"/connectors/{OCI_NAME}/resume", "PUT")
    return wait_connector(OCI_NAME)


def disable_oci():
    if connector_status(OCI_NAME).get("state") == "NOT_REGISTERED":
        return {"name": OCI_NAME, "state": "NOT_REGISTERED"}
    lab.api(f"/connectors/{OCI_NAME}/stop", "PUT")
    return lab.wait_api(f"/connectors/{OCI_NAME}/status", lambda value:
                        value["connector"]["state"] == "STOPPED" and not value["tasks"])


def wait_acceptance(targets, seconds=120):
    deadline = time.monotonic() + seconds
    while True:
        wait_connector(OCI_NAME)
        response = lab.api(f"/connectors/{OCI_NAME}/offsets")
        committed = {item["partition"]["kafka_partition"]: item["offset"]["kafka_offset"]
                     for item in response["offsets"] if item["partition"]["kafka_topic"] == lab.TOPIC}
        if all(committed.get(partition, -1) >= offset for partition, offset in targets.items()):
            return {"api_acceptance_verified": True, "target_offsets": targets, "committed_offsets": committed,
                    "log_analytics_indexing_verified": False,
                    "scope": "Committed offsets indicate upstream API acceptance; verify event IDs and parsed fields in Log Explorer"}
        if time.monotonic() > deadline:
            raise RuntimeError("OCI did not commit the generated Kafka range within 120 seconds; inspect Connect logs")
        time.sleep(1)


def test_oci(args):
    status = connector_status(OCI_NAME)
    if status.get("state") == "NOT_REGISTERED":
        raise RuntimeError("OCI path is not enabled; fill config/oci-sink.local.json and run scripts/oci.py enable")
    wait_connector(OCI_NAME)
    if args.verify_file:
        lab.wait_running()
    args.plan = generation_plan(args)
    if args.plan.count < 1:
        raise RuntimeError("A test run must produce at least one event")
    run_id = args.run_id or "oci-" + uuid.uuid4().hex[:12]
    if not run_id.replace("-", "").replace("_", "").isalnum():
        raise RuntimeError("Use letters, numbers, hyphens or underscores in the run ID")
    fixture = Path(lab.settings()["runtime"]) / "runs" / f"{run_id}.jsonl"
    if fixture.exists():
        raise RuntimeError("Choose a fresh run ID; an existing input will not be overwritten")
    generation_elapsed, publish_elapsed = lab.publish_generated(args, run_id, fixture)
    targets = topic_offsets()
    report = {"run_id": run_id, "produced": len(fixture.read_text().splitlines()),
              "planned_count": args.plan.count, "target_events_per_minute": args.plan.events_per_minute,
              "target_duration_seconds": args.plan.duration_seconds,
              "generation_elapsed_seconds": round(generation_elapsed, 3),
              "publish_elapsed_seconds": round(publish_elapsed, 3), "oci": wait_acceptance(targets)}
    if args.verify_file:
        deadline = time.monotonic() + 60
        while True:
            result = reconcile(fixture, lab.output_file())
            if result["passed"] or time.monotonic() > deadline:
                break
            time.sleep(1)
        report["file"] = result
    report["passed"] = report["produced"] == args.plan.count and report.get("file", {}).get("passed", True)
    write_report(f"{run_id}.json", report)
    shutil.copy2(fixture, ROOT / "evidence" / fixture.name)
    if not report["passed"]:
        raise RuntimeError("One of the requested paths failed event reconciliation")
    return report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    actions = parser.add_subparsers(dest="action", required=True)
    for command in ["build", "check", "status", "disable"]:
        actions.add_parser(command)
    install = actions.add_parser("install")
    install.add_argument("--skip-build", action="store_true", help="Use the checked archive in evidence/oci-build.json")
    enable = actions.add_parser("enable")
    enable.add_argument("--start-at", choices=["latest", "earliest"], default="latest",
                        help="Initial position for a new connector; existing offsets are always retained")
    test = actions.add_parser("test")
    add_timing_arguments(test)
    test.add_argument("--run-id")
    test.add_argument("--mode", choices=["normal", "error-spike", "edge-cases"], default="normal")
    test.add_argument("--message-bytes", type=int, default=0)
    test.add_argument("--verify-file", action="store_true", help="Also reconcile the FileStream output")
    args = parser.parse_args()
    if args.action == "build":
        result = build_plugin()
    elif args.action == "install":
        result = install_plugin(args.skip_build)
    elif args.action == "check":
        payload = json.loads(init_config().read_text())
        errors = configuration_errors(payload)
        result = {"ready": not errors, "configuration_file": str(local_config_path()), "errors": errors}
        print(json.dumps(result, indent=2))
        return 0 if not errors else 2
    elif args.action == "enable":
        result = enable_oci(args.start_at)
    elif args.action == "disable":
        result = disable_oci()
    elif args.action == "status":
        result = {"file": connector_status(lab.NAME), "file_output": str(lab.output_file()),
                  "oci": connector_status(OCI_NAME), "oci_plugin_installed": OCI_CLASS in installed_classes()}
    else:
        if args.message_bytes < 0:
            parser.error("message-bytes must be non-negative")
        result = test_oci(args)
    print(json.dumps(result, indent=2))
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (RuntimeError, ValueError, OSError, subprocess.SubprocessError) as error:
        print("ERROR:", error, file=sys.stderr)
        raise SystemExit(1)
