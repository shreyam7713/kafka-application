"""Install the locally rebuilt diagnostics distribution and restart this lab only."""
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import uuid
import xml.etree.ElementTree as ET
from zipfile import ZipFile

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "scripts"))
import lab
import oci


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def tree_hashes(directory):
    return {str(p.relative_to(directory)): digest(p)
            for p in sorted(directory.rglob("*")) if p.is_file()}


source = ROOT / "kafka-connect-oracleobservability"
archive = source / "target/kafka-connect-oracleobservability-0.1.0-SNAPSHOT-plugin.zip"
config = ROOT / "config/oci-sink.local.json"
config_before = digest(config)
runtime = Path(lab.settings()["runtime"])
destination = runtime / "plugins/oracle-observability"
backup = runtime / "plugin-backups" / ("oracle-observability-" + uuid.uuid4().hex[:12])
report = {
    "started_utc": datetime.now(timezone.utc).isoformat(),
    "source_directory": str(source),
    "source_revision": subprocess.check_output(
        ["git", "-C", str(source), "rev-parse", "HEAD"], text=True).strip(),
    "source_changes": subprocess.check_output(
        ["git", "-C", str(source), "status", "--short"], text=True).splitlines(),
    "source_hashes": {str(p.relative_to(source)): digest(p)
                      for p in sorted((source / "src").rglob("*")) if p.is_file()},
    "archive": str(archive),
    "archive_sha256": digest(archive),
    "local_config_sha256_before": config_before,
    "old_pid": int((runtime / "connect.pid").read_text()),
    "log_byte_start": (runtime / "logs/connect.log").stat().st_size,
    "offsets_before": lab.api(f"/connectors/{oci.OCI_NAME}/offsets"),
    "installed_directory": str(destination),
    "backup_directory": str(backup),
}
totals = dict(tests=0, failures=0, errors=0, skipped=0)
for pattern in ["surefire-reports/TEST-*.xml", "failsafe-reports/TEST-*.xml"]:
    for path in (source / "target").glob(pattern):
        suite = ET.parse(path).getroot()
        for key in totals:
            totals[key] += int(suite.attrib.get(key, 0))
if totals["tests"] != 48 or any(totals[k] for k in ["failures", "errors", "skipped"]):
    raise RuntimeError(f"Rebuilt distribution verification is incomplete: {totals}")
report["verification"] = totals
active_config = lab.api(f"/connectors/{oci.OCI_NAME}/config")
oci.validate_schema(active_config)

with tempfile.TemporaryDirectory(prefix="rebuilt-plugin-", dir=runtime) as temporary:
    distribution, jar_count = oci.prepare_distribution(archive, temporary)
    project_jar = distribution / "lib/kafka-connect-oracleobservability-0.1.0-SNAPSHOT.jar"
    with ZipFile(project_jar) as jar:
        if "com/oracle/observability/kafka/connect/UploadResponseDiagnostics.class" not in jar.namelist():
            raise RuntimeError("Rebuilt plugin does not include upload diagnostics")
    expected_hashes = tree_hashes(distribution)
    report["project_jar_sha256"] = digest(project_jar)
    report["runtime_jars"] = jar_count
    lab.stop_connect()
    changed = False
    try:
        backup.parent.mkdir(exist_ok=True)
        destination.rename(backup)
        shutil.move(str(distribution), destination)
        changed = True
        if tree_hashes(destination) != expected_hashes:
            raise RuntimeError("Installed distribution differs from the rebuilt ZIP")
        lab.start_connect()
        if oci.OCI_CLASS not in oci.installed_classes():
            raise RuntimeError("Rebuilt plugin was not discovered")
        oci.validate_schema(active_config)
    except BaseException:
        if changed:
            try:
                lab.stop_connect()
            except (OSError, RuntimeError):
                pass
            shutil.rmtree(destination)
        if backup.exists():
            backup.rename(destination)
        lab.start_connect()
        raise

report["new_pid"] = int((runtime / "connect.pid").read_text())
report["plugin_discovered"] = True
report["installed_files_match_zip"] = tree_hashes(destination) == expected_hashes
report["local_config_sha256_after"] = digest(config)
report["local_config_unchanged"] = digest(config) == config_before
report["registered_oci_config_unchanged"] = (
    lab.api(f"/connectors/{oci.OCI_NAME}/config") == active_config)
report["oci_status"] = oci.connector_status(oci.OCI_NAME)
report["file_status"] = oci.connector_status(lab.NAME)
report["offsets_after"] = lab.api(f"/connectors/{oci.OCI_NAME}/offsets")
report["finished_utc"] = datetime.now(timezone.utc).isoformat()
(ROOT / "evidence/oci-rebuilt-installation.json").write_text(json.dumps(report, indent=2) + "\n")
if not report["local_config_unchanged"] or not report["registered_oci_config_unchanged"]:
    raise RuntimeError("Configuration changed during installation; inspect the report")
print(json.dumps(report, indent=2))
