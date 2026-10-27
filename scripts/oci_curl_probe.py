#!/usr/bin/env python3
"""Send one synthetic event using signed curl, independently of Kafka and Java."""
import base64
import configparser
from datetime import datetime, timezone
from email.utils import format_datetime
import hashlib
import json
import os
from pathlib import Path
import subprocess
from urllib.parse import quote, urlencode
import uuid

ROOT = Path(__file__).resolve().parents[1]


def curl_value(value):
    if "\n" in value or "\r" in value:
        raise ValueError("Unexpected newline in curl setting")
    return '"' + value.replace("\\", "\\\\").replace('"', '\\"') + '"'


def main():
    config_path = ROOT / "config/oci-sink.local.json"
    original_config_hash = hashlib.sha256(config_path.read_bytes()).hexdigest()
    config = json.loads(config_path.read_text())["config"]
    profiles = configparser.ConfigParser(interpolation=None)
    profiles.read(Path(config["oci.config.file"]).expanduser())
    profile = profiles[config["oci.config.profile"]]
    run_id = "curl-isolation-" + uuid.uuid4().hex[:12]
    directory = ROOT / "evidence" / run_id
    directory.mkdir(mode=0o700)
    record = {
        "timestamp": datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z"),
        "level": "INFO", "service": "curl-isolation", "run_id": run_id,
        "event_id": run_id + "-1", "message": "Standalone signed curl upload check", "latency_ms": 0,
    }
    event = {"logSourceName": config["oci.la.log.source.name"]}
    for field, setting in {
        "logPath": "oci.la.log.path", "entityId": "oci.la.entity.id",
        "entityType": "oci.la.entity.type", "timezone": "oci.la.timezone",
    }.items():
        if config.get(setting):
            event[field] = config[setting]
    event["logRecords"] = [json.dumps(record, ensure_ascii=False)]
    payload = json.dumps({"logEvents": [event]}, ensure_ascii=False).encode()
    payload_path = directory / "request.json"
    payload_path.write_bytes(payload)
    host = "loganalytics." + config["oci.region"] + ".oci.oraclecloud.com"
    query = {"logGroupId": config["oci.la.log.group.id"], "payloadType": "JSON"}
    if config.get("oci.la.log.set"):
        query["logSet"] = config["oci.la.log.set"]
    target = "/20200601/namespaces/" + quote(config["oci.namespace"], safe="")
    target += "/actions/uploadLogEventsFile?" + urlencode(query)
    date = format_datetime(datetime.now(timezone.utc), usegmt=True)
    headers = {
        "date": date, "host": host, "content-type": "application/octet-stream",
        "content-length": str(len(payload)),
        "x-content-sha256": base64.b64encode(hashlib.sha256(payload).digest()).decode(),
    }
    names = ["(request-target)", "date", "host", "content-type", "content-length", "x-content-sha256"]
    signing = "(request-target): post " + target + "\n"
    signing += "\n".join(name + ": " + headers[name] for name in names[1:])
    environment = dict(os.environ)
    environment["OCI_CURL_KEY_PASSPHRASE"] = profile.get("pass_phrase", "")
    signed = subprocess.run(
        ["openssl", "dgst", "-sha256", "-sign", str(Path(profile["key_file"]).expanduser()),
         "-passin", "env:OCI_CURL_KEY_PASSPHRASE"],
        input=signing.encode(), capture_output=True, env=environment, check=True,
    ).stdout
    key_id = "/".join(profile[name] for name in ["tenancy", "user", "fingerprint"])
    authorization = (
        'Signature version="1",keyId="' + key_id + '",algorithm="rsa-sha256",headers="'
        + " ".join(names) + '",signature="' + base64.b64encode(signed).decode() + '"'
    )
    headers.update({"Authorization": authorization, "opc-request-id": run_id,
                    "opc-retry-token": str(uuid.uuid4()), "Expect": ""})
    # Supply signed headers on stdin so authorization is absent from process arguments and files.
    curl_config = "\n".join("header = " + curl_value(k + ": " + v) for k, v in headers.items()) + "\n"
    response_path = directory / "response.json"
    header_path = directory / "response.headers"
    command = [
        "curl", "--disable", "--config", "-", "--noproxy", "*", "--silent", "--show-error",
        "--connect-timeout", "10", "--max-time", "30", "--path-as-is",
        "--request", "POST", "--url", "https://" + host + target,
        "--data-binary", "@" + str(payload_path), "--dump-header", str(header_path),
        "--output", str(response_path), "--write-out", "%{json}",
    ]
    result = subprocess.run(command, input=curl_config, text=True, capture_output=True, timeout=40)
    try:
        timings = json.loads(result.stdout)
    except ValueError:
        timings = {}
    try:
        response = json.loads(response_path.read_text())
    except (ValueError, OSError):
        response = {"body": response_path.read_text()[:2000] if response_path.exists() else ""}
    response_headers = {}
    if header_path.exists():
        for line in header_path.read_text().splitlines():
            if ":" in line:
                key, value = line.split(":", 1)
                if key.lower() in ["opc-request-id", "content-type", "date", "retry-after"]:
                    response_headers[key.lower()] = value.strip()
    summary = {
        "run_id": run_id, "profile": config["oci.config.profile"],
        "authentication": "config_file API-key RSA-SHA256; no session token",
        "transport": "curl direct HTTPS; proxy bypassed; TLS verification enabled",
        "method": "POST", "endpoint": "https://" + host + target, "records": 1,
        "curl_exit_code": result.returncode, "curl_error": result.stderr.strip(),
        "http_status": timings.get("http_code"),
        "network": {name: timings.get(name) for name in [
            "remote_ip", "ssl_verify_result", "time_namelookup", "time_connect",
            "time_appconnect", "time_starttransfer", "time_total", "size_upload",
        ]},
        "response_headers": response_headers, "response": response,
        "api_acceptance_verified": timings.get("http_code") == 200,
        "log_analytics_indexing_verified": False,
        "local_config_unchanged": hashlib.sha256(config_path.read_bytes()).hexdigest() == original_config_hash,
    }
    (directory / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")
    print(json.dumps(summary, indent=2))
    print("Evidence:", directory / "summary.json")
    return 0 if summary["api_acceptance_verified"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
