#!/usr/bin/env python3
"""Run the isolated Kafka 4.1.2 and host Connect FileStream lab."""
import argparse
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import shutil
import signal
import subprocess
import sys
import time
import urllib.error
import urllib.request
import uuid
from verify_events import reconcile
from generate_logs import add_timing_arguments, generation_plan

ROOT = Path(__file__).resolve().parents[1]
SETTINGS = ROOT / "config/runtime.local.json"
TOPIC = "la-hackathon-logs"
NAME = "oci-la-hackathon"
BROKER = "localhost:19092"
REST = "http://localhost:8083"


def settings():
    if not SETTINGS.exists():
        raise RuntimeError("Run configure with the Kafka, Java 17, and runtime paths first")
    return json.loads(SETTINGS.read_text())


def output_file():
    path = Path(settings().get("output_file", "data/filestream-output.jsonl")).expanduser()
    return (path if path.is_absolute() else ROOT / path).resolve()


def env():
    cfg = settings()
    result = dict(os.environ)
    result.update(JAVA_HOME=cfg["java_home"], LOG_DIR=str(Path(cfg["runtime"]) / "logs"),
                  KAFKA_HEAP_OPTS="-Xms128m -Xmx512m")
    result["PATH"] = cfg["java_home"] + "/bin:" + result.get("PATH", "")
    return result


def kafka(tool, *args, **kwargs):
    cmd = [str(Path(settings()["kafka_home"]) / "bin" / (tool + ".sh")), *args]
    return subprocess.run(cmd, env=env(), check=True, **kwargs)


def api(path, method="GET", data=None):
    body = json.dumps(data).encode() if data is not None else None
    request = urllib.request.Request(REST + path, data=body, method=method,
                                     headers={"Content-Type": "application/json"})
    # Local Connect must not be routed through the corporate HTTP proxy.
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    with opener.open(request, timeout=30) as response:
        content = response.read()
        return json.loads(content) if content else None


def configure(args):
    java = Path(args.java_home).resolve()
    kafka_home = Path(args.kafka_home).resolve()
    runtime = Path(args.runtime).resolve()
    version = subprocess.run([str(java / "bin/java"), "-version"], capture_output=True, text=True, check=True)
    if 'version "17.' not in version.stderr:
        raise RuntimeError("The host worker must use Java 17 for this demo")
    if not (kafka_home / "libs/connect-file-4.1.2.jar").exists():
        raise RuntimeError("Expected the Kafka 4.1.2 binary distribution")
    for name in ["plugins/filestream", "logs", "runs"]:
        (runtime / name).mkdir(parents=True, exist_ok=True)
    (ROOT / "data").mkdir(parents=True, exist_ok=True)
    shutil.copy2(kafka_home / "libs/connect-file-4.1.2.jar", runtime / "plugins/filestream")
    SETTINGS.write_text(json.dumps({"java_home": str(java), "kafka_home": str(kafka_home),
                                    "runtime": str(runtime),
                                    "output_file": "data/filestream-output.jsonl",
                                    "worker_config": str(ROOT / "config/connect-distributed.local.properties")},
                                   indent=2) + "\n")
    props = {"bootstrap.servers": BROKER, "group.id": "la-hackathon-workers",
             "listeners": REST, "config.storage.topic": "la-hackathon-connect-configs",
             "offset.storage.topic": "la-hackathon-connect-offsets",
             "status.storage.topic": "la-hackathon-connect-status",
             "config.storage.replication.factor": "1", "offset.storage.replication.factor": "1",
             "status.storage.replication.factor": "1",
             "key.converter": "org.apache.kafka.connect.storage.StringConverter",
             "value.converter": "org.apache.kafka.connect.storage.StringConverter",
             "plugin.path": str(runtime / "plugins"), "consumer.max.poll.records": "500",
             "consumer.max.poll.interval.ms": "300000", "consumer.auto.offset.reset": "earliest",
             "offset.flush.interval.ms": "1000"}
    (ROOT / "config/connect-distributed.local.properties").write_text(
        "".join(f"{key}={value}\n" for key, value in props.items()))
    print("Configured isolated runtime:", runtime)


def worker_config():
    return settings().get("worker_config", str(ROOT / "config/connect-distributed.local.properties"))


def owned_worker(pid):
    result = subprocess.run(["ps", "-p", str(pid), "-o", "command="], text=True, capture_output=True)
    return worker_config() in result.stdout


def wait_api(path, predicate, seconds=90):
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        try:
            result = api(path)
            if predicate(result):
                return result
        except (OSError, ValueError):
            pass
        time.sleep(1)
    raise RuntimeError(f"Timed out waiting for {path}; inspect runtime/logs/connect.log")


def start_connect():
    runtime = Path(settings()["runtime"])
    pid_path = runtime / "connect.pid"
    try:
        api("/")
    except OSError:
        pass
    else:
        if pid_path.exists() and owned_worker(int(pid_path.read_text())):
            wait_api("/connectors", lambda value: isinstance(value, list))
            print("This lab's Connect worker is already running")
            return
        raise RuntimeError("Port 8083 is occupied by another worker; it was left untouched")
    with (runtime / "logs/connect-console.log").open("a") as log:
        process = subprocess.Popen([str(Path(settings()["kafka_home"]) / "bin/connect-distributed.sh"),
                                    worker_config()],
                                   env=env(), stdout=log, stderr=subprocess.STDOUT, start_new_session=True)
    pid_path.write_text(str(process.pid))
    wait_api("/connector-plugins", lambda plugins: any(
        item["class"] == "org.apache.kafka.connect.file.FileStreamSinkConnector" for item in plugins))
    wait_api("/connectors", lambda value: isinstance(value, list))
    print("Connect is ready on", REST)


def stop_connect():
    pid_path = Path(settings()["runtime"]) / "connect.pid"
    if not pid_path.exists():
        raise RuntimeError("No lab worker PID is recorded")
    pid = int(pid_path.read_text())
    if not owned_worker(pid):
        raise RuntimeError("The recorded process is no longer this worker; no process was stopped")
    os.kill(pid, signal.SIGTERM)
    deadline = time.monotonic() + 45
    while time.monotonic() < deadline and owned_worker(pid):
        time.sleep(0.5)
    if owned_worker(pid):
        raise RuntimeError("Connect has not stopped within 45 seconds")
    pid_path.unlink(missing_ok=True)
    print("Connect stopped; broker, records and offsets retained")


def start():
    subprocess.run(["docker", "compose", "-f", str(ROOT / "compose.yaml"), "up", "-d"], check=True)
    deadline = time.monotonic() + 90
    while True:
        try:
            kafka("kafka-topics", "--bootstrap-server", BROKER, "--list",
                  stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=12)
            break
        except (subprocess.CalledProcessError, subprocess.TimeoutExpired):
            if time.monotonic() > deadline:
                raise RuntimeError("Kafka did not become ready")
    kafka("kafka-topics", "--bootstrap-server", BROKER, "--create", "--if-not-exists",
          "--topic", TOPIC, "--partitions", "1", "--replication-factor", "1", timeout=30)
    start_connect()
    register_file()


def register_file():
    output_file().parent.mkdir(parents=True, exist_ok=True)
    config = {"connector.class": "org.apache.kafka.connect.file.FileStreamSinkConnector",
              "tasks.max": "1", "topics": TOPIC,
              "file": str(output_file()),
              "key.converter": "org.apache.kafka.connect.storage.StringConverter",
              "value.converter": "org.apache.kafka.connect.storage.StringConverter"}
    try:
        existing = api(f"/connectors/{NAME}/config")
    except urllib.error.HTTPError as error:
        if error.code != 404:
            raise
        api("/connectors", "POST", {"name": NAME, "config": config})
    else:
        if existing.get("connector.class") != config["connector.class"]:
            raise RuntimeError("This connector has been switched to another plugin; not replacing it")
        if any(existing.get(key) != value for key, value in config.items()):
            api(f"/connectors/{NAME}/config", "PUT", config)
    api(f"/connectors/{NAME}/resume", "PUT")
    wait_running()


def wait_running():
    return wait_api(f"/connectors/{NAME}/status", lambda status:
                    status["connector"]["state"] == "RUNNING" and bool(status["tasks"]) and
                    all(task["state"] == "RUNNING" for task in status["tasks"]))


def publish_generated(args, run_id, expected):
    """Save each event and stream it to Kafka while the generator is running."""
    generator_command = [sys.executable, str(ROOT / "scripts/generate_logs.py"),
                         "--run-id", run_id, "--mode", args.mode,
                         "--message-bytes", str(args.message_bytes)]
    for name in ["count", "events_per_minute", "duration_minutes", "duration_seconds", "interval_ms"]:
        value = getattr(args, name)
        if value is not None and not (name == "interval_ms" and value == 0):
            generator_command.extend(["--" + name.replace("_", "-"), str(value)])
    producer_command = [str(Path(settings()["kafka_home"]) / "bin/kafka-console-producer.sh"),
                        "--bootstrap-server", BROKER, "--topic", TOPIC,
                        "--producer-property", "acks=all",
                        "--producer-property", "enable.idempotence=true"]
    generator = producer = None
    started = time.monotonic()
    try:
        producer = subprocess.Popen(producer_command, env=env(), stdin=subprocess.PIPE,
                                    text=True, encoding="utf-8")
        generator = subprocess.Popen(generator_command, stdout=subprocess.PIPE,
                                     text=True, encoding="utf-8", bufsize=1)
        with expected.open("w", encoding="utf-8") as fixture:
            for line in generator.stdout:
                fixture.write(line)
                fixture.flush()
                producer.stdin.write(line)
                producer.stdin.flush()
        generator.wait()
        if generator.returncode:
            raise RuntimeError(f"Generator exited with status {generator.returncode}")
        generation_elapsed = time.monotonic() - started
        producer.stdin.close()
        producer.wait(timeout=90)
        if producer.returncode:
            raise RuntimeError(f"Kafka producer exited with status {producer.returncode}")
        return generation_elapsed, time.monotonic() - started
    finally:
        for process in [generator, producer]:
            if process is not None and process.poll() is None:
                process.terminate()
                try:
                    process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait()


def exercise(args, recovery=False):
    wait_running()
    run_id = args.run_id or ("recovery-" if recovery else "baseline-") + uuid.uuid4().hex[:12]
    runtime = Path(settings()["runtime"])
    expected = runtime / "runs" / f"{run_id}.jsonl"
    if expected.exists():
        raise RuntimeError("Choose a fresh run ID; an existing fixture will not be overwritten")
    if recovery:
        # Allow the previous successful run's offsets to reach Kafka before shutdown.
        time.sleep(2)
        stop_connect()
    try:
        generation_elapsed, publish_elapsed = publish_generated(args, run_id, expected)
    finally:
        if recovery:
            start_connect()
    wait_running()
    actual = output_file()
    deadline = time.monotonic() + 60
    while True:
        result = reconcile(expected, actual)
        if result["passed"] or time.monotonic() > deadline:
            break
        time.sleep(1)
    result.update(test="restart-recovery" if recovery else "filestream-baseline", mode=args.mode,
                  planned_count=args.plan.count,
                  target_events_per_minute=args.plan.events_per_minute,
                  target_duration_seconds=args.plan.duration_seconds,
                  generation_elapsed_seconds=round(generation_elapsed, 3),
                  publish_elapsed_seconds=round(publish_elapsed, 3),
                  completed_utc=datetime.now(timezone.utc).isoformat(), kafka_version="4.1.2",
                  broker=BROKER, expected_fixture=str(expected), actual_output=str(actual),
                  connector_status=api(f"/connectors/{NAME}/status"))
    result["passed"] = result["passed"] and result["produced"] == args.plan.count
    if recovery:
        result["worker_stopped_during_publish"] = True
    report = ROOT / "evidence" / f"{run_id}.json"
    report.write_text(json.dumps(result, indent=2, ensure_ascii=False) + "\n")
    shutil.copy2(expected, ROOT / "evidence" / expected.name)
    print(json.dumps(result, indent=2, ensure_ascii=False))
    if not result["passed"]:
        raise RuntimeError("Event reconciliation failed")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="action", required=True)
    configure_parser = sub.add_parser("configure")
    for arg in ["kafka-home", "java-home", "runtime"]:
        configure_parser.add_argument("--" + arg, required=True)
    for command in ["start", "start-connect", "stop-connect", "status"]:
        sub.add_parser(command)
    for command in ["test", "recovery"]:
        test = sub.add_parser(command)
        add_timing_arguments(test)
        test.add_argument("--run-id")
        test.add_argument("--mode", choices=["normal", "error-spike", "edge-cases"], default="normal")
        test.add_argument("--message-bytes", type=int, default=0)
    args = parser.parse_args()
    if args.action == "configure":
        configure(args)
    elif args.action == "start":
        start()
    elif args.action == "start-connect":
        start_connect(); wait_running()
    elif args.action == "stop-connect":
        stop_connect()
    elif args.action == "status":
        print(json.dumps({"worker": api("/"), "connector": api(f"/connectors/{NAME}/status")}, indent=2))
    else:
        try:
            args.plan = generation_plan(args)
        except ValueError as error:
            parser.error(str(error))
        if args.plan.count < 1 or args.message_bytes < 0:
            parser.error("count must be positive and message-bytes non-negative")
        if args.run_id and (not args.run_id.replace("-", "").replace("_", "").isalnum()):
            parser.error("run-id must contain only letters, numbers, hyphens or underscores")
        exercise(args, recovery=args.action == "recovery")


if __name__ == "__main__":
    try:
        main()
    except (RuntimeError, OSError, subprocess.SubprocessError) as error:
        print("ERROR:", error, file=sys.stderr)
        raise SystemExit(1)
