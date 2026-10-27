# Kafka to OCI Log Analytics Lab

A setup and operations guide for generating application logs, sending them through Kafka, and delivering them to a local JSONL file, OCI Log Analytics, or both.

**Start with the file path.** It needs no OCI account and proves the generator, broker, Connect worker, and message contract. Add OCI after its authentication, source, parser, and direct upload work independently.

This is a single-machine hackathon lab: one broker, one application topic partition, one task per sink, and plaintext Kafka restricted to the local machine. It demonstrates correctness and recovery; it is not a production deployment or an exactly-once delivery system.

## Contents

- [1. Architecture and identifiers](#1-architecture-and-identifiers)
- [2. Requirements and supported environment](#2-requirements-and-supported-environment)
- [3. Project layout and local configuration](#3-project-layout-and-local-configuration)
- [4. Fresh-machine setup](#4-fresh-machine-setup)
- [5. File path quickstart](#5-file-path-quickstart)
- [6. Traffic rate duration and event format](#6-traffic-rate-duration-and-event-format)
- [7. Trace a run manually](#7-trace-a-run-manually)
- [8. Prepare OCI Log Analytics](#8-prepare-oci-log-analytics)
- [9. Build and install the OCI plugin](#9-build-and-install-the-oci-plugin)
- [10. Configure and authenticate the OCI path](#10-configure-and-authenticate-the-oci-path)
- [11. Prove a direct OCI upload](#11-prove-a-direct-oci-upload)
- [12. Enable and test the OCI path](#12-enable-and-test-the-oci-path)
- [13. Select file OCI or both](#13-select-file-oci-or-both)
- [14. Recovery testing](#14-recovery-testing)
- [15. Status logs offsets and evidence](#15-status-logs-offsets-and-evidence)
- [16. Troubleshooting](#16-troubleshooting)
- [17. Stop resume and reset](#17-stop-resume-and-reset)
- [18. Command reference](#18-command-reference)
- [19. Share the project with another person](#19-share-the-project-with-another-person)
- [20. Update the upstream connector](#20-update-the-upstream-connector)
- [21. Validation history and remaining limits](#21-validation-history-and-remaining-limits)
- [22. References](#22-references)

## 1. Architecture and identifiers

```text
Synthetic JSON generator
          |
          v
Kafka topic: la-hackathon-logs
          |
          +--> FileStream sink --> data/filestream-output.jsonl
          |
          +--> OCI sink --> Log Events API --> LA source/parser --> Log Explorer
```

The two sinks are separate Kafka Connect connectors with independent consumer groups. They each receive the Kafka records; they do not divide the records between themselves. Stopping one connector does not stop the other. Stopping the shared worker stops both until the worker resumes.

| Setting | Default |
|---|---|
| Broker exposed to host tools | `localhost:19092` |
| Connect REST endpoint | `http://localhost:8083` |
| Application topic | `la-hackathon-logs` |
| Application topic partitions / replicas | `1` / `1` |
| File connector name | `oci-la-hackathon` |
| File consumer group | `connect-oci-la-hackathon` |
| OCI connector name | `oci-la-cloud` |
| OCI consumer group | `connect-oci-la-cloud` |
| Connect worker group | `la-hackathon-workers` |
| Connect internal topics | `la-hackathon-connect-configs`, `la-hackathon-connect-offsets`, `la-hackathon-connect-status` |
| Docker Compose project | `shreyam-hackathon` |
| Broker container | `shreyam-hackathon-kafka` |
| Persistent Docker volume | `shreyam-hackathon-kafka-data` |
| Locally built broker image | `shreyam-hackathon-kafka:4.1.2` |

The file connector keeps its historical `oci-la-hackathon` name to retain its offsets. Its class is still FileStream. **Keep the OCI connector named `oci-la-cloud`** in the local OCI configuration. Reusing the file connector name would collide with the existing path.

The worker runs on the host. Its `localhost:19092` address is therefore correct. This listener configuration is not intended for a Connect worker running in another container or on another machine.

## 2. Requirements and supported environment

| Requirement | Purpose / check |
|---|---|
| macOS or Linux with a Bash-compatible shell | The controller uses POSIX process handling and `ps`. Native Windows is not supported by these scripts. |
| Python 3.10 or newer | `python3 --version`; the Python scripts use only the standard library. |
| Java **17** JDK on the host | `java -version`; `lab.py configure` requires version 17. |
| Kafka **4.1.2 binary distribution** on the host | Provides Connect, FileStream, producer, consumer, and administration tools. |
| Docker engine and Compose | `docker version` must show a reachable server; `docker compose version` must work. |
| Git | Downloads the separate OCI connector repository. |
| Maven | Builds and verifies the OCI plugin. Not needed for file-only testing after setup. |
| curl and tar | Downloads dependencies and runs REST checks. |
| OCI CLI, optionally | Recommended for authentication checks and direct Log Events uploads. |
| Your own OCI identity and resources | Required only for the cloud path. |

The original tested environment was macOS on Apple Silicon, native arm64 Java 17 for Connect, and an amd64 Java 21 broker image under Rancher Desktop. The Compose file and broker build script currently select `linux/amd64`; Apple Silicon needs Docker's amd64 emulation. WSL2 may be usable with Linux Java/Python and Docker integration, but it has not been validated here.

For Rancher Desktop, select a Docker-compatible engine such as Moby and verify the Docker context. A containerd-only configuration does not supply the Docker workflow used below.

Allocate enough memory for Docker and a host Connect JVM. The scripts use a 512 MiB maximum broker heap and a 512 MiB maximum Connect heap; dependencies, native memory, and other containers use additional memory.

## 3. Project layout and local configuration

```text
.
├── README.md
├── NOTICE
├── Dockerfile
├── compose.yaml
├── config/
│   ├── server.properties                      # broker settings
│   ├── oci-upstream.json                      # repository URL and pinned commit
│   ├── oci-sink.json                          # shareable OCI template
│   ├── oci-sink.local.json                    # your OCI destination; ignored by Git
│   ├── runtime.local.json                     # your absolute tool/runtime paths; ignored
│   └── connect-distributed.local.properties   # generated worker settings; ignored
├── scripts/
│   ├── lab.py                                # file path and worker lifecycle
│   ├── oci.py                                # OCI build/install/enable/test
│   ├── generate_logs.py                       # standalone paced JSONL generator
│   ├── verify_events.py                       # exact event comparison
│   ├── build_broker.py                        # builds the local broker image
│   └── broker-entrypoint.sh
├── connectors/
│   ├── README.md
│   └── oracle-observability/                  # separate upstream checkout; ignored
├── data/
│   └── filestream-output.jsonl                # live sink output; ignored
├── evidence/                                 # per-run reports and retained fixtures
├── tests/                                    # local Python tests
└── .runtime/                                 # suggested new-user dependency/state location
```

The upstream repository alone does not contain this lab's controllers. Give another person the complete wrapper project, as described in [section 19](#19-share-the-project-with-another-person).

`runtime.local.json` records `java_home`, `kafka_home`, `runtime`, `worker_config`, and `output_file`. An optional `maven_repository` selects a Maven cache. `output_file` can be absolute or relative to this repository; its default is `data/filestream-output.jsonl`.

The existing author's laptop uses a runtime outside this folder and an explicit worker-config path. **Another person must generate their own local configuration.** Copying these local files will retain someone else's absolute paths.

For a new setup, the guide puts everything local under `.runtime/`. Its state directory contains `runs/`, `logs/`, `plugins/filestream/`, `plugins/oracle-observability/`, and the worker PID. Kafka records and committed offsets are retained in the Docker volume, not in the JSONL output file.

## 4. Fresh-machine setup

This section is for a new machine or a new unconfigured copy. If this lab is already configured and running, use [section 5](#5-file-path-quickstart) instead. Running `configure` again overwrites local paths and generated worker settings.

### 4.1 Open the complete project

The examples assume the folder is on your Desktop. Change the first command if you unpacked it elsewhere. All later commands run from this project root.

```bash
cd ~/Desktop/Shreyam-Kafka-Hackathon
export LAB_ROOT="$(pwd -P)"
export LAB_DOWNLOADS="$LAB_ROOT/.runtime/downloads"
export LAB_DEPS="$LAB_ROOT/.runtime/deps"
mkdir -p "$LAB_DOWNLOADS" "$LAB_DEPS" "$LAB_ROOT/evidence"
```

Use a new Terminal tab for this project. Do not change or overwrite the shell's `HOME` variable.

### 4.2 Install host tools and select Java 17

On macOS with Homebrew, one option is:

```bash
brew install python git maven openjdk@17
export JAVA_HOME="$(brew --prefix openjdk@17)/libexec/openjdk.jdk/Contents/Home"
export PATH="$JAVA_HOME/bin:$PATH"
```

On a Debian/Ubuntu Linux host, one option is:

```bash
sudo apt-get update
sudo apt-get install -y python3 git curl maven openjdk-17-jdk
export JAVA_HOME="$(dirname "$(dirname "$(readlink -f "$(command -v javac)")")")"
export PATH="$JAVA_HOME/bin:$PATH"
```

A vendor Java 17 archive is also suitable. Set `JAVA_HOME` to its JDK home, containing `bin/java`; on macOS that is normally the archive's `Contents/Home` directory.

Install and start Docker Desktop, Rancher Desktop with its Docker engine, or Docker Engine on Linux. Then check:

```bash
python3 --version
java -version
mvn -version
git --version
docker version
docker compose version
```

Java and Maven should report Java 17 in this shell. A different system Java can remain installed; the generated lab configuration explicitly selects the supplied Java 17 home.

### 4.3 Download the Kafka 4.1.2 binary and verify it

The binary distribution is required, rather than the source archive.

```bash
curl --fail --location --retry 3 \
  --output "$LAB_DOWNLOADS/kafka_2.13-4.1.2.tgz" \
  https://archive.apache.org/dist/kafka/4.1.2/kafka_2.13-4.1.2.tgz

curl --fail --location --retry 3 \
  --output "$LAB_DOWNLOADS/kafka_2.13-4.1.2.tgz.sha512" \
  https://archive.apache.org/dist/kafka/4.1.2/kafka_2.13-4.1.2.tgz.sha512

python3 - "$LAB_DOWNLOADS/kafka_2.13-4.1.2.tgz" "$LAB_DOWNLOADS/kafka_2.13-4.1.2.tgz.sha512" <<'PY'
import hashlib
from pathlib import Path
import sys

archive, checksum = map(Path, sys.argv[1:])
digest = hashlib.sha512()
with archive.open("rb") as source:
    for chunk in iter(lambda: source.read(1024 * 1024), b""):
        digest.update(chunk)
published = "".join(checksum.read_text().split()).lower()
if digest.hexdigest() not in published:
    raise SystemExit("SHA-512 mismatch; do not extract this download")
print("Kafka archive SHA-512 verified")
PY

tar -xzf "$LAB_DOWNLOADS/kafka_2.13-4.1.2.tgz" -C "$LAB_DEPS"
export LAB_KAFKA_DIR="$LAB_DEPS/kafka_2.13-4.1.2"
```

The archive URL is pinned for reproducibility. For Apache signature verification and release information, see the [Kafka downloads page](https://kafka.apache.org/community/downloads/).

### 4.4 Generate this machine's local configuration

```bash
python3 scripts/lab.py configure \
  --kafka-home "$LAB_KAFKA_DIR" \
  --java-home "$JAVA_HOME" \
  --runtime "$LAB_ROOT/.runtime/state"
```

This creates the worker configuration and runtime directories, explicitly installs `connect-file-4.1.2.jar` in its own plugin directory, and selects the repo's `data/filestream-output.jsonl` destination. It does not start Docker or Connect.

### 4.5 Build the local Kafka broker image

`shreyam-hackathon-kafka:4.1.2` is a **local image tag**. It is not a published image that other people can pull. Build it before starting Compose.

The following uses a generic public Java 21 base image:

```bash
docker pull --platform linux/amd64 eclipse-temurin:21-jre

python3 scripts/build_broker.py \
  --base-image eclipse-temurin:21-jre

docker image inspect shreyam-hackathon-kafka:4.1.2 \
  --format '{{.Os}}/{{.Architecture}} {{.Id}}'
```

The builder copies the verified Kafka distribution into `/opt/hackathon-kafka`, builds for `linux/amd64`, and uses the supplied image only for its Linux/Java runtime. The entrypoint explicitly starts Kafka 4.1.2 from the new directory. The base must have Java 17 or newer, Bash, and standard Linux utilities.

On the original laptop, an approved cached Java 21 image was used because pulling the official Kafka Docker image encountered a Docker daemon certificate-chain error. Another person does not need access to that internal image. If public pulls are blocked in your organization, use an approved cached Java image or approved registry mirror; resolve certificate trust through your normal Docker setup. TLS verification should remain enabled.

### 4.6 Start and prove the file path

```bash
python3 scripts/lab.py start
python3 scripts/lab.py status
python3 scripts/lab.py test --count 10
```

`start` brings up the broker with persistent storage, waits for host connectivity, creates the topic if missing, starts the host Connect worker, and registers/resumes FileStream. Expect the connector and task to show `RUNNING` and the test report to show `passed: true`, `produced: 10`, and `received_unique: 10`.

Keep the terminal's setup variables for later commands, or reload them from local configuration as shown in section 7. There is no Python virtual environment or `pip install` step for this project.

## 5. File path quickstart

For an already configured project:

```bash
python3 scripts/lab.py start
python3 scripts/lab.py status

# Ten events, one approximately every six seconds, for one minute.
python3 scripts/lab.py test \
  --events-per-minute 10 \
  --duration-minutes 1
```

This verifies FileStream without disabling an OCI connector that is already active. For file-only traffic, run `python3 scripts/oci.py disable` after starting the worker, or follow section 13.

Watch the output from a second Terminal opened in the same repository:

```bash
tail -f data/filestream-output.jsonl
```

Or inspect the latest ten records after the run:

```bash
tail -n 10 data/filestream-output.jsonl
```

The output is JSON Lines: one JSON object per physical line. It is not a single JSON array. Existing records are appended; every test gets a fresh run ID unless you explicitly supply one. `Ctrl+C` stops `tail`, not Kafka or Connect.

## 6. Traffic rate duration and event format

### 6.1 Scheduling rules

Supply any two of total count, rate, and duration; the third is calculated. A count alone is an immediate run. With no timing options, the default is 100 immediate events.

| Options | Result |
|---|---|
| `--count 100` | 100 events as quickly as the pipeline permits |
| `--events-per-minute 10 --duration-minutes 1` | 10 events over one minute; target spacing 6 seconds |
| `--events-per-minute 600 --duration-minutes 5` | 3,000 events over five minutes; target spacing 0.1 seconds |
| `--count 1000 --duration-minutes 5` | 1,000 events over five minutes; 200 events/minute |
| `--count 300 --events-per-minute 600` | 300 events over 30 seconds |
| `--events-per-minute 120 --duration-seconds 10` | 20 events over ten seconds |
| `--count 10 --interval-ms 500` | Legacy count-mode pacing, approximately two events/second |

Minutes and seconds accept positive fractional values. Choose only one duration unit. Rate × duration is rounded down to a whole event; a timed setting yielding zero events is rejected. Do not combine all three count/rate/duration settings, or combine `--interval-ms` with rate/duration.

The first event is generated at the start. For ten events over one minute, scheduled event times are approximately 0, 6, 12, …, 54 seconds; the generator stays open until the one-minute target ends. It uses a monotonic clock. Pacing is a target generation rate, not a guarantee of downstream arrival timing. Producer startup, Kafka buffering, backpressure, API calls, and LA indexing add delay. A slow output can extend the actual run.

### 6.2 Modes and message size

```bash
python3 scripts/lab.py test --count 100 --mode normal
python3 scripts/lab.py test --count 100 --mode error-spike
python3 scripts/lab.py test --count 100 --mode edge-cases
python3 scripts/lab.py test --count 250 --message-bytes 16000
```

| Mode | Content |
|---|---|
| `normal` | Deterministic INFO/WARN/ERROR traffic; an ERROR every 20th index and WARN at other multiples of five |
| `error-spike` | An ERROR at every other index, with WARN and INFO in the remaining traffic |
| `edge-cases` | Quotes, backslashes, Unicode, embedded newline/tab text, and JSON text inside the message |

`--message-bytes` is a legacy name for **minimum character padding**. With the normal ASCII messages, `16000` creates a 16,000-byte message string. Unicode can require more UTF-8 bytes. The entire serialized event and upload envelope are larger than the message field.

All modes work with timed runs and with `scripts/oci.py test`. An ERROR-level application log is ordinary data. Simulated HTTP 429/503 failures are a separate connector reliability test.

### 6.3 Event contract

Example only; live timestamps and IDs are generated for each run:

```json
{
  "timestamp": "2026-10-27T10:00:00.000Z",
  "demo_run_id": "example-run",
  "event_id": "example-run-000000",
  "service": "checkout",
  "level": "ERROR",
  "message": "Payment request failed",
  "latency_ms": 850
}
```

`demo_run_id` groups a test; `event_id` identifies an event within it. Timestamp strings use current UTC with milliseconds and `Z`. Identifiers, levels, services, and messages are strings; `latency_ms` is an integer. Services cycle through checkout, inventory, and payments. Event indexes start at zero.

Kafka carries the raw compact JSON as a string. StringConverter preserves that contract. Embedded newlines are escaped inside the JSON string rather than creating additional Kafka messages.

The standalone generator writes JSONL to stdout and diagnostics to stderr:

```bash
python3 scripts/generate_logs.py \
  --events-per-minute 10 --duration-minutes 1
```

For paced end-to-end traffic, use the test controllers or pipe the generator directly to Kafka. Generating a complete file first and publishing it later preserves the data, but not the original sending rate.

## 7. Trace a run manually

These commands demonstrate each stage rather than using the combined test runner. Run them in the same Terminal. The console-read example assumes this demo is the only producer during the run.

### 7.1 Load configured paths

```bash
export LAB_ROOT="$(pwd -P)"
export JAVA_HOME="$(python3 -c 'import json; print(json.load(open("config/runtime.local.json"))["java_home"])')"
export LAB_KAFKA_DIR="$(python3 -c 'import json; print(json.load(open("config/runtime.local.json"))["kafka_home"])')"
export LAB_RUNTIME="$(python3 -c 'import json; print(json.load(open("config/runtime.local.json"))["runtime"])')"
export PATH="$JAVA_HOME/bin:$PATH"
export LAB_OUTPUT="$LAB_ROOT/data/filestream-output.jsonl"
```

If you customized `output_file`, use that configured destination for `LAB_OUTPUT` instead.

### 7.2 Check the topic and connector

```bash
"$LAB_KAFKA_DIR/bin/kafka-topics.sh" \
  --bootstrap-server localhost:19092 \
  --describe --topic la-hackathon-logs

curl --noproxy '*' --fail --silent --show-error \
  http://localhost:8083/connectors/oci-la-hackathon/status |
  python3 -m json.tool

curl --noproxy '*' --fail --silent --show-error \
  http://localhost:8083/connectors/oci-la-hackathon/config |
  python3 -m json.tool
```

Expect one application partition, replication factor one, FileStream as the class, and the repo file as its destination.

### 7.3 Remember where new Kafka records will begin

```bash
LAB_START_OFFSET="$(
  "$LAB_KAFKA_DIR/bin/kafka-get-offsets.sh" \
    --bootstrap-server localhost:19092 \
    --topic la-hackathon-logs --time -1 |
  awk -F: '{print $3}'
)"
LAB_RUN_ID="manual-$(python3 -c 'import uuid; print(uuid.uuid4().hex[:12])')"
LAB_INPUT="$LAB_RUNTIME/runs/$LAB_RUN_ID.jsonl"
```

### 7.4 Generate save and publish live

Open `tail -f data/filestream-output.jsonl` in another Terminal first if you want to watch arrivals.

```bash
set -o pipefail
python3 scripts/generate_logs.py \
  --run-id "$LAB_RUN_ID" \
  --events-per-minute 10 --duration-minutes 1 |
  tee "$LAB_INPUT" |
  "$LAB_KAFKA_DIR/bin/kafka-console-producer.sh" \
    --bootstrap-server localhost:19092 \
    --topic la-hackathon-logs \
    --producer-property acks=all \
    --producer-property enable.idempotence=true
```

`tee` retains the exact generated input while forwarding each line. FileStream independently consumes Kafka and writes the final output file.

### 7.5 Read those records directly from Kafka

```bash
"$LAB_KAFKA_DIR/bin/kafka-console-consumer.sh" \
  --bootstrap-server localhost:19092 \
  --topic la-hackathon-logs --partition 0 \
  --offset "$LAB_START_OFFSET" \
  --max-messages 10 --timeout-ms 10000
```

This is a separate reader and does not advance the FileStream consumer group. With other producers active, the next ten offsets can include their records; use run IDs and the verifier to identify your test precisely.

### 7.6 Inspect and compare the final file

```bash
grep -F "$LAB_RUN_ID" "$LAB_OUTPUT"
python3 scripts/verify_events.py "$LAB_INPUT" "$LAB_OUTPUT"

"$LAB_KAFKA_DIR/bin/kafka-consumer-groups.sh" \
  --bootstrap-server localhost:19092 \
  --describe --group connect-oci-la-hackathon
```

Expect ten produced and ten unique received events, no missing or changed IDs, and `passed: true`. The verifier compares complete event values, not just line counts. It reports duplicates separately; duplicates alone do not make the comparison fail because replay can be at least once. Invalid JSON lines anywhere in the accumulated output are reported as malformed.

For a caught-up file sink, `LAG` reaches zero and the current offset equals the log end offset. Give the asynchronous consumer time to catch up before declaring a missing event.

## 8. Prepare OCI Log Analytics

The file path can remain running while you prepare OCI. Use an authorized tenancy, region, compartment, and dedicated log group for synthetic demo traffic. Do not reuse another person's OCIDs or profile.

Record these values:

| Value | Meaning |
|---|---|
| Region | OCI region identifier, such as `us-phoenix-1` |
| Namespace | Verified Log Analytics namespace, not a tenancy display name |
| Log group OCID | Actual destination log group; different from a compartment OCID |
| Source API name | Exact existing source name accepted by the upload API |
| OCI profile | API-key profile whose user has the required upload permissions |
| Parser | JSON parser attached to that source |

Ask the environment administrator to apply the appropriate compartment-scoped upload policies and resource access. Follow the service's [Log Events prerequisites and upload guidance](https://docs.oracle.com/en-us/iaas/log-analytics/doc/upload-event-logs-using-logevents-api.html); this project does not provision IAM policies.

Create or verify a **File** source for these application logs. The source type describes the log source; selecting a REST API source is not how you choose the push transport. Attach a JSON parser with root `$`. A suitable mapping for the generator's contract is:

| JSON path | Example LA field | Type |
|---|---|---|
| `$.timestamp` | Time | Timestamp |
| `$.message` | Message | String |
| `$.demo_run_id` | Hackathon Run ID | String |
| `$.event_id` | Hackathon Event ID | String |
| `$.service` | Hackathon Service | String |
| `$.level` | Hackathon Level | String |
| `$.latency_ms` | Hackathon Latency Milliseconds | Long |

Create custom fields where necessary. In parser preview, check all values, numeric latency, and UTC event time. Falling back to ingestion time does not prove timestamp parsing. See [Oracle source configuration](https://docs.oracle.com/en-us/iaas/log-analytics/doc/create-log-source.html).

An entity is optional for this demo. Leave entity ID/type and additional metadata empty initially. If the tenancy requires a log set, agree one fixed value and configure `oci.la.log.set`.

## 9. Build and install the OCI plugin

The OCI connector is a separate Git checkout from [Kalsaria-C/kafka-connect-oracleobservability](https://github.com/Kalsaria-C/kafka-connect-oracleobservability). Its pin is in `config/oci-upstream.json`; the initial integration uses commit `f2aa2f639fea2fc5a2c3967c1c892cf36d78cd35` on `ckalsari/initial-connector`.

```bash
python3 scripts/oci.py build
python3 scripts/oci.py install --skip-build
python3 scripts/oci.py status
```

`build` clones the checkout when missing, requires a clean checkout at the pinned revision, runs Maven `clean verify` using the configured Java 17, and produces a plugin ZIP. It also creates `config/oci-sink.local.json` from the tracked template if the local file is absent. It never overwrites an existing local OCI config.

If the branch has advanced since the pin, the automatic clone can correctly refuse the newer HEAD. Select the recorded commit before retrying:

```bash
LAB_OCI_REVISION="$(python3 -c 'import json; print(json.load(open("config/oci-upstream.json"))["revision"])')"
git -C connectors/oracle-observability fetch origin "$LAB_OCI_REVISION"
git -C connectors/oracle-observability checkout --detach "$LAB_OCI_REVISION"
python3 scripts/oci.py build
```

First inspect and retain any local source changes; do not discard them to satisfy this check. The wrapper does not patch the upstream Java source.

`install --skip-build` verifies the recorded ZIP hash and revision, safely extracts the distribution with its `lib/` directory, installs it in a separate worker plugin directory, and briefly restarts this lab's worker. FileStream and its offsets remain registered. It checks that both plugins are discoverable and validates the OCI configuration schema. Running `install` without `--skip-build` performs the build first.

Confirm plugin discovery:

```bash
curl --noproxy '*' --fail --silent --show-error \
  http://localhost:8083/connector-plugins |
  python3 -m json.tool
```

The list should include both:

```text
org.apache.kafka.connect.file.FileStreamSinkConnector
com.oracle.observability.kafka.connect.OracleObservabilitySinkConnector
```

The plugin build does not require OCI credentials. Its tests use local HTTP fixtures and ephemeral test keys. `mvn test` alone omits the assembled-plugin verification; use `clean verify` or the wrapper's `build` command.

The Maven helper uses a local dependency cache and can create local Maven proxy settings from `https_proxy`/`HTTPS_PROXY`. That configures dependency downloads, not the OCI SDK's runtime HTTP proxy. No global Java, Maven, or certificate settings are changed.

## 10. Configure and authenticate the OCI path

### 10.1 Set up your OCI API-key profile

Install the OCI CLI if you will use the diagnostic and direct-upload commands. Follow [OCI API signing key setup](https://docs.oracle.com/en-us/iaas/Content/API/Concepts/apisigningkey.htm) and [OCI configuration-file documentation](https://docs.oracle.com/en-us/iaas/Content/API/Concepts/sdkconfig.htm).

An API-key profile has this shape; replace every example with your own values in your OCI configuration file outside this repository:

```ini
[kafka-la-demo]
user=ocid1.user.REPLACE_WITH_YOUR_USER_OCID
tenancy=ocid1.tenancy.REPLACE_WITH_YOUR_TENANCY_OCID
fingerprint=REPLACE_WITH_THE_REGISTERED_PUBLIC_KEY_FINGERPRINT
key_file=/absolute/path/to/your/private-api-key.pem
region=us-phoenix-1
```

Register the corresponding **public** key under the exact IAM user in the profile. The private key stays local. Keep its filesystem permissions restricted, and use the official configuration guidance for an encrypted key. The upstream plugin currently implements API-key `config_file` authentication; it does not implement instance principals, resource principals, or a separate session-token mode.

### 10.2 Edit the local connector configuration

Open `config/oci-sink.local.json`, retaining the wrapper and property names from `config/oci-sink.json`. In particular:

```json
{
  "name": "oci-la-cloud",
  "config": {
    "connector.class": "com.oracle.observability.kafka.connect.OracleObservabilitySinkConnector",
    "tasks.max": "1",
    "topics": "la-hackathon-logs",
    "key.converter": "org.apache.kafka.connect.storage.StringConverter",
    "value.converter": "org.apache.kafka.connect.storage.StringConverter",
    "oci.region": "us-phoenix-1",
    "oci.namespace": "REPLACE_WITH_VERIFIED_NAMESPACE",
    "oci.auth.type": "config_file",
    "oci.config.file": "/absolute/path/to/your/.oci/config",
    "oci.config.profile": "kafka-la-demo",
    "oci.la.log.group.id": "REPLACE_WITH_ACTUAL_LOG_GROUP_OCID",
    "oci.la.log.source.name": "REPLACE_WITH_EXACT_SOURCE_API_NAME",
    "oci.la.log.path": "kafka-hackathon.log",
    "oci.la.batch.max.records": "100",
    "oci.la.batch.max.bytes": "1048576",
    "oci.connect.timeout.ms": "5000",
    "oci.read.timeout.ms": "15000",
    "oci.retry.initial.ms": "1000",
    "oci.retry.max.ms": "30000",
    "oci.put.budget.ms": "60000"
  }
}
```

The source name must match your existing LA source, including any distinction between a display label and API name. `oci.la.log.path` is a logical label; it does not point OCI at a file on your laptop. Do not substitute the local FileStream output path there.

The Java plugin requires an absolute `oci.config.file`. The wrapper expands `~` before submitting it, but an explicit absolute path is easiest to inspect. Do not paste private key contents into this JSON.

### 10.3 Run the local preflight

```bash
python3 scripts/oci.py check
```

`ready: true` means the local required values, profile, and key file are present. **It is not a remote authentication or upload test.** Missing placeholders return exit code 2. The wrapper also protects the file connector name and the separate OCI consumer group.

### 10.4 Load diagnostic variables and check remote authentication

```bash
export LAB_OCI_CONFIG="$(python3 -c 'import json; from pathlib import Path; c=json.load(open("config/oci-sink.local.json"))["config"]; print(Path(c["oci.config.file"]).expanduser().resolve())')"
export LAB_OCI_PROFILE="$(python3 -c 'import json; print(json.load(open("config/oci-sink.local.json"))["config"]["oci.config.profile"])')"
export LAB_OCI_REGION="$(python3 -c 'import json; print(json.load(open("config/oci-sink.local.json"))["config"]["oci.region"])')"
export LAB_OCI_NAMESPACE="$(python3 -c 'import json; print(json.load(open("config/oci-sink.local.json"))["config"]["oci.namespace"])')"
export LAB_OCI_GROUP="$(python3 -c 'import json; print(json.load(open("config/oci-sink.local.json"))["config"]["oci.la.log.group.id"])')"

oci --config-file "$LAB_OCI_CONFIG" \
  --profile "$LAB_OCI_PROFILE" --region "$LAB_OCI_REGION" \
  --max-retries 0 log-analytics log-group get \
  --namespace-name "$LAB_OCI_NAMESPACE" \
  --log-group-id "$LAB_OCI_GROUP"
```

This is a read-only resource check. Success confirms that identity can authenticate and read that group. An upload-only identity may lack read permission; a 403/404 requires checking resource access and does not by itself prove uploads are forbidden. A 401 `NotAuthenticated` means the authentication information is being rejected. Resolve that before treating connector startup as proof of cloud connectivity.

## 11. Prove a direct OCI upload

Do this after the source/parser and authentication are ready. Use the diagnostic variables from section 10 and the runtime variables from section 7. This step sends synthetic events directly through the Log Events API, independently of Kafka and the custom sink.

```bash
LAB_DIRECT_RUN_ID="direct-$(python3 -c 'import uuid; print(uuid.uuid4().hex[:12])')"
LAB_DIRECT_INPUT="$LAB_RUNTIME/runs/$LAB_DIRECT_RUN_ID.jsonl"
LAB_DIRECT_ENVELOPE="$LAB_RUNTIME/runs/$LAB_DIRECT_RUN_ID-envelope.json"

python3 scripts/generate_logs.py --count 10 --run-id "$LAB_DIRECT_RUN_ID" \
  > "$LAB_DIRECT_INPUT"

python3 - "$LAB_DIRECT_INPUT" "$LAB_DIRECT_ENVELOPE" <<'PY'
import json
from pathlib import Path
import sys

source, destination = map(Path, sys.argv[1:])
config = json.loads(Path("config/oci-sink.local.json").read_text())["config"]
records = source.read_text(encoding="utf-8").splitlines()
payload = {"logEvents": [{
    "logSourceName": config["oci.la.log.source.name"],
    "logPath": config.get("oci.la.log.path", "kafka-hackathon.log"),
    "logRecords": records
}]}
destination.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
print("Prepared", len(records), "records in", destination)
PY

oci --config-file "$LAB_OCI_CONFIG" \
  --profile "$LAB_OCI_PROFILE" --region "$LAB_OCI_REGION" \
  --max-retries 0 log-analytics upload upload-log-events-file \
  --file "$LAB_DIRECT_ENVELOPE" \
  --namespace-name "$LAB_OCI_NAMESPACE" \
  --log-group-id "$LAB_OCI_GROUP" \
  --payload-type JSON \
  --content-type application/octet-stream
```

If your tenancy requires a log set, include the same fixed `--log-set` value that you configured as `oci.la.log.set` in the connector.

The envelope contains an array of **strings** in `logRecords`: preserve each original application JSON string and let the envelope serializer encode it once. The log group is a request parameter, not an application-event field.

Use the direct run ID in Log Explorer. Verify all ten unique event IDs, source/group, timestamp, service, level, exact message, and numeric latency. HTTP acceptance is followed by asynchronous parsing and indexing; wait through the observed search delay and use an event-time window covering the run. A successful HTTP response alone is not the parser compatibility gate.

## 12. Enable and test the OCI path

### 12.1 Enable without replacing FileStream

```bash
python3 scripts/oci.py enable
python3 scripts/oci.py status
```

On first creation, the controller creates `oci-la-cloud` in `STOPPED` state, seeds only its own Kafka consumer group to the topic's current end, and resumes it. It therefore starts with fresh traffic instead of silently uploading the existing file-test history. The file connector and its offsets remain intact.

For a new OCI connector where you deliberately want all retained history:

```bash
python3 scripts/oci.py enable --start-at earliest
```

`--start-at` applies only when initializing a group with no committed offsets. Re-enabling an existing connector retains its committed offsets. This option is not an offset reset or replay command. An interrupted first-time creation can be completed while the connector is stopped and its group still has no committed offsets.

### 12.2 Generate ten events over one minute and verify both paths

```bash
python3 scripts/oci.py test \
  --events-per-minute 10 \
  --duration-minutes 1 \
  --verify-file
```

The run ID is printed on stderr. The command streams records while saving their exact input, waits for the OCI group to commit through the produced Kafka range, and compares FileStream output when `--verify-file` is present.

For the unchanged upstream connector, successful `put()` calls require accepted uploads before offsets can commit. The OCI report therefore treats committed offsets as an API-acceptance check. It explicitly reports `log_analytics_indexing_verified: false`. Confirm the run's unique IDs and parsed values in Log Explorer before claiming end-to-end LA success.

Without `--verify-file`, the OCI test does not require FileStream to be running:

```bash
python3 scripts/oci.py test --count 100
```

The existing `lab.py test` always verifies the file path. If OCI is also enabled, it will independently consume that traffic, but a passing file report does not verify OCI.

### 12.3 Batch and byte-cap demonstrations

```bash
python3 scripts/oci.py test --count 250 --verify-file
python3 scripts/oci.py test --count 250 --message-bytes 16000 --verify-file
```

Default request caps are 100 records and 1,048,576 complete serialized UTF-8 bytes. A record is never split. Envelope overhead and escaping count toward the cap. The Log Events service documents a larger JSON limit; this plugin deliberately keeps its own cap at 1 MiB.

These are upper limits, not a promise of exactly 100 records per request. Poll boundaries and Kafka partitions can produce smaller requests. The task flushes each `put()` tail immediately; it does not wait across successful calls for a timer or a full batch. Low-rate traffic can therefore produce single-record uploads.

An individually oversized record fails visibly. Null, empty, non-string, or invalid-Unicode values are rejected by the connector. It preserves application strings rather than validating their JSON fields; field extraction is LA's responsibility.

## 13. Select file OCI or both

### File only

```bash
python3 scripts/oci.py disable
python3 scripts/lab.py start
python3 scripts/lab.py test --count 10
```

### Both paths

```bash
python3 scripts/lab.py start
python3 scripts/oci.py enable
python3 scripts/oci.py test --count 10 --verify-file
```

### OCI only

```bash
python3 scripts/oci.py enable
curl --noproxy '*' --fail -X PUT \
  http://localhost:8083/connectors/oci-la-hackathon/stop
python3 scripts/oci.py test --count 10
```

To restore the file connector:

```bash
python3 scripts/lab.py start
```

`oci.py disable` stops only OCI, retaining its configuration and offsets. Disabling a sink does not discard Kafka messages. When resumed, an existing group processes its retained backlog, subject to Kafka retention. A stopped file sink can therefore append older retained records when you enable it again.

## 14. Recovery testing

The combined file recovery test keeps Kafka running, stops this lab's Connect worker, generates/publishes a fresh run while the worker is down, restarts it with the same state, and reconciles the file output:

```bash
python3 scripts/lab.py recovery --count 100
python3 scripts/lab.py recovery --count 20 --duration-seconds 10
```

This stops the shared worker, so an enabled OCI path also pauses. The recovery report checks FileStream. For OCI recovery, inspect its committed offsets, acceptance diagnostics, and the same run's IDs in Log Explorer after it catches up.

Retain the Docker volume, connector names, worker group, internal topics, and application topic across a recovery run. Kafka is the durable backlog. The default broker retention is seven days; recovery cannot restore records that Kafka has already removed.

The delivery contract is at least once to API acceptance, subject to retention and successful recovery. A restart can replay accepted records whose offsets had not yet committed. Count unique event IDs and document duplicates; do not claim exactly-once indexing.

The upstream tests separately simulate transient 503 and other status classes, stable retry payloads/tokens, chunk progress, and withheld offsets. A stop/restart demonstration does not itself prove API throttling behavior.

## 15. Status logs offsets and evidence

### 15.1 Status and plugin discovery

```bash
python3 scripts/lab.py status
python3 scripts/oci.py status
docker compose ps

curl --noproxy '*' --fail --silent --show-error \
  http://localhost:8083/connectors |
  python3 -m json.tool

curl --noproxy '*' --fail --silent --show-error \
  http://localhost:8083/connectors/oci-la-cloud/status |
  python3 -m json.tool
```

Inspect both the connector state and every task state. The connector can show `RUNNING` while a task is `FAILED`. A task can remain `RUNNING` while retrying uploads that have not been accepted.

### 15.2 Logs

Load `LAB_RUNTIME` as in section 7, then:

```bash
tail -f "$LAB_RUNTIME/logs/connect.log"
tail -n 100 "$LAB_RUNTIME/logs/connect-console.log"
docker logs --tail 100 shreyam-hackathon-kafka

grep -E 'Log Events accepted|transient failure|ERROR|WARN|Caused by:' \
  "$LAB_RUNTIME/logs/connect.log" |
  tail -n 80
```

Connect also keeps rolled log files in that directory. Accepted-upload diagnostics include record counts, bytes, timing, and OCI request IDs. The upstream uploader intentionally omits application payloads and sanitizes service failures; a generic error can hide the underlying client exception.

### 15.3 Consumer offsets and lag

```bash
"$LAB_KAFKA_DIR/bin/kafka-consumer-groups.sh" \
  --bootstrap-server localhost:19092 \
  --describe --group connect-oci-la-hackathon

"$LAB_KAFKA_DIR/bin/kafka-consumer-groups.sh" \
  --bootstrap-server localhost:19092 \
  --describe --group connect-oci-la-cloud

curl --noproxy '*' --fail --silent --show-error \
  http://localhost:8083/connectors/oci-la-cloud/offsets |
  python3 -m json.tool
```

Offsets refer to the next position after committed processing. Lag is the gap to Kafka's log end. A nonzero OCI lag with a caught-up file group isolates the cloud path. Kafka offsets do not prove that the LA parser indexed the expected fields.

### 15.4 Evidence locations

| Artifact | Location |
|---|---|
| Generated source fixtures | `<runtime>/runs/<run-id>.jsonl` |
| Live accumulated file output | `data/filestream-output.jsonl` by default |
| Completed file/OCI test report | `evidence/<run-id>.json` |
| Retained successful-run fixture | `evidence/<run-id>.jsonl` |
| Upstream build test counts / ZIP hash | `evidence/oci-build.json` |
| Installed plugin location / discovery result | `evidence/oci-installation.json` |
| Kafka durable data and internal topics | Docker volume `shreyam-hackathon-kafka-data` |

On a timeout or exception before the report is written, the final evidence JSON may be absent. The generated fixture is still in `<runtime>/runs/`. Preserve the run ID, logs, and offsets. For example, after a failed cloud run:

```bash
# Replace this with the run ID printed by your failed command.
LAB_FAILED_RUN_ID="oci-REPLACE_WITH_YOUR_RUN_ID"
python3 scripts/verify_events.py \
  "$LAB_RUNTIME/runs/$LAB_FAILED_RUN_ID.jsonl" \
  "$LAB_OUTPUT"
```

This independently confirms whether FileStream received those events; it does not retry or regenerate them.

## 16. Troubleshooting

### Docker daemon unavailable or socket permission denied

Check `docker version`, `docker context show`, and `docker context ls`. Start the intended engine and use a context your shell can access. With Rancher Desktop, ensure its Docker-compatible engine is enabled. Do not assume that having the Docker CLI installed means the server is running.

### Image pull access denied for shreyam-hackathon-kafka

This is the project's local image tag. Run the broker image build in section 4.5 before `lab.py start`. It is not a Docker Hub login issue for a published project image.

### Docker pull certificate error

Configure approved Docker CA trust or an approved registry mirror, or use a trusted cached Java base image. The custom build still needs the official, verified Kafka binary. Keep TLS verification enabled. A Maven proxy setting does not repair Docker daemon certificate trust.

### Port 19092 or 8083 is already occupied

On macOS, inspect listeners with:

```bash
lsof -nP -iTCP:19092 -iTCP:8083 -sTCP:LISTEN
```

The wrapper refuses to stop an unrelated worker on port 8083. Ports are currently constants, not CLI options: changing them requires consistent edits to `scripts/lab.py`, Compose, broker advertised listeners, and regenerated worker properties. Do not change only a published port and leave the advertised address pointing at the old one.

The fixed project/container/volume names and ports support one default instance per Docker engine. Two copied folders are not automatically isolated labs.

### Java version mismatch

Use Java 17 for the host and rerun `configure` with its actual JDK home only after stopping the old worker. Check `mvn -version` in the selected environment. Maven or a Kafka tool can otherwise choose a newer system Java than you intended. The broker's Java 21 base is separate from host Java selection.

### FileStream plugin missing

`configure` must point to the Kafka 4.1.2 **binary** distribution containing `libs/connect-file-4.1.2.jar`. The wrapper copies it into `<runtime>/plugins/filestream/`. Verify `plugin.path` in the worker properties and restart the worker after changing installed JARs. An existing worker does not automatically discover new plugins.

### OCI plugin missing or NoClassDefFoundError

Run the full upstream build, then install its ZIP with dependencies, preserving `lib/`. Do not install only the project JAR or mix the OCI JARs into the FileStream directory. Check the actual worker plugin path, installation manifest, and `/connector-plugins` after restart.

### Maven cannot download dependencies

Check repository connectivity and the shell's `https_proxy`/`HTTPS_PROXY`. The helper creates local proxy settings for Maven downloads and uses the configured Java 17. Correct approved CA trust when certificates fail; do not disable TLS checks. If the clone is dirty or its HEAD differs from the pin, inspect the checkout instead of bypassing the revision check.

### Connector name collision

The local OCI JSON must use `"name": "oci-la-cloud"`. `oci-la-hackathon` already belongs to FileStream. The upstream's standalone example uses a different ownership assumption; do not paste its wrapper name unchanged into this two-path project.

### oci.config.file must be an absolute path

Use a real absolute profile-file path in `oci-sink.local.json`. The current wrapper expands `~` before submission, but a direct REST configuration update bypasses that normalization. After changing the local connector configuration, run `oci.py check` and `oci.py enable` again.

### Task remains FAILED after correcting settings

`resume` alone does not recreate a failed task. The current `enable` controller stops and resumes the OCI connector when its task failed or its submitted config changed, preserving offsets. If you changed the profile/key file on disk while the task is still `RUNNING` and retrying, explicitly restart it to reload credentials:

```bash
curl --noproxy '*' --fail -X POST \
  'http://localhost:8083/connectors/oci-la-cloud/restart?includeTasks=true&onlyFailed=false'
python3 scripts/oci.py status
```

### 401 NotAuthenticated

Use the read-only CLI check in section 10 to separate local file presence from service authentication. Verify the exact user OCID, tenancy OCID, registered public-key fingerprint, matching private key, and system clock. A locally matching fingerprint does not prove that OCI has registered that key under that user.

To compute the public fingerprint from your local RSA key for comparison with the Console:

```bash
openssl rsa -pubout -outform DER \
  -in /absolute/path/to/your/private-api-key.pem |
  openssl md5 -c
```

The command prints a fingerprint, not the private key. Correct the profile or remote key registration as appropriate, restart the OCI connector, and let the existing Kafka backlog retry. Increasing the test timeout does not fix rejected authentication. See [OCI API error definitions](https://docs.oracle.com/en-us/iaas/Content/API/References/apierrors.htm).

### status=-1 with opcRequestId=null

`-1` is an SDK/client failure classification, not an OCI HTTP status. Investigate the underlying exception, DNS, connection/TLS setup, and authentication. Do not conclude that OCI returned a transient 5xx. In this lab's recorded failure, the upload log hid the cause while independent Java SDK and OCI CLI reads both returned 401 `NotAuthenticated`.

### OCI did not commit the generated Kafka range within 120 seconds

The wrapper generated and published its input but did not observe the OCI group commit through the target range. Inspect task state, retry/acceptance logs, and both consumer groups. The 120-second acceptance wait starts after publishing completes, in addition to the requested generation duration.

The current wrapper has no `--timeout` flag for this wait. Resolve the underlying failure first. Do not reset offsets or regenerate the same run merely to hide the timeout. The original records remain recoverable while retained by Kafka. The file verifier can independently check them using the source fixture.

### HTTP 400 403 404 or 413

- 400: check the request/source contract and destination values.
- 403/404: check the requested region, resource existence, namespace, compartment policies, and operation-specific access. OCI can conceal unauthorized resources behind a not-found response.
- 413: inspect the complete serialized payload size and single-record size. Keep the plugin's byte cap at or below its supported 1 MiB limit.

The upstream task treats permanent HTTP failures as task failures. Timeouts, transport failures, 408, 429, and selected 5xx responses are retriable. Observe the actual retry behavior in logs; a `RUNNING` task with repeated errors is not a successful upload.

### API accepted but logs are absent or fields are wrong

Check the Log Explorer event-time window and processing delay, then source/group selection, parser association, exact source API name, JSON mappings, timestamp parsing, and run ID. Confirm the direct-upload gate separately. A bad parser can fail after an accepted API request; Kafka commits cannot validate indexing.

### File verification shows missing changed or malformed records

Check the connector's actual `file` setting, task state, fixture run ID, and whether it has caught up. Keep output as JSONL; do not insert headings or manually edit event values. The comparison can report old malformed lines elsewhere in the accumulated file. Use the saved source fixture rather than regenerating the same IDs.

### Moving the folder or changing the runtime directory

Stop Connect using its existing configuration before moving or reconfiguring the project. The PID ownership guard identifies the worker through its config path. Regenerate local paths at the new location and intentionally preserve the Kafka volume if recovery is desired. Do not copy another user's `runtime.local.json` to work around a path mismatch.

## 17. Stop resume and reset

### Stop one path

```bash
# OCI only; keeps its configuration and offsets.
python3 scripts/oci.py disable

# FileStream only; keeps its configuration and offsets.
curl --noproxy '*' --fail -X PUT \
  http://localhost:8083/connectors/oci-la-hackathon/stop
```

### Stop the complete lab without deleting data

```bash
python3 scripts/lab.py stop-connect
docker compose stop
```

This stops the shared worker and broker. The Docker volume, output file, configuration, and fixtures remain.

### Resume later

```bash
python3 scripts/lab.py start
python3 scripts/oci.py status
```

`start` starts/resumes FileStream. An OCI connector that was already registered as running is restored with the worker. One explicitly stopped with `oci.py disable` stays stopped until `oci.py enable`. `docker compose start` alone starts the broker, not the host Connect process.

### Destructive reset of a disposable local lab

Only do this when you intentionally want to delete this lab's Kafka records, connector registrations, and consumer offsets. It is not a recovery test or an authentication fix.

```bash
python3 scripts/lab.py stop-connect
docker compose down --volumes
```

The repo's JSONL output and evidence remain. If you want a separate file for the next clean run, archive the old one while the worker is stopped:

```bash
if [ -f data/filestream-output.jsonl ]; then
  mv data/filestream-output.jsonl \
    "data/filestream-output.before-reset-$(date -u +%Y%m%dT%H%M%SZ).jsonl"
fi
python3 scripts/lab.py start
```

Re-enable OCI when ready. Deleting or truncating the output file alone does not reset Kafka offsets or replay old messages. Do not delete the volume when trying to demonstrate durable backlog recovery.

## 18. Command reference

All commands below run from the project root. Use `--help` after a script or subcommand for its parser's full options.

| Command | Effect |
|---|---|
| `python3 scripts/lab.py configure --kafka-home PATH --java-home PATH --runtime PATH` | Creates this machine's local worker/tool configuration |
| `python3 scripts/build_broker.py --base-image IMAGE` | Builds the local amd64 Kafka image |
| `python3 scripts/lab.py start` | Starts broker/worker and registers or resumes the file sink |
| `python3 scripts/lab.py start-connect` | Starts the host worker; requires existing broker and file registration |
| `python3 scripts/lab.py stop-connect` | Gracefully stops only this lab's owned worker |
| `python3 scripts/lab.py status` | Shows worker and file-connector state |
| `python3 scripts/lab.py test TIMING_FLAGS` | Publishes and reconciles the file path |
| `python3 scripts/lab.py recovery TIMING_FLAGS` | Stops worker, publishes backlog, restarts and verifies FileStream |
| `python3 scripts/generate_logs.py TIMING_FLAGS` | Writes generated JSONL to stdout only |
| `python3 scripts/verify_events.py EXPECTED_JSONL ACTUAL_JSONL` | Compares all fields and unique IDs for the expected runs |
| `python3 scripts/oci.py build` | Verifies and packages the pinned upstream source |
| `python3 scripts/oci.py install` | Builds, installs, and reloads the worker |
| `python3 scripts/oci.py install --skip-build` | Installs the existing hash-checked build |
| `python3 scripts/oci.py check` | Checks local OCI values/profile/key presence; no remote authentication test |
| `python3 scripts/oci.py enable` | Creates/resumes OCI independently, starting a new group at latest |
| `python3 scripts/oci.py enable --start-at earliest` | Initializes a new/empty OCI group at the earliest retained record |
| `python3 scripts/oci.py disable` | Stops only OCI and preserves its offsets |
| `python3 scripts/oci.py status` | Shows both paths and OCI plugin availability |
| `python3 scripts/oci.py test TIMING_FLAGS` | Generates traffic and verifies OCI acceptance via committed offsets |
| `python3 scripts/oci.py test TIMING_FLAGS --verify-file` | Also reconciles the local file output |

Shared generation flags:

| Flag | Meaning |
|---|---|
| `--count N` | Total number of events |
| `--events-per-minute RATE` | Positive target rate |
| `--duration-minutes MINUTES` | Positive duration, including fractions |
| `--duration-seconds SECONDS` | Alternative positive duration unit |
| `--interval-ms MS` | Legacy interval for count-only pacing |
| `--run-id ID` | Explicit fresh ID; controllers accept letters/numbers, hyphens, and underscores |
| `--mode normal` | Default deterministic application traffic |
| `--mode error-spike` | Higher ERROR frequency |
| `--mode edge-cases` | Special-character messages |
| `--message-bytes N` | Minimum message character padding |

For a standalone generator, count zero is allowed and emits no events. The test controllers require at least one event. Keep run IDs unique; the controllers refuse to overwrite an existing fixture.

## 19. Share the project with another person

Share the wrapper source and public templates, not a copy of your running state. Private profiles and signing keys stay outside the project. The following creates a source archive from an explicit allowlist; it excludes local configs, logs, output/evidence, dependencies, and the upstream checkout:

```bash
tar --exclude='__pycache__' --exclude='*.pyc' \
  -czf ../kafka-la-lab-source.tgz \
  README.md NOTICE .gitignore Dockerfile compose.yaml \
  scripts tests \
  config/server.properties config/oci-sink.json config/oci-upstream.json \
  connectors/README.md
```

The recipient should create a new project directory, extract the archive there, and follow section 4. That setup creates `evidence/`, and `configure` creates `data/`; a new recipient has no author-specific OCI settings or evidence. They must use their own OCI profile and resources.

If you publish the wrapper in Git, retain the existing ignore rules. The nested `connectors/oracle-observability/` checkout has its own Git origin and is intentionally ignored by the parent. The tracked pin and build command reconstruct it. Do not publish an actual private key, an OCI profile, or a local config containing your environment details as a sample.

## 20. Update the upstream connector

The current source URL, branch, and exact commit are in `config/oci-upstream.json`. Inspect local changes and upstream changes before upgrading:

```bash
git -C connectors/oracle-observability status
git -C connectors/oracle-observability remote -v
git -C connectors/oracle-observability fetch origin
git -C connectors/oracle-observability log --oneline --max-count=10 origin/ckalsari/initial-connector
```

Choose the intended commit, check out that commit, and update the `revision` field to the exact same 40-character hash. Keep or commit local changes intentionally; the build refuses a dirty or mismatched checkout. If the upstream configuration interface changes, review the wrapper's class name, template, validation, and acceptance assumptions too.

Then rebuild, reinstall, and run the local/file regression checks:

```bash
python3 scripts/oci.py build
python3 scripts/oci.py install --skip-build
python3 -m unittest discover -s tests -v
python3 scripts/lab.py test --count 20 --mode edge-cases
python3 scripts/oci.py status
```

Run the OCI acceptance/search test only once the real environment is authenticated and ready. Installation retains Kafka state, but a worker restart can replay uncommitted records, so reconcile IDs afterward.

## 21. Validation history and remaining limits

Recorded on 27 October 2026 for the original environment:

| Area | Recorded result |
|---|---|
| Initial FileStream counts | 1, 100, and 1,000 event runs passed |
| File content and outage checks | Special characters, error spike, and 100-event stop/restart backlog passed |
| Initial six-run reconciliation | 1,401 unique events; no missing, changed, or duplicate records observed |
| Timed streaming | 20 events at 120/minute for 10 seconds; first file arrival before generation finished |
| Post-integration file regression | 20 fresh special-character events arrived unchanged |
| Local Python tests | 18 pacing/configuration/controller checks passed |
| Upstream Java build | 43 checks passed, including local signed HTTP and packaged-plugin initialization |
| Plugin discovery and offset API | Both plugins discovered; a temporary stopped OCI connector's initial offsets were verified and cleaned up without starting a cloud task |
| OCI task initialization | Absolute config-path issue corrected; the task initialized |
| Live OCI ingestion | **Not validated successfully:** the configured identity returned 401 `NotAuthenticated` in independent Java SDK and CLI reads |

Historical offsets and run counts in evidence are snapshots, not current totals. Do not present a `RUNNING` state, build success, or file-only test as proof of LA ingestion.

The fresh-machine instructions use the actual current CLI and configuration contract. The generic public-base-image path and additional operating systems have not been exercised as a clean-machine installation on this laptop. The original image used a verified Kafka 4.1.2 binary with a cached Java 21 base; host Connect used Java 17.

Current scope: config-file API-key authentication, synchronous uploads, one fixed destination/source, one demo topic partition, and no DLQ, dashboards, saved searches, compression, timer-based buffering across successful puts, or production certification. Source fixtures and reports aid reconciliation; the file output is not the connector's durable spool.

The generator began with the upstream file at the pinned commit and retains its attribution. This project adds WARN/edge-case traffic, pacing, orchestration, evidence comparison, and separate sink controls. The upstream Java source remains unchanged. See `NOTICE` and the upstream dependency licenses.

## 22. References

- [Apache Kafka 4.1 quickstart](https://kafka.apache.org/41/getting-started/quickstart/)
- [Apache Kafka Connect 4.1 user guide and REST API](https://kafka.apache.org/41/kafka-connect/user-guide/)
- [Apache Kafka 4.1 Docker guide](https://kafka.apache.org/41/getting-started/docker/)
- [Pinned Kafka 4.1.2 distribution archive](https://archive.apache.org/dist/kafka/4.1.2/)
- [Upstream OCI connector source](https://github.com/Kalsaria-C/kafka-connect-oracleobservability)
- [OCI Log Events upload format and prerequisites](https://docs.oracle.com/en-us/iaas/log-analytics/doc/upload-event-logs-using-logevents-api.html)
- [OCI Log Analytics source configuration](https://docs.oracle.com/en-us/iaas/log-analytics/doc/create-log-source.html)
- [OCI API signing keys](https://docs.oracle.com/en-us/iaas/Content/API/Concepts/apisigningkey.htm)
- [OCI SDK and CLI profile configuration](https://docs.oracle.com/en-us/iaas/Content/API/Concepts/sdkconfig.htm)
- [OCI CLI Log Events upload command](https://docs.oracle.com/en-us/iaas/tools/oci-cli/latest/oci_cli_docs/cmdref/log-analytics/upload/upload-log-events-file.html)
- [OCI API errors](https://docs.oracle.com/en-us/iaas/Content/API/References/apierrors.htm)
