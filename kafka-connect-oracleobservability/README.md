# Oracle Observability Kafka Sink Connector

A Java 17 Kafka Connect sink that sends string log records to OCI Log Analytics through `UploadLogEventsFile`.
This initial hackathon implementation includes exact-byte JSON chunking, count-based batching,
OCI config-file authentication, resumable delivery within a failed `put()`, and a plugin ZIP.

## Build and verification

```sh
mvn clean verify
```

The build runs unit tests, a local real-SDK HTTP test, and an isolated-classloader test of the assembled ZIP.
No OCI credentials or Kafka broker are needed for these checks. `mvn test` runs the pre-packaging tests only.

Output: `target/kafka-connect-oracleobservability-0.1.0-SNAPSHOT-plugin.zip`.
Extract it beneath the worker's `plugin.path`; preserve the distribution's `lib/` directory and all runtime JARs.
Install the same distribution on every worker and restart workers before registering the connector.
Kafka Connect and SLF4J APIs come from the worker and are deliberately excluded from the ZIP.

## Scope

- One fixed region, namespace, log group and source per connector; multiple tasks are supported.
- Use `StringConverter`. JSON application events are carried as strings inside `logRecords`.
- A request contains one source/entity/path/timezone group. Original Kafka partitions are kept separate.
- Batch within each `put()` call; flush its tail immediately. Timer-based buffering across successful calls is deferred.
- Default request limits: 100 records and 1,048,576 complete serialized UTF-8 bytes, whichever comes first.
- Preserve whole records. Null, empty, non-string, invalid-Unicode and individually oversized records fail explicitly.
- Delivery is at least once to API acceptance, subject to Kafka retention and successful recovery. Search visibility is asynchronous.
- Saved searches, dashboards, DLQ, compression, per-record routing and principal authentication are deferred.

## Configure

Copy `config/oci-sink.json` to `config/oci-sink.local.json` and replace all placeholders.
Verify the source actually exists and its exact API name matches the config. Do not commit credentials or private keys.

| Setting | Required or default |
| --- | --- |
| `oci.region` | Required; demo `us-phoenix-1` |
| `oci.namespace` | Required verified namespace |
| `oci.config.file` | Required absolute profile-file path |
| `oci.config.profile` | `DEFAULT` |
| `oci.auth.type` | `config_file` only |
| `oci.la.log.group.id` | Required actual log group OCID |
| `oci.la.log.source.name` | Required exact source name |
| `oci.la.log.path` | Optional logical log path |
| `oci.la.entity.id`, `oci.la.entity.type` | Optional; type requires ID |
| `oci.la.log.set` | Optional fixed request-level log set; configure if the tenancy requires it |
| `oci.la.timezone` | Optional event-group timezone; prefer timestamps with explicit UTC `Z` |
| `oci.la.batch.max.records` | 100; range 1–12,000 |
| `oci.la.batch.max.bytes` | 1,048,576; range 128–1,048,576 |
| `oci.connect.timeout.ms`, `oci.read.timeout.ms` | 5,000 and 15,000 |
| `oci.retry.initial.ms`, `oci.retry.max.ms` | 1,000 and 30,000; bounded jittered Connect delay |
| `oci.put.budget.ms` | 60,000; at least the sum of connect/read timeouts |

Use a consumer poll interval comfortably above the processing budget plus one HTTP attempt.
`config/connect-distributed.properties` provides local-demo defaults of 500 records per poll and a five-minute poll interval.
The processing budget is checked between requests, so an in-flight request can extend beyond it.
Tune broker fetch/message limits and poll sizes for memory use; an entire poll's prepared requests are held until completion.

## Shreyam's workflow

Follow [the local and LA runbook](docs/RUNBOOK.md):

1. Verify Kafka → FileStream with the supplied generator.
2. Create/verify the LA source and JSON parser; prove one direct API upload first.
3. Install this plugin and switch the existing connector configuration.
4. Generate fresh run IDs; verify count batching, byte chunking, parsing and restart recovery.

## Reliability

SDK automatic retries are disabled. The task schedules Connect retries for timeouts/transport failures,
408, 429, 500, 502, 503 and 504. Other HTTP failures fail the task for investigation.
Persistent authentication failures require correcting credentials and restarting the task.
The same failed request retains its payload and token during in-process retries; successful earlier chunks are skipped.
`preCommit()` withholds all offsets while a `put()` is unresolved. Successful calls retain no pending uploads.
Revocation or restart discards memory and relies on uncommitted Kafka replay, which can duplicate accepted logs.
No durable deduplication or exactly-once indexing is claimed.

Operational logs contain record/byte counts, timing, retry delay and OCI request IDs. Log content and credentials are omitted.
API acceptance cannot report later parser/indexing failures; inspect the run in Log Explorer.

## Implementation status

See [PLAN.md](docs/PLAN.md) for completed stages, validation evidence and the live handoff.

## References

- [ClickHouse sink design](https://github.com/ClickHouse/clickhouse-kafka-connect/blob/main/docs/DESIGN.md): task/delivery separation and offset safety.
- [Azure Data Explorer sink](https://github.com/Azure/kafka-sink-azure-kusto): SDK/configuration and test structure.
- [Oracle Fluentd plugin](https://github.com/oracle-quickstart/oci-logan-fluentd-output-plugin): Log Events grouping and source/entity/path concepts.
- LA Fluent Bit plugin in the existing local third-party-plugin repository: record mapping reference.
- [Oracle Observability OTel exporter](https://github.com/oracle-samples/otel-collector-exporter-oracleobservability): sizing/chunking structure. It uses `UploadOtlpLogs`; this project uses Log Events and its own envelope/limits.
- [Log Events API requirements](https://docs.oracle.com/en-us/iaas/log-analytics/doc/upload-event-logs-using-logevents-api.html): public 2 MB JSON limit; this implementation caps requests at 1 MiB.

New code was generated by Codex for Chintan Kalsaria and requires review. Reference implementations were studied;
this project does not include their source files. Third-party libraries retain their licenses.
