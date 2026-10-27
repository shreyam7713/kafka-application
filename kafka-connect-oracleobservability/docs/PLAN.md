# Implementation plan and progress

## Completed implementation stages

1. Standalone Maven/Java 17 project with the oracleobservability connector identity.
2. Configuration validation, Connect connector/task entry points and service discovery descriptor.
3. Log Events envelope construction with string records and fixed source/path/entity/timezone.
4. Count-based batching and complete serialized UTF-8 byte chunking, preserving whole records.
5. Public OCI SDK config-file authentication and UploadLogEventsFile transport; SDK retries disabled.
6. Resumable failed put calls with stable request bytes/tokens, bounded retry scheduling and offset withholding.
7. Unit and local HTTP tests for payloads, retries, SDK signing, lifecycle and safe commit behavior.

## Packaging and handoff

8. **Completed:** build the installable ZIP and verify its isolated plugin classloader, SDK provider and startup without external calls.
9. **Next, Shreyam:** follow RUNBOOK.md to prove Kafka to FileStream and validate the actual LA source/parser with direct upload.
10. **Pending live setup:** switch the existing connector config, produce fresh run IDs and reconcile LA records.
11. **Pending live setup:** demonstrate count chunking, byte chunking, stop/start backlog recovery and an error-spike run.

## Verification evidence

`mvn clean verify` passed on JDK 17: **47 pre-packaging tests plus 1 packaged-plugin integration test**,
zero failures and zero skipped tests. The local HTTP test uses the actual SDK, Jersey HTTP provider and signer,
then responds 503 before success to verify retry ownership, exact request replay, chunk progress and redacted header diagnostics.
Upload diagnostics cover success/failure response headers, sanitized SDK error messages and chunk timing at DEBUG level.
The packaged-plugin test starts the SDK from ZIP dependencies with only Kafka/SLF4J APIs supplied by its parent classloader.
The generator was separately checked with 250 JSON events, unique IDs, UTC timestamps and 16000-byte messages.
See TESTING.md for the test inventory and remaining live acceptance checks.

## Deferred

Destination identifiers and environment-specific source/parser names belong in Git-ignored local configuration.
The shared configuration uses placeholders and example resource names.
Live OCI/Kafka acceptance still requires runtime credentials, parser preview and a successful indexed upload.
No live OCI upload was performed by the local build.
Principal auth, DLQ, async delivery, timer batching, multiple destination routing, compression, dashboards and saved searches are future work.

## Design choices

- Batch only within put; no buffer survives a succescresful call.
- Prepare each partition separately and retain original coordinates across SMTs.
- Refuse malformed/oversized records rather than truncating or dropping them.
- A failed chunk retains its request identity while retried in the same process.
- Kafka is the durable backlog; restart/rebalance can replay accepted but uncommitted logs.
- API acceptance is the offset boundary; downstream parser/indexing failures remain separately observable in LA.
