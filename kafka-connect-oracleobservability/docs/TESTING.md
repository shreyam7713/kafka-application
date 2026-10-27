# Validation results and remaining acceptance

## Completed local checks

Command: `mvn clean verify`, using JDK 17.0.8 and Maven 3.9.9.

| Suite | Checks | Result |
| --- | ---: | --- |
| LogEventsChunkerTest | 14 | Passed |
| OracleObservabilitySinkTaskTest | 8 | Passed |
| OracleObservabilitySinkConnectorTest | 3 | Passed |
| OciSdkLogEventsUploaderTest | 16 | Passed |
| UploadResponseDiagnosticsTest | 5 | Passed |
| SdkHttpIntegrationTest | 1 | Passed |
| PackagedPluginIT | 1 | Passed |
| Total | 48 | Zero failures, errors or skipped checks |

- Count boundaries: 0, 1, 99, 100, 101 and 250 records.
- Exact payload-byte fit and one-byte overflow, including commas and envelope overhead.
- JSON escaping, UTF-8 Unicode, optional metadata fields, immutable payloads and original coordinates after SMT routing.
- Explicit rejection of null, non-string, empty, invalid-Unicode, out-of-order and oversized inputs.
- No commit while a failed put remains pending; no re-upload of earlier successful chunks during an in-process retry.
- Time-budget yielding, stable failed request identity, empty calls, fatal failures and task cleanup.
- Transient and permanent HTTP status classification and sanitized service error messages.
- Real SDK HTTP calls with a test-only ephemeral signing key and local server; no OCI calls or real credentials.
- Success/failure response header interception preserves SDK error handling and retry behavior; a 503 exposes its request ID and Retry-After while cookies are redacted.
- Diagnostic field/header bounds, common credential redaction, single-line output, disabled DEBUG behavior and transport-error context.
- ZIP inspection and isolated plugin discovery/SDK initialization, with Kafka and SLF4J supplied by the worker parent.

The generator also passed a standalone 250-event check with unique IDs, current UTC timestamps
and exact 16000-byte synthetic message bodies. No generator output was committed to the project.

## What remains live

These checks require Shreyam's Kafka/Connect environment and the actual OCI namespace, log group and profile:

1. Kafka 4.1.2 worker discovery and task scheduling with this plugin distribution.
2. FileStream baseline and configuration switch under the same connector name.
3. Direct Log Events upload with your File source and JSON parser in the destination region.
4. Actual record parsing, event timestamps, searchable fields and indexing delay.
5. Count/byte chunking and restart recovery with reconciliation of unique event IDs in LA.

The local tests prove connector logic, SDK wire behavior and packaged dependency loading.
They do not certify broker rebalance behavior or end-to-end LA ingestion; keep the live checks explicit.
