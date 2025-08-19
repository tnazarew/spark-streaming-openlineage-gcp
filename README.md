# Spark Streaming OpenLineage (GCP) – Scenarios

This project contains Spark Streaming scenarios that emit OpenLineage events. Events are written to both a local file (under `events/`) and to GCP Lineage (when configured).

Important: Do not open or parse the large JSON event files in `events/` directly here—they can be large. Use external tools or tail small excerpts if needed.

## Prerequisites
- Java 8 toolchain (project is configured to compile with Java 8)
- Gradle wrapper (included)
- A valid GCP service account JSON key file if you want to send to GCP Lineage

## Configure properties
All configuration is read from a single file on the test classpath:

`src/test/resources/openlineage-gcp.properties`

Fill the required keys with non-empty values:

```
ol.gcp.projectId=your-gcp-project
ol.gcp.location=your-region
ol.gcp.credentialsFile=/absolute/path/to/service-account.json
```

Notes:
- The `credentialsFile` path must be readable by the test runner.
- If you only want to generate local files under `events/` and not send to GCP, keep valid values anyway; the transport is configured, but you can ignore the GCP output.

## Running scenarios
There are two main streaming scenarios implemented as tests:
- Stream to Stream Enrichment: `StreamToStreamEnrichmentTest`
- Stream to Batch Enrichment: `StreamToBatchEnrichmentTest`

Build and compile tests:

```bash
./gradlew testClasses
```

Run all tests (executes both scenarios):

```bash
./gradlew test
```

Run a single scenario:

```bash
# Stream to Stream Enrichment only
./gradlew test --tests io.openlineage.proto.StreamToStreamEnrichmentTest

# Stream to Batch Enrichment only
./gradlew test --tests io.openlineage.proto.StreamToBatchEnrichmentTest
```

## Outputs
- Local OpenLineage events: written to `events/<TestName>.json`, for example:
  - `events/StreamToStreamEnrichmentTest.json`
  - `events/StreamToBatchEnrichmentTest.json`
- GCP Lineage: if credentials and project/location are set, events are also sent asynchronously to GCP.

Tip: Avoid opening the large JSON files directly. If you need to inspect them, use tools like `jq` or `head/tail` from a terminal.

## Troubleshooting
- Missing or empty config error: ensure `openlineage-gcp.properties` is present and all three keys have non-empty values.
- Credentials issues: confirm the path in `ol.gcp.credentialsFile` is correct and readable by your user.
- Port conflicts or container startup issues: tests use Testcontainers for Kafka; retry after freeing resources.

