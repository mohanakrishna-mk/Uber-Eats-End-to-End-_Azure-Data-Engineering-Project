# 08 – ADF Pipeline Patterns, Dynamic Expressions and Debugging

This companion guide explains the ADF building blocks used across the SQL, CSV and API pipelines.

## 1. Lookup → ForEach → Copy

Typical metadata-driven flow:

```text
Lookup active metadata
       |
       v
ForEach metadata row
       |
       v
Copy source object to ADLS
       |
       v
Log success / failure
```

Lookup:
- Query active metadata.
- Set **First row only** to false when processing multiple rows.
- Confirm output contains `value` array.

ForEach:
- Items: `@activity('Lookup_Metadata').output.value`
- Start with sequential execution.
- Increase batch count only after testing source limits and sink throughput.

Inside Copy:
- Map the current metadata row to dataset parameters.
- Verify dynamic content names match the Lookup query aliases.

## 2. Useful expression patterns

These are generic ADF expression examples. Replace activity and property names with the actual pipeline names.

```text
@activity('Lookup_Metadata').output.value
@item().tableName
@pipeline().RunId
@pipeline().parameters.runDate
@concat('landing/sql/', item().schemaName, '/', item().tableName, '/')
@utcNow()
```

Do not copy these expressions blindly if the repository JSON uses different activity names or metadata field names.

## 3. Dynamic SQL safety

When constructing a source query dynamically:
- Use metadata values from a controlled table, not arbitrary user input.
- Validate schema/table identifiers against an allowlist.
- Use parameterized query support for values wherever possible.
- Do not concatenate untrusted values into SQL.
- Be careful with quoting identifiers; SQL parameters generally parameterize values, not object names.

## 4. Copy activity validation

For each source and sink, check:
- Source preview/schema.
- Sink path and file naming.
- Column mapping.
- Null handling.
- Data type conversion.
- Fault tolerance behavior.
- Rows read/written and duration.
- Output file count and size.

## 5. Debug vs Trigger now vs Publish

- **Debug**: interactive test of the current draft; useful for a small test.
- **Trigger now**: runs the published pipeline configuration.
- **Publish**: commits authoring changes to the live factory.
- **Monitor**: inspect actual runs.

A common mistake is to edit a pipeline and use Trigger now without publishing the latest changes.

## 6. Retry and concurrency

- Configure retries for transient failures, not permanent configuration errors.
- Avoid excessive retries against APIs with rate limits.
- Set ForEach concurrency according to SQL capacity, SHIR capacity, API rate limits and ADLS throughput.
- Prevent concurrent writes to the same output path unless the sink design supports it.

## 7. Debugging checklist

1. Open the failed pipeline run in Monitor.
2. Open the failed activity.
3. Read the full error and activity input/output.
4. Identify whether the failure is authentication, network, metadata, source query, mapping, or sink.
5. Reproduce with a single object and a small dataset.
6. Fix the root cause.
7. Rerun and verify data correctness, not just green status.
8. Confirm audit/watermark state is consistent.
