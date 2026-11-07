# 03 – Azure SQL Initial Load and Metadata-Driven ADF Pipeline

## 1. Prepare SQL source tables

Use the repository's SQL migration guide, source-to-target mapping and insert statements to create the actual source schema and sample records. Confirm:
- Database and schema names.
- Table names.
- Primary keys.
- Date/time columns used for incremental processing.
- Data types and nullable columns.
- Expected row counts.

Do not substitute guessed schemas for the repository's mapping.

## 2. Create SQL linked service and datasets

1. In ADF Studio → **Manage** → **Linked services** → **New**.
2. Select Azure SQL Database.
3. Configure server/database and authentication.
4. Test connection.
5. Create a SQL dataset for the source table.
6. For reusable ingestion, add `schemaName` and `tableName` parameters to the dataset.

## 3. Build a simple initial-load pipeline first

Create a pipeline named e.g. `PL_SQL_InitialLoad_Test`.

1. Add a **Copy data** activity.
2. Source: Azure SQL dataset.
3. Select one small source table.
4. Sink: ADLS Gen2 dataset in the landing/SQL path.
5. Choose a suitable file format (CSV or Parquet according to the project's configured target).
6. Configure file naming and mapping.
7. Debug.
8. Verify the output path and record count.

Only after a single-table test works should you generalize the pipeline.

## 4. Create the metadata/control table

The project uses metadata-driven ingestion: metadata tells ADF which source objects to process and how to process them. The exact schema should come from the repository's SQL migration document and metadata definitions.

A control table commonly needs fields equivalent to:

| Logical field | Purpose |
|---|---|
| source schema/table | Identifies source object |
| target folder/table | Identifies destination |
| load type | Full or incremental |
| watermark column | Source column used to detect changes |
| last watermark | Last successfully processed boundary |
| active flag | Enables/disables an object |
| load status / audit fields | Tracks execution |

These are conceptual fields; align names and types with the actual project DDL.

## 5. Build the metadata-driven pipeline

Create a pipeline such as `PL_SQL_Metadata_Load`.

### Activity A – Lookup metadata

1. Add **Lookup**.
2. Connect to the control/metadata table.
3. Use a query that returns active source objects.
4. Enable **First row only = false** so the result is an array.
5. Test the Lookup output. Confirm the shape of `output.value`.

### Activity B – ForEach

1. Add **ForEach** after Lookup.
2. Items expression:

```text
@activity('Lookup_Metadata').output.value
```

3. Configure sequential processing initially. Parallel processing can be enabled later after assessing SQL, ADF and storage capacity.
4. Inside ForEach, add Copy activity.

### Activity C – Parameterize source and sink

In Copy activity:
- Source dataset parameters receive the current metadata row's schema/table.
- Sink dataset parameters receive the target directory/file/table.
- Use dynamic content from `item()`.

Illustrative expressions:

```text
@item().schemaName
@item().tableName
@item().targetFolder
```

The exact JSON property names must match the metadata query output.

### Activity D – Logging

Add success and failure paths to an audit mechanism. Record pipeline run ID, table name, start/end time, row count (where available), status and error message. Avoid allowing a logging failure to silently hide the original ingestion failure.

## 6. Incremental processing

The repository has a separate document dedicated to SQL source and incremental loading. Follow its exact watermark column and control-table design.

General watermark pattern:
1. Read the previous successful watermark from metadata.
2. Query the source for rows greater than the old watermark and less than or equal to a captured upper watermark.
3. Copy the selected rows to the lake.
4. Validate the copy.
5. Update the stored watermark only after successful completion.
6. On failure, do not advance the watermark.

Illustrative SQL predicate:

```sql
WHERE ModifiedDate > @OldWatermark
  AND ModifiedDate <= @NewWatermark
```

Capture `NewWatermark` once per run; do not recalculate it independently for every activity. Use a stable timestamp or source-side maximum according to the design.

### Why update watermark after success?

If you advance it before the copy completes, a failed run can skip records permanently. If you do not advance it after a successful run, records may be re-read. Design the sink to be idempotent or use a deduplication/merge strategy downstream.

## 7. Test scenarios

- Initial load with a populated table.
- Empty table.
- New rows inserted after initial load.
- Existing row updated (if the source has a reliable modified timestamp).
- Pipeline fails during copy.
- Pipeline rerun after failure.
- Two tables processed by the same metadata-driven pipeline.
- Inactive metadata row is skipped.

## 8. Common failures

| Failure | Check |
|---|---|
| Login/network failure | SQL firewall, private endpoint, credentials, server name |
| Invalid object name | Database/schema/table spelling |
| Dynamic query error | Quote/escape identifiers safely; verify metadata field names |
| Lookup returns one row only | First row only must be false for ForEach |
| ForEach receives null | Inspect Lookup output and expression |
| Watermark skips rows | Boundary semantics, timezone, precision, update timing |
| Duplicate records | Rerun behavior, sink idempotency, primary key strategy |

## Completion checklist

- [ ] Source tables created from repository scripts.
- [ ] SQL linked service tests successfully.
- [ ] Single-table full load works.
- [ ] Metadata/control table populated.
- [ ] Lookup returns all active objects.
- [ ] ForEach processes each object.
- [ ] Parameterized source/sink paths work.
- [ ] Incremental watermark advances only after success.
- [ ] Failure and rerun tests pass.
