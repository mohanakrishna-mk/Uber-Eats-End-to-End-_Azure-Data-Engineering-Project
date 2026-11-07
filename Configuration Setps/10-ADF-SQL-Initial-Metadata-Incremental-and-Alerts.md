# 10 – ADF SQL Initial Load, Control Table, Incremental Load and Logic Apps (Chapters 6–10)

This is the detailed SQL ingestion sequence demonstrated in the transcript.

## 1. Configure Azure SQL linked service

1. ADF Studio → Manage → Linked services → New.
2. Select **Azure SQL Database**.
3. Select the subscription, SQL server and database.
4. Select **System-assigned managed identity** as the authentication method, as demonstrated.
5. Use **AutoResolveIntegrationRuntime** for this cloud-to-cloud SQL connection.
6. Test connection and save.
7. At the SQL target, authorize the ADF managed identity to read the required source tables. The exact SQL grants must match your security setup.

## 2. Configure ADLS linked service

1. Create an ADLS Gen2 linked service.
2. Select the storage account and use the ADF identity-supported authentication option.
3. At the storage account/container scope, assign ADF the **Storage Blob Data Contributor** role as shown in the video.
4. Test the linked service.

The video explains the distinction:
- Linked service = connection/bridge and authentication to a source or sink.
- Dataset = reference to the actual table, file, folder or data object.
- For a Copy activity, configure source and sink linked services and source and sink datasets.

## 3. Create source and sink datasets

Create an Azure SQL dataset for the source table and an ADLS dataset for the target.

For the initial simple test:
1. Create the SQL dataset and select a source table.
2. Create the ADLS dataset and select the `bronze/sql` landing path.
3. Place a Copy data activity in a new pipeline.
4. Select the SQL dataset as Source.
5. Select the ADLS dataset as Sink.
6. Debug one table and verify the landed file.
7. Only after the single-table copy works, parameterize and scale it.

## 4. Build the SQL metadata/control table

The transcript uses a SQL table named `dbo.pipeline_control`. It stores table-level metadata and pipeline run state. The instructor describes fields/purposes including:

| Field/purpose | What it records |
|---|---|
| Source/table identity | Which source table the row represents |
| Load type | Full/initial or incremental processing |
| Watermark column | Source timestamp column, demonstrated as `updated_at` |
| Last watermark value | Last stored timestamp boundary |
| Pipeline start date/time | When the table's run started |
| Pipeline finish date/time | When the run completed |
| Run status | Success/failure/in-progress state |
| Rows processed | Number of records processed in the run |

Use the exact column names and SQL types from `Uber_Eats_SQL_Source_and_Incremental_Loading.docx` and the SQL script in the repository. The table is both a metadata/control table and an audit source.

Populate one control row for each SQL source entity. The transcript demonstrates five rows returned by Lookup.

## 5. Convert the initial-load pipeline to metadata-driven

### A. Create a Lookup dataset

1. Create a new Azure SQL dataset that points to `dbo.pipeline_control`.
2. Name it clearly (the transcript refers to a SQL control dataset).
3. Add a Lookup activity to the pipeline.
4. Select the SQL control dataset.
5. Use a simple query such as:

```sql
SELECT *
FROM dbo.pipeline_control;
```

6. In Lookup settings, turn **First row only** off. The transcript explicitly changes this because it needs all five metadata rows, not just the first row.
7. Debug and inspect `output.value`.

### B. Add ForEach

1. Add ForEach after Lookup.
2. Set Items to the Lookup array:

```text
@activity('Lookup_Control').output.value
```

3. Start with sequential execution while validating.
4. Inside ForEach, add the Copy activity.
5. Map the current control row's source table, target folder, file name, load type and watermark fields to the dataset/activity parameters.

Use the actual JSON/property names from the control table. In the transcript, the Lookup output exposes table metadata, target table/folder, watermark column and last successful watermark.

### C. Parameterize datasets

For the SQL dataset, create parameters for the schema/table or table name used by the source. For the ADLS sink, create parameters for the destination folder and file name.

In the ForEach activity, pass the current metadata row values with `@item().<columnName>` expressions. The exact property spelling must match the output of `dbo.pipeline_control`.

The video demonstrates the pattern of using one pipeline and iterating across all configured SQL tables instead of manually building a separate pipeline for each table.

## 6. Build incremental SQL processing

The transcript demonstrates an update to an existing row (an order status change), then filters the source query using the `updated_at` timestamp and the stored last watermark.

Conceptual query produced by the ADF expression:

```sql
SELECT *
FROM <source_table>
WHERE updated_at > '<last_watermark_value>';
```

The actual pipeline constructs the query dynamically using ADF's expression builder and the current ForEach metadata item.

Steps:
1. Duplicate/reuse the metadata-driven pipeline for incremental processing.
2. Lookup the control table.
3. ForEach each metadata row.
4. In Copy activity Source, select **Query** rather than copying the entire table.
5. Build a dynamic query using:
   - Current source table name.
   - Current watermark column name.
   - Current `last_watermark` value.
6. Debug the pipeline.
7. Update a source row's `updated_at`/business status to create a test change.
8. Run the incremental pipeline.
9. Inspect Copy activity output and verify that only changed rows are read.

The transcript mentions adding another activity to update the control table after a successful incremental run. Configure that update only on the successful path. Do not advance the watermark when the copy fails.

## 7. Add success notification using Logic Apps

### A. Create Logic App

1. Azure portal → resource group → Create → Logic App.
2. The instructor chooses a multi-tenant Consumption-style hosting plan to keep the demo lightweight.
3. Create the Logic App.
4. Open Logic App Designer.
5. Select the **When an HTTP request is received** trigger.
6. Add an email action (the video uses an email notification).
7. Configure recipient, subject and body. The subject/body can include pipeline/run status details.
8. Save the Logic App.
9. Copy the generated HTTP POST URL from the request trigger. Treat this URL as a secret.

### B. Call it from ADF

1. Return to the SQL incremental pipeline.
2. Add a **Web** activity after the successful pipeline path.
3. Name it something like `Send_Success_Notification`.
4. Method: POST.
5. URL: the HTTP URL generated by the Logic App trigger.
6. Body: include the success message and useful run details (pipeline name/run ID).
7. Configure the activity dependency so it runs after the successful processing path.
8. Debug the pipeline and confirm the email arrives.

The transcript demonstrates the success path. It also explains that a separate Web activity can be added for the failure path. For failure notification, connect a second Web activity to the failure dependency and send a failure status/error message.

## 8. Validate and troubleshoot

- Lookup output count should equal active control rows.
- ForEach should execute once per metadata row.
- Initial load should bring the full table.
- Incremental load should return only rows with `updated_at` newer than the stored watermark.
- Watermark should update only after a successful load.
- Logic App trigger URL must not be committed to Git.
- SQL login/firewall errors: verify SQL network and ADF identity permissions.
- Empty incremental result: verify watermark type, timezone and comparison boundary.
