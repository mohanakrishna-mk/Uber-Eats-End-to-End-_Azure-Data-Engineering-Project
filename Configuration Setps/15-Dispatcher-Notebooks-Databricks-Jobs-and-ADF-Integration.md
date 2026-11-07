# 15 – Dispatcher Notebooks, Databricks Jobs and ADF Orchestration (Chapters 19–26)

## 1. Why the dispatcher pattern is used

The transcript separates upstream ingestion (ADF copies raw source data to ADLS) from downstream transformation (Databricks creates Silver managed tables).

A dispatcher notebook decides which child transformation notebook to run based on a parameter received from ADF. This decouples ingestion from transformation and allows a failed downstream entity to be repaired without rerunning every source copy.

## 2. SQL dispatcher pattern

The video demonstrates this flow:

```text
ADF SQL metadata Lookup
        |
      ForEach (one SQL table per iteration)
        |
        +---- Copy SQL table/query to Bronze
        |
        +---- Trigger Databricks SQL job
                    |
             Job parameter: table_name
                    |
             SQL dispatcher notebook
                    |
             Route table_name to child notebook
                    |
             silver_<entity> notebook
                    |
             Unity Catalog Silver managed table
```

The control table Lookup returns table metadata. The `table_name` field becomes the property passed through ForEach → Databricks job parameter → notebook parameter.

## 3. Create the SQL dispatcher notebook

1. Create a dispatcher notebook in the project `routing`/dispatcher folder.
2. Read the job parameter `table_name`.
3. Create a mapping from source table name to child notebook path.
4. Validate that the received table name exists in the mapping.
5. Invoke the selected child notebook using the notebook-run utility shown in the project.
6. Pass any required parameters (source path, table name, load type) to the child.
7. Fail clearly for an unknown table name.

Illustrative routing pattern:

```python
table_name = dbutils.widgets.get("table_name")

notebook_map = {
    "customers": "/Workspace/.../silver_customers",
    "restaurants": "/Workspace/.../silver_restaurants",
    "menu_items": "/Workspace/.../silver_menu_items",
    "orders": "/Workspace/.../silver_orders",
    "order_items": "/Workspace/.../silver_order_items",
}

if table_name not in notebook_map:
    raise ValueError(f"Unsupported SQL table: {table_name}")

dbutils.notebook.run(notebook_map[table_name], 0, {
    "table_name": table_name
})
```

This is a pattern example. Use the actual utility function and notebook paths from the repository; the transcript specifically discusses a shared DB utilities `notebook.run` function and matching parameter names.

## 4. Create the Databricks SQL job

1. Databricks → Jobs & Pipelines → Create job.
2. Add a Notebook task.
3. Select the SQL dispatcher notebook as the task.
4. Select the workspace notebook source/path.
5. Configure job compute (the instructor uses a dedicated job cluster for this workflow).
6. Add a job parameter named exactly `table_name`.
7. Save the job.

The task's parameter name must match the notebook widget/parameter name.

## 5. Create ADF Databricks linked service and job activity

1. ADF Studio → Manage → Linked services → New.
2. Select Azure Databricks.
3. Select the Azure subscription and workspace.
4. Configure supported authentication and permissions.
5. Test the connection.
6. Inside the SQL ForEach, add the Databricks Job activity after the Copy activity.
7. Select the SQL Silver job.
8. Pass the job parameter:

```text
table_name = @item().table_name
```

9. Validate and debug.

The ADF identity must be authorized to invoke the Databricks job/workspace. The transcript explicitly calls out identity/authorization errors and shows granting access to the ADF identity.

## 6. CSV dispatcher and job

The video uses a separate CSV dispatcher because the input is a file rather than a SQL table.

1. Create child notebooks for `drivers`, `promotions`, and `zones`.
2. Create a CSV dispatcher notebook.
3. Read the job parameter `file_name`.
4. Map each file to its child notebook.
5. Create a Databricks job with the CSV dispatcher task.
6. Add a job parameter named `file_name`.
7. In the ADF CSV ForEach, after the Copy activity, add the Databricks Job activity.
8. Pass the current metadata value:

```text
file_name = @item().file_name
```

The parameter name must match from ADF → Job → Dispatcher notebook.

## 7. API dispatcher and job

The API dispatcher uses the endpoint identifier rather than table/file name.

1. Create child notebooks for `payments`, `deliveries`, `ratings`, and `order-status`.
2. Create an API dispatcher notebook.
3. Read the `endpoint_name` parameter.
4. Map endpoint names to the corresponding Silver notebook.
5. Create a Databricks job with an `endpoint_name` parameter.
6. In ADF API ForEach, after REST Copy activity, add the Databricks Job activity.
7. Pass:

```text
endpoint_name = @item().endpoint_name
```

## 8. Job compute configuration shown in the transcript

The instructor uses job compute for dedicated workflow execution:
- Dedicated job cluster rather than a shared all-purpose cluster.
- A specific Databricks Runtime is selected in the video (runtime versions change; use one supported by your workspace).
- Worker/CPU sizing is deliberately small for the demo.
- The transcript discusses a cluster policy controlling VM size, memory and worker count.
- The transcript highlights that jobs can remain queued while cloud VM capacity is being allocated.
- ForEach/job concurrency defaults can serialize work; increase only after considering available compute capacity and source/sink limits.

Do not copy the instructor's runtime version or worker count blindly. Select supported runtime/compute for your region and account.

## 9. Concurrency and execution behavior

The video demonstrates five SQL table iterations triggering the same job. Job runs may queue while compute starts. It explains that job concurrency set to one means only one job run executes at a time; remaining runs wait.

To tune:
1. Inspect ADF ForEach batch count.
2. Inspect Databricks job maximum concurrent runs.
3. Check cluster policy and workspace quota.
4. Check available VM capacity.
5. Increase concurrency gradually and monitor.
6. Avoid running many heavy tasks if they compete for the same cluster capacity.

## 10. Failure isolation and rerun

The transcript demonstrates a case where some Silver child tables succeed and one child fails. The instructor does not rerun the entire ingestion pipeline. Instead:
1. Open the failed Databricks job run.
2. Identify the failed notebook task and error.
3. Fix the child notebook/configuration.
4. Rerun the failed downstream task/job.
5. Verify the managed table.
6. Keep the successful upstream Bronze copy intact.

This is a key benefit of separating ADF ingestion from Databricks transformation.

## 11. Serverless vs job compute

The transcript compares:
- Job compute: Databricks provisions job-specific ephemeral compute; startup can take time and depends on VM capacity.
- Serverless compute: Databricks manages the underlying compute and autoscaling; the demo observed faster job execution after switching.

Serverless availability/features depend on workspace, region, account and workload. Choose based on governance, runtime support, startup latency, cost and workload compatibility.
