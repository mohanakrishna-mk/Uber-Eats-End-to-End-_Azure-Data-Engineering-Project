# 06 – Azure Databricks, Unity Catalog and PySpark Transformations

## 1. Create the Databricks workspace

1. In Azure portal, search **Azure Databricks** → **Create**.
2. Select the project resource group and a supported region.
3. Choose a workspace name, pricing tier and networking configuration appropriate for the learning environment.
4. Review cost implications before creating compute.
5. Open the workspace.

The transcript uses Databricks for downstream processing of the data landed by ADF.

## 2. Configure Unity Catalog

Use Unity Catalog rather than relying on unmanaged workspace-local storage for a governed implementation.

High-level sequence:
1. Confirm the workspace is attached to a Unity Catalog metastore.
2. Create or identify an Azure Databricks Access Connector.
3. Assign the connector's managed identity the minimum required Storage Blob Data permissions on the intended ADLS scope.
4. In Databricks, create a storage credential that references the Access Connector identity.
5. Create an external location pointing to the ADLS path and referencing that credential.
6. Grant the required privileges to the user/group that will create schemas, volumes and tables.

Exact SQL syntax and permissions depend on your Databricks account/workspace setup and the storage path. Use the current Unity Catalog UI/SQL documentation and least privilege.

## 3. Create catalog and schemas

A logical development layout could be:

```sql
CREATE CATALOG IF NOT EXISTS dev;
CREATE SCHEMA IF NOT EXISTS dev.bronze;
CREATE SCHEMA IF NOT EXISTS dev.silver;
CREATE SCHEMA IF NOT EXISTS dev.gold;
```

If the course/repository uses a specific schema name such as `dev.step_right` or a different naming convention, retain that convention consistently. Do not create duplicate schemas with slightly different spellings.

## 4. Create volumes / folders

The video describes landing and staging locations/volumes. In Unity Catalog, volumes provide governed file access under a catalog/schema. Create the volumes using the external location and names required by the project, for example:

```sql
CREATE VOLUME IF NOT EXISTS dev.bronze.landing;
CREATE VOLUME IF NOT EXISTS dev.silver.staging;
```

These are illustrative names. Confirm the exact catalog/schema/volume names from the transcript and repository notebooks before running.

## 5. Connect Databricks to ADLS

Prefer Unity Catalog external locations and storage credentials. Avoid account keys or SAS tokens embedded in notebooks.

Validate access by listing the expected path and reading a small landed file. Confirm:
- Storage account/container/path is correct.
- The Access Connector identity has required data permissions.
- Unity Catalog grants are sufficient.
- Network/firewall/private endpoint settings allow the workspace to reach storage.

## 6. Configure Git integration

1. In Databricks workspace, open **User settings** → **Git integration**.
2. Configure a supported Git provider and authentication method.
3. Add the repository URL.
4. Clone the repo into a Databricks Git folder.
5. Confirm notebooks and supporting files are visible.

Do not paste a personal access token into notebook source or commit it. Use the provider's recommended credential flow.

## 7. Create a cluster / compute

For learning:
1. Open **Compute**.
2. Create a small all-purpose cluster if permitted and cost-effective, or use a job compute configuration for scheduled runs.
3. Select a supported Databricks Runtime.
4. Choose a small worker size and low worker count.
5. Set auto-termination.
6. Attach required libraries only when needed.
7. Start and test.

For production, prefer job compute for scheduled workloads and define policies, tags, access controls and budgets. Serverless availability depends on workspace, region, account and workload support.

## 8. Build the notebook workflow

The transcript describes:
- A utility module.
- Importing utilities into notebooks.
- PySpark transformations for SQL source tables.
- Business rules and data validation.
- Overwrite vs append behavior.
- Dispatcher notebooks.
- Databricks jobs for SQL, CSV/on-premises and API sources.
- ADF orchestration of jobs.

Recommended notebook responsibilities:
- `00_config`: paths, catalog/schema names and non-secret settings.
- `01_utils`: reusable read/write/logging/validation functions.
- `bronze_*`: read raw landed source data and persist a bronze representation.
- `silver_*`: clean types, normalize columns, handle nulls/duplicates and apply business rules.
- `gold_*`: aggregate and model business-ready datasets.
- `dispatcher_*`: accept source/run parameters and invoke the relevant processing flow.

Use the actual notebook names and code in the repository's `notebooks/` folder when implementing.

## 9. Read and write Delta

Example pattern (adjust path/table names to the project):

```python
from pyspark.sql import functions as F

source_path = "abfss://<container>@<storage>.dfs.core.windows.net/<landing-path>"
df = spark.read.format("json").load(source_path)

# Apply explicit schema and transformations in the actual project.
clean_df = df.dropDuplicates()

(clean_df.write
    .format("delta")
    .mode("append")
    .saveAsTable("dev.bronze.example_table"))
```

Use `overwrite` only when the business load pattern is full replacement or a controlled partition overwrite. Use `append` for new data only when duplicate/replay behavior is handled. For mutable business entities, a Delta `MERGE` pattern may be more appropriate than blind append.

## 10. Data quality checks

At minimum validate:
- Expected columns and data types.
- Nulls in mandatory fields.
- Primary/business key uniqueness.
- Duplicate records.
- Valid date/time ranges.
- Referential integrity where relevant.
- Row counts between source and target.
- Rejected/quarantined records and reason.

Do not silently discard invalid rows. Keep counts and sample rejected records for diagnosis.

## 11. Jobs and ADF orchestration

1. Create a Databricks Job for the relevant dispatcher/notebook.
2. Define required parameters (source, run date, load type, input path, target schema).
3. Configure job compute and retries.
4. Run the job manually with a small dataset.
5. Confirm output tables and logs.
6. In ADF, add the supported Databricks job/notebook activity.
7. Configure workspace connection and authentication securely.
8. Pass the parameters from ADF.
9. Test ADF-triggered execution and failure propagation.

The project uses source-specific job flows; keep SQL, CSV and API execution independently testable.

## 12. Troubleshooting

| Symptom | Check |
|---|---|
| Permission denied on ADLS | Access Connector RBAC, external location, UC grants |
| `abfss` path fails | Account/container/path and storage networking |
| Table not found | Catalog/schema/table and current catalog |
| Module import fails | Git folder path, Python package layout, notebook working directory |
| Delta schema mismatch | Explicit schema, evolution policy, source changes |
| Duplicate rows | Append/replay behavior, key strategy, deduplication |
| Job fails but notebook works | Job parameters, compute policy, library/environment differences |
| ADF cannot start job | Workspace URL, identity/permissions, job ID and parameter mapping |

## Completion checklist

- [ ] Workspace and Unity Catalog access ready.
- [ ] Access Connector identity can access ADLS.
- [ ] Storage credential and external location created.
- [ ] Catalog/schema/volumes created.
- [ ] Git repository cloned.
- [ ] Utility and transformation notebooks run.
- [ ] Bronze and Silver outputs validated.
- [ ] Dispatcher/job runs successfully.
- [ ] ADF can orchestrate the Databricks job.
