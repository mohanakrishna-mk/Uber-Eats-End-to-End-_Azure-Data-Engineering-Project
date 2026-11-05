# End-to-End Azure Data Engineering Platform — Video Notes

> **Source:** YouTube tutorial shared by the user  
> **Video:** https://youtu.be/Lt2OIwQqnwk  
> **Notes basis:** The supplied video transcript, organized into the 28 chapters present in the transcript.  
> **Project scenario:** Uber Eats-style data platform using Azure Data Factory (ADF), Azure Data Lake Storage Gen2 (ADLS Gen2), Azure SQL, Azure Databricks / PySpark, Delta Lake, and Power BI.

---

## Table of contents

1. [Project overview](#1-project-overview)
2. [End-to-end architecture](#2-end-to-end-architecture)
3. [Business requirements and source-to-target mapping](#3-business-requirements-and-source-to-target-mapping)
4. [ADLS Gen2 storage design](#4-adls-gen2-storage-design)
5. [ADF fundamentals](#5-adf-fundamentals)
6. [Azure identities and permissions](#6-azure-identities-and-permissions)
7. [Azure resource setup](#7-azure-resource-setup)
8. [Azure SQL source setup](#8-azure-sql-source-setup)
9. [ADF linked services and datasets](#9-adf-linked-services-and-datasets)
10. [SQL initial-load pipeline](#10-sql-initial-load-pipeline)
11. [Control table and metadata-driven pipelines](#11-control-table-and-metadata-driven-pipelines)
12. [SQL incremental-load pipeline](#12-sql-incremental-load-pipeline)
13. [Logic Apps notifications](#13-logic-apps-notifications)
14. [On-premises CSV ingestion and SHIR](#14-on-premises-csv-ingestion-and-shir)
15. [REST API ingestion](#15-rest-api-ingestion)
16. [Databricks workspace, Unity Catalog and storage access](#16-databricks-workspace-unity-catalog-and-storage-access)
17. [Databricks Git integration and utility code](#17-databricks-git-integration-and-utility-code)
18. [Bronze and Silver processing with PySpark](#18-bronze-and-silver-processing-with-pyspark)
19. [Business rules and data quality](#19-business-rules-and-data-quality)
20. [Delta Lake: overwrite, append, ACID and time travel](#20-delta-lake-overwrite-append-acid-and-time-travel)
21. [Dispatcher notebooks and Databricks jobs](#21-dispatcher-notebooks-and-databricks-jobs)
22. [ADF-to-Databricks orchestration](#22-adf-to-databricks-orchestration)
23. [Troubleshooting and operational practices](#23-troubleshooting-and-operational-practices)
24. [Gold layer and reporting](#24-gold-layer-and-reporting)
25. [Project execution sequence](#25-project-execution-sequence)
26. [Interview revision questions](#26-interview-revision-questions)
27. [Production considerations and gaps to validate](#27-production-considerations-and-gaps-to-validate)
28. [Video chapter index](#28-video-chapter-index)

---

## 1. Project overview

The tutorial builds an end-to-end data engineering platform for an Uber Eats-like business. It demonstrates ingestion from three different source types, central landing in a data lake, transformation using Databricks/PySpark, and downstream reporting.

### Technologies used

| Area | Technology | Responsibility |
|---|---|---|
| Cloud platform | Microsoft Azure | Hosts the platform resources |
| Orchestration and ingestion | Azure Data Factory (ADF) | Extracts and moves data |
| Cloud data lake | Azure Data Lake Storage Gen2 | Central landing and storage |
| Relational source | Azure SQL Database | Structured source tables |
| On-premises ingestion | Self-hosted Integration Runtime (SHIR) | Connects ADF to local/private files |
| API ingestion | REST API | Supplies operational endpoint data |
| Transformation | Azure Databricks + PySpark | Reads, cleans, validates and transforms data |
| Lakehouse storage | Delta Lake | Stores transactional Delta tables |
| Governance | Unity Catalog | Organizes and governs Databricks data objects and storage access |
| Reporting | Power BI | Business reporting and dashboards |
| Notification | Azure Logic Apps | Sends pipeline completion notifications |
| Source control | Git integration | Stores and synchronizes project notebooks/code |

### Three source systems

1. **Azure SQL Database** — cloud-hosted relational source.
2. **CSV files on an on-premises/local machine** — represents files supplied by users or vendors, for example through an on-premises file server or SFTP-style process.
3. **REST API** — operational data exposed through API endpoints.

### Main project flow

```text
Azure SQL Database ───────────────┐
                                  │
On-premises CSV files ── SHIR ────┼──> Azure Data Factory
                                  │             │
REST API ─────────────────────────┘             │
                                                v
                                      ADLS Gen2 landing area
                                                │
                                                v
                                      Azure Databricks / PySpark
                                                │
                                      Bronze and Silver layers
                                                │
                                                v
                                           Gold layer
                                                │
                                                v
                                             Power BI

ADF pipeline status ─────────────────────> Logic Apps notification
```

ADF is the ingestion/orchestration layer in this design. Databricks is the downstream processing layer. ADLS Gen2 is the centralized lake storage.

---

## 2. End-to-end architecture

The platform separates ingestion from transformation.

### Ingestion responsibility — ADF

ADF connects to the source systems, reads the required entities/files/endpoints, and lands the data in ADLS Gen2. The tutorial uses ADF activities such as:

- **Lookup** — reads configuration/metadata rows.
- **ForEach** — iterates through metadata records.
- **Copy Data** — copies data from a source to a sink.
- **Web activity** — invokes a Logic Apps endpoint for notification.
- **Databricks activity** — triggers a Databricks job/notebook workflow.

### Processing responsibility — Databricks

Databricks reads the landed files, applies common standardization and entity-specific business rules, and writes managed Delta tables into the relevant schemas/layers.

### Why separate the two?

- ADF handles source connectivity, ingestion and orchestration.
- Databricks handles Spark-based transformations and Delta table operations.
- The lake provides a centralized handoff between ingestion and processing.
- A downstream Databricks job can be retried without repeating a successful source ingestion, because the upstream and downstream stages are decoupled.

---

## 3. Business requirements and source-to-target mapping

The tutorial emphasizes that pipeline development should begin with requirements and mapping documents, not by immediately creating activities.

### Requirement-gathering steps

1. Identify the source systems.
2. Identify the tables, files and API endpoints in each source.
3. Identify schemas and columns for each entity.
4. Map each source entity to its target lake folder/table.
5. Identify keys, timestamps and business-status columns.
6. Define the storage/folder layout.
7. Define the load type and any watermark column needed for incremental processing.

### Example source inventory from the tutorial

| Source | Example entities / endpoints |
|---|---|
| Azure SQL | Customers, orders, order items, menu items, restaurants, and the pipeline control table |
| CSV | Drivers, promotions, zones (the transcript's audio recognition renders the last name imperfectly; verify against the instructor's mapping file) |
| REST API | Payments, deliveries, order status, ratings, and watermark-related endpoint/configuration |

The exact source columns and full SQL DDL/insert statements are supplied as separate project materials by the instructor; they are not fully reproduced in the transcript. Use the instructor's source-to-target mapping and SQL scripts for exact schemas.

### Important column types to identify

| Column type | Why it matters |
|---|---|
| Primary/business key | Identifies a record and supports deduplication |
| Foreign key | Establishes relationships between entities |
| Created timestamp | Helps identify newly inserted records and support time-based analytics |
| Updated timestamp | Helps detect changed records for incremental loads |
| Status column | Supports business KPIs and filtering |
| Numeric/amount columns | Supports revenue, payment and other measures |

For example, a business may report total sales for the current year versus the previous year. Correct timestamp and amount columns are therefore important for downstream analytics.

---

## 4. ADLS Gen2 storage design

The tutorial creates a container named `branch` and organizes landing data by source system.

### Folder layout shown conceptually

```text
<storage-account>/
└── branch/
    ├── SQL/
    │   ├── customers/
    │   ├── orders/
    │   ├── order_items/
    │   ├── menu_items/
    │   └── restaurants/
    │
    ├── CSV/
    │   ├── drivers/
    │   ├── promotions/
    │   └── zones/
    │
    └── API/
        ├── payments/
        ├── deliveries/
        ├── order_status/
        └── ratings/
```

The instructor also places JSON configuration/metadata files in the lake so ADF can read the list of entities and target locations dynamically.

### Why enable hierarchical namespace?

ADLS Gen2 hierarchical namespace provides directory and file-system semantics. The tutorial highlights:

- Organizing data into folders and subfolders.
- Applying access control at directory/file level using ACLs.
- Giving a vendor or user access to only the folder they need, rather than exposing unrelated source folders.
- Making source-based landing zones easier to manage.

### Storage account options discussed

- **Hierarchical namespace:** Enabled for ADLS Gen2.
- **Performance tier:** Standard in the practice setup.
- **Redundancy:** Locally redundant storage (LRS) selected in the demo.
- **SFTP:** Not enabled in the demo because it can add cost; the tutorial mentions it as an option for vendor file drops.
- **Access tier:** The instructor selects Cool for the practice setup. Confirm the access pattern and current Azure pricing before choosing a tier in a real workload.

> **Design note:** The folder names above are a conceptual reconstruction of the spoken walkthrough. Follow the exact mapping document if the instructor's downloadable project files use different spellings or casing.

---

## 5. ADF fundamentals

Three ADF concepts are central to the project: linked services, datasets and integration runtimes. Identity and authorization are also required for connections to work.

### 5.1 Linked service

A linked service defines connection information for a data store or compute service. It is the connection/bridge ADF uses to reach a source or sink.

Examples in this project:

- Azure SQL linked service.
- ADLS Gen2 linked service.
- JSON configuration-file linked service.
- On-premises file-system linked service.
- REST API linked service.
- Databricks linked service.

It contains connection details such as server/database or storage account, endpoint, authentication method and integration runtime.

### 5.2 Dataset

A dataset identifies the data structure/location that an ADF activity reads or writes.

Examples:

- A particular Azure SQL table.
- A file/folder in ADLS Gen2.
- A JSON metadata file.
- A CSV file or folder.
- A REST resource/relative path.

**Remember:**

| Object | Meaning |
|---|---|
| Linked service | How to connect |
| Dataset | Which data/location to use |
| Pipeline/activity | What operation to perform |

A linked service does not by itself select a particular table/file. A dataset supplies that reference.

### 5.3 Source and sink

- **Source:** Where ADF reads data from.
- **Sink:** Where ADF writes data to.

For the SQL-to-lake copy:

```text
Source linked service + source dataset
                  |
                  v
             Copy activity
                  |
                  v
Sink linked service + sink dataset
```

### 5.4 Integration Runtime (IR)

The tutorial discusses two important IR types:

- **Azure AutoResolve Integration Runtime:** Used for the cloud-to-cloud Azure SQL to ADLS movement in the demo.
- **Self-hosted Integration Runtime (SHIR):** Used when ADF needs to reach an on-premises machine or a private-network data source.

IR supplies the data movement/dispatch infrastructure used by ADF. It is not the dataset and it is not the source database itself.

---

## 6. Azure identities and permissions

A connection can be configured correctly and still fail if the identity used by ADF or Databricks has not been authorized at the target.

### Identity options mentioned

1. **System-assigned managed identity** — created and managed with the Azure resource lifecycle.
2. **User-assigned managed identity** — a separately managed identity that can be associated with resources.
3. **Service principal** — an application identity registered in Microsoft Entra ID.

The tutorial uses the ADF system-assigned managed identity for the Azure SQL and ADLS linked services.

### Azure SQL authorization

The walkthrough creates the ADF managed identity as a database user using an external provider and grants it the database reader role so it can read source tables.

Conceptual SQL pattern (use the actual ADF managed-identity name and database context):

```sql
-- Run in the target Azure SQL database with an authorized admin identity.
CREATE USER [<adf-managed-identity-name>] FROM EXTERNAL PROVIDER;

ALTER ROLE db_datareader
ADD MEMBER [<adf-managed-identity-name>];
```

The exact statement and permissions must be checked against the database's authentication configuration. Do not grant broad database permissions if read-only access is sufficient.

### ADLS Gen2 authorization for ADF

The tutorial assigns:

- **Role:** Storage Blob Data Contributor
- **Principal:** ADF system-assigned managed identity
- **Scope:** The storage account in the demo

This permits the ADF identity to read/write data in the lake according to the role's scope.

### Databricks authorization for ADLS

The tutorial creates/uses an Azure Databricks Access Connector and assigns the relevant storage data role to its managed identity. Unity Catalog storage credentials and external locations are then configured to access ADLS.

> **Security practice:** Prefer least-privilege scope. The demo uses broad storage-account scope for simplicity; production designs should scope access as narrowly as the workload allows and review both Azure RBAC and ADLS ACL requirements.

---

## 7. Azure resource setup

The tutorial creates the core resources inside one resource group.

### Resources

- Resource group for the project.
- Azure Data Factory.
- Storage account with ADLS Gen2 enabled.
- Azure SQL Database.
- Logic App.
- Azure Databricks workspace and Access Connector (later in the walkthrough).

### Portal setup sequence

1. Create a resource group.
2. Create an Azure Data Factory instance in that resource group.
3. Create a storage account.
4. Enable hierarchical namespace to use ADLS Gen2.
5. Choose the required performance, redundancy and access-tier settings.
6. Create the `branch` container.
7. Create source-specific folders.
8. Create the Azure SQL Database and populate the source tables using the instructor's SQL scripts.
9. Open ADF Studio and configure linked services, datasets and pipelines.

Resource names in the demo contain an Uber Eats-style naming convention. Use names that are unique in Azure and follow your organization's naming standards.

---

## 8. Azure SQL source setup

The tutorial creates an Azure SQL Database and uses it as the cloud relational source.

### Setup

1. Create an Azure SQL logical server/database.
2. Record the server name and database name; ADF needs them for its linked service.
3. Use the Azure SQL free offer if eligible and available in the subscription, as the instructor recommends for practice.
4. Open the Query Editor or another authorized SQL client.
5. Execute the instructor's SQL scripts to create and populate the project source tables.
6. Create/populate the pipeline control table used for metadata and watermark tracking.
7. Create the ADF managed-identity database user and grant the required read role.

### Source entities

The transcript refers to customers, orders, order items, menu items and restaurants, as well as a pipeline control table. It also discusses multiple source entities in the control metadata.

The precise DDL and seed data are separate instructor-provided files and are not fully included in the transcript.

---

## 9. ADF linked services and datasets

Create linked services before configuring the copy activity.

### Linked services in the SQL-to-ADLS flow

| Linked service | Connects ADF to | Authentication / IR in demo |
|---|---|---|
| SQL source | Azure SQL Database | System-assigned managed identity; AutoResolve IR |
| ADLS sink | ADLS Gen2 storage account | System-assigned managed identity; AutoResolve IR |
| JSON metadata | ADLS configuration file | Uses the configured storage connection |
| CSV source | Local/on-premises file system | SHIR |
| REST API source | API base URL | Configured API authentication/connection details |
| Databricks | Databricks workspace/job | Managed identity/authorized workspace connection |

### Datasets

Create a source dataset and a sink dataset for each data movement pattern.

Examples:

- SQL source dataset.
- ADLS Gen2 sink dataset.
- SQL control-table dataset for Lookup.
- JSON dataset for metadata.
- CSV file-system dataset.
- REST API dataset.

### Naming convention used in the walkthrough

The instructor uses prefixes such as:

- `LS` for linked service.
- `DS` for dataset.
- `PL` for pipeline.

Examples from the walkthrough include SQL source/sink linked-service names and SQL source/sink datasets. Use a consistent naming standard across the project.

---

## 10. SQL initial-load pipeline

### What is an initial load?

Initial load copies the existing data for an entity into the target for the first time. If a table contains one million rows, the initial load copies the full set.

### Basic pipeline

1. Create an ADF pipeline with a name indicating SQL and initial load.
2. Add a **Copy Data** activity.
3. Configure the source dataset as Azure SQL.
4. Configure the sink dataset as ADLS Gen2.
5. Select the SQL entity/table.
6. Select the target folder in the `branch/SQL` area.
7. Validate the pipeline.
8. Debug/run it.
9. Check the output file in the target ADLS folder.

### Why the first single-table pipeline is not enough

The first version is configured for one specific SQL table. If there are 20, 30 or 100 tables, creating one independent pipeline per table becomes difficult to maintain.

The tutorial first demonstrates a single-table copy to explain the basic mechanism, then converts the design into a metadata-driven pipeline.

---

## 11. Control table and metadata-driven pipelines

This is one of the most important parts of the tutorial.

### 11.1 Purpose of the control table

The pipeline control table stores metadata and run information for each entity. It is used for:

- Identifying the source entity.
- Identifying the target location.
- Identifying the load type.
- Identifying the watermark column.
- Storing the last successful watermark.
- Recording pipeline execution/audit information.
- Supporting incremental processing.

The transcript describes columns such as:

| Metadata field | Purpose |
|---|---|
| Control ID | Identifies a configuration row |
| Source name | Identifies the source system |
| Schema name | Source schema |
| Table name | Source entity/table |
| Load type | Initial or incremental load |
| Watermark column | Source column used to detect changes |
| Last watermark value | Last successfully processed watermark |
| Batch size | Configured batch size/metadata |
| Target table/folder | Target destination |

The transcript also discusses storing execution details such as start time, end time, status and rows processed. The exact DDL and full column list should be checked against the instructor's SQL script.

### 11.2 What is a watermark?

A watermark is a stored boundary value—often a timestamp—used to identify records that are newer than the last successful load.

Example:

```sql
SELECT *
FROM dbo.orders
WHERE updated_at > '<last_successful_watermark>';
```

If the control table stores `2026-11-04 10:00:00`, the next incremental query selects rows whose `updated_at` is greater than that value.

The tutorial uses an `updated_at`-style column to track inserted/updated records.

### 11.3 Lookup activity

The pipeline uses Lookup to read the control table.

Important configuration:

- Source dataset points to the SQL control table.
- Query reads the configured rows.
- Disable **First row only** when the pipeline needs all configuration rows rather than one row.
- Lookup returns an array of row objects/dictionaries in its output.

The tutorial contrasts Lookup with Get Metadata:

| Activity | Main purpose |
|---|---|
| Lookup | Reads rows/data from a table or query |
| Get Metadata | Reads metadata about a file/folder, such as file properties and size |

### 11.4 ForEach activity

The ForEach activity iterates over the array returned by Lookup.

Conceptually, the items expression is:

```text
@activity('Lookup_SQL_Control').output.value
```

For each control-table row, the pipeline receives values such as source table, target folder, load type and watermark configuration.

### 11.5 Parameterized datasets

A hard-coded dataset points to one table or folder. The tutorial creates parameterized datasets so a single pipeline can process multiple entities.

Typical parameters in the walkthrough include:

- Folder name.
- File name/table name.

The ForEach iteration passes the current metadata row's values into the dataset parameters. The Copy activity then uses those parameters to read the correct source entity and write to the correct target location.

### 11.6 Metadata-driven pipeline pattern

```text
Lookup control/metadata table
            |
            v
       ForEach row
            |
            v
  Pass row values to parameters
            |
            v
       Copy activity
            |
            v
  Correct source table -> target folder
```

### Benefits

- One reusable pipeline can process many tables.
- New entities can often be onboarded by adding configuration rows instead of creating new pipelines.
- Source/target mapping is maintained centrally.
- Load type and watermark settings are entity-specific.
- Easier maintenance than one hard-coded pipeline per table.

---

## 12. SQL incremental-load pipeline

### Initial load versus incremental load

| Initial load | Incremental load |
|---|---|
| Reads the full existing entity | Reads only new/changed records |
| Usually used for first-time population | Used for subsequent scheduled runs |
| Can use overwrite when creating the initial target | Uses append or merge/upsert depending on data semantics |
| More data may be moved | Reduces repeated data movement |

### Incremental flow in the tutorial

1. Clone/adapt the initial-load pipeline for incremental processing.
2. Read the last successful watermark from the control table.
3. Identify the configured watermark column for the current entity.
4. Build a source query that filters rows newer than the watermark.
5. Copy only those rows to the lake.
6. After a successful load, update the control-table watermark to the new high-water mark.
7. Record run status and audit information.

Example filter:

```sql
SELECT *
FROM dbo.orders
WHERE updated_at > '<last_successful_watermark>';
```

### Critical operational rule

Advance the watermark only after the data has been successfully processed and committed to the target. If the watermark is advanced before a successful write, records can be skipped.

### Important implementation considerations

The transcript explains the watermark concept and demonstrates filtering an updated row. For production, also decide how to handle:

- Rows with identical timestamps.
- Late-arriving records.
- Deletes.
- Time-zone consistency.
- Failed/retried runs.
- Multiple changes to the same row between runs.
- Overlapping extraction windows and duplicate handling.

These are production design considerations to validate; they are not all fully implemented in the transcript.

---

## 13. Logic Apps notifications

The tutorial creates an Azure Logic App and calls it from ADF using a Web activity after a successful pipeline run.

### Flow

```text
ADF pipeline succeeds
        |
        v
ADF Web activity
        |
        v
Logic Apps HTTP trigger
        |
        v
Send email/notification
```

The instructor chooses a multi-tenant hosting plan for the practice setup to reduce cost.

### What the notification communicates

The demo sends a message indicating that the pipeline completed successfully. The transcript shows the instructor receiving an email notification.

### Practical use

A production notification can include:

- Pipeline name.
- Run ID.
- Start/end time.
- Status.
- Number of rows processed.
- Error summary (for failures).
- Link to the ADF Monitor run.

The transcript focuses on a successful-run notification. A failure branch and richer alert payload should be added if required.

---

## 14. On-premises CSV ingestion and SHIR

The CSV source represents files located on a local/on-premises machine.

### CSV files in the demo

- Drivers.
- Promotions.
- Zones (verify the exact source filename against the instructor's metadata file).

### Why SHIR is required

ADF cloud services cannot automatically read arbitrary files on a user's local machine or a private on-premises network. A **Self-hosted Integration Runtime** is installed on a machine that can reach those files and communicate with ADF.

### SHIR setup sequence

1. Create a self-hosted Integration Runtime in ADF.
2. Download/install the runtime on a Windows machine that can access the source folder.
3. Register the runtime using the authentication key generated by ADF.
4. Confirm the runtime is online/available in ADF.
5. Create a file-system linked service.
6. Select the self-hosted IR for that linked service.
7. Configure the local folder path and credentials/access required by the runtime host.
8. Create a CSV dataset.
9. Configure the ADLS Gen2 sink dataset.
10. Use Lookup + ForEach + Copy to process the configured files dynamically.

### CSV metadata-driven flow

The tutorial stores a JSON configuration file in ADLS with file names and target folders. ADF reads this configuration using Lookup, loops through the returned rows, and copies each file to its corresponding landing folder.

```text
JSON file metadata in ADLS
          |
          v
        Lookup
          |
          v
        ForEach
          |
          v
 Copy from local file system (SHIR)
          |
          v
 ADLS Gen2 CSV landing folders
```

### Reuse

The instructor emphasizes reusing linked services and datasets where possible, and using consistent names so multiple ingestion pipelines can share connection definitions.

---

## 15. REST API ingestion

The third source is a REST API. The tutorial creates a mock API and exposes multiple endpoints for the project.

### Endpoints named in the transcript

- Deliveries.
- Order status.
- Payments.
- Ratings.
- Watermark-related endpoint/configuration.

The API includes pagination. The instructor notes that pagination is important because an API may not return all records in one response.

### API ingestion pattern

1. Configure an API linked service with the base URL and required authentication/connection information.
2. Create a REST source dataset.
3. Store endpoint metadata (endpoint name, relative URL and target folder) in a configuration file/table.
4. Use Lookup to read endpoint metadata.
5. Use ForEach to iterate through endpoint configurations.
6. Use Copy activity to call the relevant relative URL and land the response in ADLS Gen2.
7. Verify that each endpoint's output appears in the expected API folder.

### Relative URL concept

The base URL identifies the API service. A relative URL identifies a resource beneath that base.

Conceptual example:

```text
Base URL:     https://<api-host>
Relative URL: payments
Final path:   https://<api-host>/payments
```

The actual endpoint paths, API code, pagination settings and authentication details must be taken from the instructor's API source code/configuration. They are not fully specified in the transcript.

### API incremental processing

The instructor discusses watermark handling for API endpoints. The transcript indicates that the demo's API watermark value is manually maintained for some endpoints, rather than being fully automated by the API. Treat that as a limitation of the demo.

For a production API, verify whether the API supports:

- Page number/page size or continuation tokens.
- Time-based filters.
- Updated-since parameters.
- Stable ordering.
- Rate limits and retry behavior.
- Authentication token renewal.
- Delete/change events.

---

## 16. Databricks workspace, Unity Catalog and storage access

The tutorial moves downstream processing into Azure Databricks.

### Databricks concepts used

- Workspace.
- Catalog.
- Schema.
- Tables.
- Compute.
- Jobs.
- Notebooks.
- Access Connector.
- Storage credential.
- External location.

### Access Connector and ADLS

The instructor creates/uses an Azure Databricks Access Connector and assigns the storage data role to its managed identity.

High-level sequence:

1. Create or identify the Databricks Access Connector.
2. Open its properties and copy the resource/connector ID as needed.
3. Assign the required Storage Blob Data Contributor role to the connector's managed identity on the storage account.
4. In Databricks Unity Catalog, create a storage credential that uses the Access Connector identity.
5. Create an external location pointing to the relevant ADLS Gen2 path.
6. Use the external location for governed access to lake storage.

### Unity Catalog organization

The tutorial creates a catalog and uses schemas to organize project tables. It refers to a Silver storage location and a Silver schema for transformed tables.

Conceptually:

```text
Catalog
└── Schema: silver
    ├── silver_customers
    ├── silver_orders
    ├── silver_order_items
    ├── silver_menu_items
    └── silver_restaurants
```

The instructor also explains that catalogs/schemas/tables/views/functions are part of the Databricks data organization model.

### Important distinction

- **External location:** Unity Catalog object that represents a governed cloud storage path.
- **Storage credential:** Defines the identity/credential used to access that storage.
- **Access Connector:** Azure resource that provides a managed identity for Databricks access.
- **Catalog/schema:** Organize data objects in Unity Catalog.

---

## 17. Databricks Git integration and utility code

The tutorial connects the Databricks workspace to Git and brings the project code/notebooks into the workspace.

### Git integration sequence shown

1. Generate a Git access token/credential with the Git provider.
2. In Databricks user settings, add the Git credential.
3. Create/open the Git folder/repository in the Databricks workspace.
4. Synchronize the repository so notebooks and code are available in the workspace.
5. Organize the project notebooks and reusable utility module.

> Do not paste access tokens into notebooks or commit them to Git. Use the workspace's supported credential/secret mechanism and rotate any token that has been exposed.

### Utility module

The tutorial creates a common utility module and imports it into entity-specific notebooks.

The goal is to avoid repeating common code in every notebook. The transcript describes common functionality for reading data and handling shared operations, while the transformations/business rules differ by entity.

Conceptual structure:

```text
project/
├── utilities/
│   └── common_utils.py
├── bronze/
│   └── ...
├── silver/
│   ├── silver_customers.py
│   ├── silver_orders.py
│   ├── silver_restaurants.py
│   └── ...
└── dispatcher/
    └── silver_dispatcher.py
```

This is an illustrative organization, not a guaranteed copy of the instructor's exact repository layout.

---

## 18. Bronze and Silver processing with PySpark

The tutorial describes the Bronze and Silver layers and implements PySpark notebooks for downstream processing.

### Bronze layer

The Bronze layer is the raw/landing representation of source data. It preserves ingested data in the lake before applying entity-specific business transformations.

The tutorial's overall flow lands source data in ADLS and then processes it in Databricks. It discusses Delta Lake storage and catalog-managed tables as part of the lakehouse implementation.

### Silver layer

The Silver layer contains standardized, cleaned and validated data.

Common transformation tasks described:

1. Read the relevant source landing file.
2. Inspect/display the data and schema.
3. Standardize column names.
4. Cast business-key columns to the expected data types.
5. Remove duplicate records based on business keys.
6. Apply entity-specific business filters/rules.
7. Write the cleaned data as a Delta table in the Silver schema.

### Example entities and rules described

| Entity | Transformation examples mentioned |
|---|---|
| Customers | Standardize columns and data types, deduplicate by customer/business key, retain active customers |
| Orders | Deduplicate using order ID, standardize payment status to uppercase, filter for delivered orders where required |
| Restaurants | Deduplicate by restaurant ID, standardize fields, retain active restaurants |
| Other entities | Apply common standardization and entity-specific rules based on the source mapping |

The tutorial also mentions checking data types and handling date/time columns consistently for downstream analytics.

### DataFrame transformation pattern

The exact code should be copied from the instructor's repository if available. A simplified PySpark illustration of the concepts is:

```python
from pyspark.sql import functions as F

# Read a landed file (illustrative path and format).
df = (
    spark.read
         .format("parquet")
         .load("<adls-landing-path>")
)

# Standardize a status field.
df = df.withColumn("payment_status", F.upper(F.col("payment_status")))

# Cast a business key to the expected type.
df = df.withColumn("order_id", F.col("order_id").cast("int"))

# Deduplicate using the business key.
df = df.dropDuplicates(["order_id"])

# Example business filter; use only if it matches the actual requirement.
df = df.filter(F.col("order_status") == "delivered")
```

> **Illustration only:** The tutorial's actual input formats, column names, source paths and transformations vary by entity. Do not paste this example unchanged into the project.

---

## 19. Business rules and data quality

The tutorial stresses that technical ingestion is not the same as producing business-ready data. Each entity needs the right business rules.

### Examples discussed

- Retain active customers rather than inactive customers.
- Retain active restaurants.
- Standardize payment status values to uppercase.
- Retain delivered orders where the reporting requirement calls for delivered orders.
- Deduplicate rows based on the business key.
- Cast key columns to the expected numeric data type.
- Standardize column names and types consistently.

### Generic versus entity-specific workflows

The instructor explains this design decision:

- If the business rules are the same for every entity, a generic workflow may be suitable.
- If business rules differ substantially by entity, separate entity-specific notebooks/workflows are more appropriate.

In the demo, common read/standardization/write logic is reused, but business transformation logic is entity-specific.

### Data validation checks to perform

- Is the expected business key populated?
- Are business keys unique after deduplication?
- Are columns using the expected data types?
- Are status values standardized?
- Are only allowed business statuses present?
- Does the target row count reconcile with the source/filter logic?
- Did the notebook write to the expected schema/table?
- Are unexpected nulls or malformed values present?

---

## 20. Delta Lake: overwrite, append, ACID and time travel

The tutorial uses Delta format for Databricks-managed tables.

### Delta format

Delta Lake adds transaction-log-based table management on top of Parquet data files. The instructor highlights features such as ACID transactions, schema enforcement and time travel.

### Overwrite versus append

| Write mode | Typical use in the tutorial |
|---|---|
| Overwrite | Initial load / rebuilding a target table |
| Append | Adding newly arrived incremental rows to an existing table |
| Merge/upsert | A possible approach when existing records need to be updated as well as new records inserted |

The video explains that overwriting the entire table for every small incremental batch is wasteful. For example, if a table already contains one million rows and the next run has 100 new rows, append avoids rewriting the full million rows.

### Illustrative PySpark write pattern

```python
# Initial load: replace/create the target table.
df.write.format("delta").mode("overwrite").saveAsTable(
    "project_catalog.silver.silver_orders"
)

# Incremental insert-only example: add new rows.
df.write.format("delta").mode("append").saveAsTable(
    "project_catalog.silver.silver_orders"
)
```

This is a simplified illustration. In a real incremental pipeline, use a deduplication or MERGE strategy if source records can be updated, not merely inserted.

### Delta features discussed

- **ACID transactions:** A write is committed as a transaction; incomplete/failed transactions should not appear as a partially committed table update.
- **Schema enforcement:** Helps ensure incoming data follows the table's schema/data types.
- **Time travel:** Allows querying/restoring prior table versions while the required history and data files remain available under the retention settings.

The instructor mentions a seven-day time-travel example. Actual retention and vacuum settings depend on the workspace/table configuration; do not assume every table is guaranteed to retain seven days.

---

## 21. Dispatcher notebooks and Databricks jobs

The tutorial uses dispatcher notebooks to select and invoke the correct entity-specific transformation notebook.

### Why a dispatcher?

Instead of making ADF know every notebook path and maintain a separate orchestration step for every entity, the dispatcher receives an entity/table parameter and maps it to the relevant notebook.

### Dispatcher concept

```text
ADF passes entity/table name
           |
           v
   Dispatcher notebook
           |
           v
    Notebook mapping
           |
           v
  Entity-specific notebook
           |
           v
     Silver Delta table
```

The tutorial uses a notebook map/dictionary and Databricks utilities to invoke the selected notebook. It discusses `dbutils.notebook.run` as the notebook invocation mechanism.

Illustrative pattern:

```python
notebook_map = {
    "customers": "/Project/Silver/silver_customers",
    "orders": "/Project/Silver/silver_orders",
    "restaurants": "/Project/Silver/silver_restaurants",
}

entity = dbutils.widgets.get("table_name")

target_notebook = notebook_map[entity]

dbutils.notebook.run(
    target_notebook,
    timeout_seconds=0,
    arguments={"table_name": entity}
)
```

This is an example of the dispatcher pattern, not the exact code from the instructor's repository.

### Databricks Jobs

The instructor creates jobs for the source-specific downstream workflows, including SQL, CSV/on-premises and API paths.

The job:

- Points to a dispatcher notebook.
- Accepts a parameter such as table name/entity name.
- Runs on configured compute.
- Can be triggered by ADF.
- Produces a run record/status that can be monitored and debugged.

---

## 22. ADF-to-Databricks orchestration

The tutorial integrates the ADF ingestion pipelines with Databricks jobs.

### Integration sequence

1. Create the Databricks linked service in ADF.
2. Configure the workspace connection and authentication.
3. Ensure ADF has permission to invoke the Databricks resource/job.
4. Create the Databricks job with its dispatcher notebook.
5. Configure the job parameter expected from ADF.
6. In ADF, use a Databricks activity after the source ingestion succeeds.
7. Pass the entity/table name or file identifier from the ForEach metadata row.
8. Monitor the ADF run and Databricks job run.

### Passing parameters

The tutorial emphasizes passing the current metadata row's entity name from the upstream ADF pipeline to the downstream Databricks job. This allows one job/dispatcher to select the appropriate entity notebook.

### Decoupling upstream and downstream

This is an important production concept:

- Upstream ADF ingestion lands data in ADLS.
- Downstream Databricks processing reads the already-landed data.
- If a Silver notebook fails, repair/re-run the affected downstream entity job.
- Do not re-run the entire ingestion pipeline when the source data has already landed successfully and does not need to be re-extracted.

This reduces unnecessary source reads and makes failure recovery more targeted.

---

## 23. Troubleshooting and operational practices

The tutorial includes practical debugging examples and emphasizes checking each stage separately.

### ADF checks

- Validate the pipeline before debugging/running it.
- Check linked-service test connections.
- Confirm the managed identity has the required source/sink permissions.
- Confirm the correct Integration Runtime is selected.
- For Lookup, verify whether **First row only** is enabled or disabled as intended.
- Inspect Lookup output to confirm the metadata array contains all expected rows.
- Check ForEach input and current item values.
- Confirm dataset parameters receive the expected folder/table/file values.
- Verify the output files exist in the expected ADLS folder.
- Use ADF Monitor to inspect activity status and errors.

### SHIR checks

- Confirm SHIR is installed and registered.
- Confirm its status is online.
- Confirm the SHIR host can access the local folder.
- Check file-system permissions for the account used by the runtime.
- Confirm the linked service is configured to use SHIR, not AutoResolve IR.

### REST API checks

- Test the base URL and relative endpoint.
- Verify authentication.
- Verify pagination settings.
- Check endpoint metadata and target folder mapping.
- Review API response/error details and rate limits.

### Databricks checks

- Confirm the Access Connector identity has storage permissions.
- Confirm the storage credential and external location are configured correctly.
- Confirm the catalog/schema exists and the notebook has the required privileges.
- Check the notebook path in the dispatcher mapping.
- Confirm the job parameter name matches the value passed from ADF.
- Inspect the failing entity notebook directly.
- Re-run only the failed downstream task when the source landing data is already present.
- Reconcile row counts and inspect the target table after a successful run.

### Compute start-up time

The video compares serverless compute and job clusters and demonstrates that job clusters may take time to start. A job cluster can terminate after the run, so subsequent runs may incur startup time again. Select compute based on workload, governance, latency and cost requirements.

---

## 24. Gold layer and reporting

The tutorial has a final Gold-layer phase after implementing Bronze and Silver. It discusses building downstream business-ready data and integrating Power BI with the Gold layer.

### Layer responsibilities

| Layer | Purpose |
|---|---|
| Bronze | Raw/landed source representation |
| Silver | Cleaned, standardized and validated entity-level data |
| Gold | Business-ready data models/aggregations for analytics and reporting |

### Gold layer

The instructor introduces Gold as the next phase after Bronze and Silver and begins implementing it using Databricks notebooks/jobs. The transcript's final chapter contains the Gold implementation walkthrough, but the detailed business definitions and exact final table schemas should be checked against the notebook/code supplied with the tutorial.

### Power BI

Power BI is identified as the reporting and stakeholder-consumption layer. The architecture places it downstream of the Gold layer so reports consume curated business data rather than raw source files.

Typical reporting outputs could include sales, order status, delivery performance, payments and restaurant performance, depending on the business requirements and final Gold model.

---

## 25. Project execution sequence

Use this sequence when reproducing the tutorial.

### Phase 1 — Understand and design

- [ ] Identify SQL, CSV and REST API sources.
- [ ] Collect table/file/endpoint definitions.
- [ ] Create source-to-target mapping.
- [ ] Identify business keys and watermark columns.
- [ ] Design the ADLS folder layout.
- [ ] Define initial and incremental load behavior.

### Phase 2 — Azure foundation

- [ ] Create resource group.
- [ ] Create ADF.
- [ ] Create ADLS Gen2 storage account with hierarchical namespace.
- [ ] Create the `branch` container and source folders.
- [ ] Create Azure SQL Database.
- [ ] Create and populate source tables.
- [ ] Create/populate the pipeline control table.

### Phase 3 — ADF SQL ingestion

- [ ] Configure SQL linked service.
- [ ] Create ADF managed-identity user in Azure SQL.
- [ ] Grant required SQL read permission.
- [ ] Configure ADLS linked service.
- [ ] Assign Storage Blob Data Contributor to ADF identity.
- [ ] Create source and sink datasets.
- [ ] Build and test single-table initial load.
- [ ] Parameterize datasets.
- [ ] Build Lookup + ForEach + Copy metadata-driven pipeline.
- [ ] Build incremental-load logic and watermark update.

### Phase 4 — Notifications and file ingestion

- [ ] Create Logic App.
- [ ] Configure HTTP trigger and notification action.
- [ ] Call Logic App from ADF Web activity.
- [ ] Create/register SHIR on a machine that can access the CSV files.
- [ ] Configure file-system linked service and CSV dataset.
- [ ] Create CSV metadata configuration.
- [ ] Build Lookup + ForEach + Copy pipeline for CSV files.
- [ ] Validate files in ADLS.

### Phase 5 — REST API ingestion

- [ ] Review API base URL and endpoint definitions.
- [ ] Verify authentication.
- [ ] Verify pagination.
- [ ] Create REST linked service and dataset.
- [ ] Create endpoint metadata.
- [ ] Build Lookup + ForEach + Copy API pipeline.
- [ ] Verify API output in ADLS.
- [ ] Validate incremental/watermark behavior.

### Phase 6 — Databricks foundation

- [ ] Create/identify Databricks workspace.
- [ ] Create/identify Access Connector.
- [ ] Assign storage permissions to its managed identity.
- [ ] Configure Unity Catalog storage credential.
- [ ] Configure external location.
- [ ] Create project catalog and schemas.
- [ ] Configure Git credentials and repository.
- [ ] Create common utility module.

### Phase 7 — Transformation and orchestration

- [ ] Build Bronze processing.
- [ ] Create Silver entity notebooks.
- [ ] Standardize columns and types.
- [ ] Deduplicate using business keys.
- [ ] Apply entity-specific business rules.
- [ ] Write Delta tables.
- [ ] Use overwrite for initial load and append/merge as appropriate for incremental loads.
- [ ] Create dispatcher notebooks.
- [ ] Create Databricks jobs for SQL, CSV and API processing.
- [ ] Configure ADF Databricks linked service/activity.
- [ ] Pass entity parameters from ADF.
- [ ] Test failure recovery and targeted reruns.

### Phase 8 — Gold and reporting

- [ ] Define business-ready Gold data models.
- [ ] Create Gold transformation notebooks/jobs.
- [ ] Validate Gold output.
- [ ] Connect Power BI to the Gold data.
- [ ] Build and validate stakeholder reports.

---

## 26. Interview revision questions

### ADF

**1. What is a linked service?**  
A connection definition used by ADF to connect to a data store or compute service.

**2. What is a dataset?**  
A reference to the data structure/location used by an activity, such as a SQL table or ADLS file.

**3. Linked service versus dataset?**  
Linked service defines how to connect; dataset identifies which data to read or write.

**4. What is an Integration Runtime?**  
The ADF infrastructure used to provide data movement, activity dispatch and connectivity to data stores/compute.

**5. AutoResolve IR versus SHIR?**  
AutoResolve is used for supported cloud connectivity/data movement. SHIR is installed in a network that can access on-premises or private sources.

**6. What does Copy Data do?**  
Moves data from a configured source to a configured sink.

**7. Lookup versus Get Metadata?**  
Lookup reads rows/data from a source query or table. Get Metadata reads properties about a file/folder/dataset object.

**8. Why use Lookup + ForEach?**  
Lookup reads a list of configuration rows; ForEach iterates through them so the same pipeline can process many entities.

**9. Why parameterize datasets?**  
To supply table names, file names and folders dynamically instead of hard-coding one source/target.

### Incremental processing

**10. What is a watermark?**  
A stored high-water-mark value used to identify records that are newer than the last successful processing boundary.

**11. How does a timestamp watermark work?**  
Read the previous watermark, filter source rows where the update timestamp is greater, process them, and update the watermark after successful processing.

**12. Why maintain a control table?**  
To store entity metadata, load configuration, last successful watermark and execution/audit details.

**13. When should the watermark be updated?**  
After the corresponding data has been successfully committed to the target.

**14. What is the risk of a timestamp-only watermark?**  
Rows with equal timestamps, late-arriving updates and deletes can be missed unless tie-breakers, overlap windows or another change-tracking mechanism is designed.

### Databricks and Delta Lake

**15. Why use Bronze, Silver and Gold?**  
They separate raw ingestion, cleaned/conformed data and business-ready analytics data.

**16. Why use Delta Lake?**  
It provides transactional table behavior and capabilities such as ACID transactions, schema enforcement and time travel.

**17. Overwrite versus append?**  
Overwrite replaces existing table data; append adds new rows. The correct mode depends on whether the load is a rebuild or an incremental insert.

**18. When is MERGE better than append?**  
When incoming data can contain updates to existing business keys and the target must be upserted rather than simply extended.

**19. What is deduplication?**  
Removing repeated records based on a business key or defined uniqueness criteria.

**20. Why use a dispatcher notebook?**  
It maps a parameter such as an entity name to the correct transformation notebook and reduces repeated orchestration configuration.

**21. Why create Databricks jobs?**  
To schedule/orchestrate notebook execution, parameterize runs and monitor their status independently from ingestion.

**22. Why decouple ADF and Databricks?**  
A downstream transformation can be repaired or rerun without re-extracting source data that has already landed successfully.

### Azure security and operations

**23. What is a managed identity?**  
An Azure-managed identity that allows a resource to authenticate to supported services without embedding a password or client secret in code.

**24. Why does ADF need permissions on both SQL and ADLS?**  
It needs source read access to extract data and sink write access to land it.

**25. What is the Databricks Access Connector used for?**  
It provides a managed identity that Databricks/Unity Catalog can use to access Azure storage.

**26. What is an external location?**  
A Unity Catalog object that represents a governed cloud storage path and uses a storage credential.

**27. Why monitor row counts and pipeline status?**  
To audit execution, identify failed or incomplete loads and reconcile source and target data.

---

## 27. Production considerations and gaps to validate

These are engineering checks to apply when turning the tutorial into a real production platform. They are additional implementation guidance, not claims that every item is fully implemented in the video.

### Security

- Use managed identities wherever supported.
- Store API credentials and tokens in a secret manager, not in notebooks or Git.
- Apply least-privilege Azure RBAC and ADLS ACLs.
- Restrict network access to SQL, storage and Databricks.
- Avoid granting broad contributor permissions when narrower access is sufficient.
- Rotate credentials and audit access.

### Reliability

- Update watermarks only after successful target commit.
- Make retries idempotent.
- Use a deterministic file naming strategy and preserve run identifiers.
- Handle partial failures and duplicate delivery.
- Define a strategy for late-arriving data and source deletes.
- Add API retry/backoff and rate-limit handling.
- Make pagination robust to changing data during extraction.

### Data quality and reconciliation

- Validate schema and required columns.
- Check null business keys.
- Deduplicate using documented business keys.
- Reconcile source and target counts.
- Capture rejected/quarantined records.
- Record pipeline run IDs and row counts.
- Validate Gold metrics against business definitions.

### Performance and cost

- Choose storage redundancy and access tiers based on actual recovery and access requirements.
- Avoid creating a new compute cluster for every tiny task if startup overhead dominates.
- Set appropriate job compute sizes and auto-termination.
- Partition data only when query patterns and volume justify it.
- Avoid excessive small files.
- Monitor ADF activity duration, Databricks job duration and storage transactions.

### Governance

- Use Unity Catalog permissions for catalogs, schemas, tables and external locations.
- Separate development, test and production environments.
- Version-control notebooks and configuration.
- Keep secrets outside source control.
- Document ownership, data classification and retention.

---

## 28. Video chapter index

The supplied transcript contains these 28 chapter headings:

| # | Chapter |
|---:|---|
| 1 | Introduction |
| 2 | Linked Services |
| 3 | Resource Group Configuration |
| 4 | ADLS Gen2 Data Lake Configuration |
| 5 | Azure SQL Configuration |
| 6 | Building the SQL Source System Initial Load Pipeline |
| 7 | Control & Metadata Table Explanation |
| 8 | Metadata-Driven Implementation |
| 9 | SQL Source System Incremental Processing Pipeline |
| 10 | Azure Logic Apps Configuration |
| 11 | On-Premises CSV File Migration |
| 12 | SHIR Configuration |
| 13 | REST API Data Migration Pipeline |
| 14 | PySpark Transformation Code for All SQL Source Tables |
| 15 | Creating a Utility Module in Databricks |
| 16 | Importing Utilities into Databricks Notebooks |
| 17 | Business Rules & Data Validation |
| 18 | Overwrite vs Append Mode in Databricks PySpark |
| 19 | Dispatcher Notebook Explained |
| 20 | Creating a Databricks Job for the SQL Source System |
| 21 | Databricks Job Execution Process |
| 22 | Dispatcher Notebooks for API & On-Premises Sources |
| 23 | Creating a Databricks Job for the On-Premises Source |
| 24 | Data Engineering Interview Scenario |
| 25 | Creating a Databricks Job for the API Source |
| 26 | Serverless Compute vs Job Cluster |
| 27 | Gold Layer Introduction |
| 28 | Gold Layer Implementation |

---

## Final revision summary

The main engineering pattern demonstrated by this project is:

1. **Design first:** document sources, entities, columns, keys, watermarks and source-to-target mapping.
2. **Land centrally:** use ADF to ingest SQL, on-premises CSV and REST API data into ADLS Gen2.
3. **Make ingestion reusable:** use a control/metadata table, Lookup, ForEach and parameterized datasets.
4. **Support incremental loads:** use a watermark to filter changed data and update it only after successful processing.
5. **Transform downstream:** use Databricks/PySpark to standardize, deduplicate, validate and apply business rules.
6. **Use Delta tables:** choose overwrite, append or merge based on the load semantics.
7. **Orchestrate independently:** use dispatcher notebooks and Databricks jobs, triggered by ADF with entity parameters.
8. **Recover intelligently:** rerun a failed downstream entity without repeating successful upstream ingestion.
9. **Serve analytics:** create Gold business-ready data and expose it to Power BI.
10. **Operate securely:** use managed identities, RBAC/ACLs, Unity Catalog, monitoring and secret management.

**Core interview takeaway:** The most important reusable pattern in the tutorial is the metadata-driven ADF pipeline: Lookup reads configuration, ForEach iterates through entities, parameterized datasets select the source and target dynamically, and a control table maintains load metadata and watermarks.
