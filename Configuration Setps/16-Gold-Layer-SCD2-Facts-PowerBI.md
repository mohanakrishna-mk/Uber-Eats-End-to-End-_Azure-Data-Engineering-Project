# 16 – Gold Layer, SCD Type 2 Dimensions and Power BI (Chapters 27–28)

## 1. Why the video builds Gold

The transcript explains that Silver contains the enriched source entities (12 managed tables across SQL, CSV and API). The reporting team should not be expected to understand every source table, relationship or grain.

Gold provides a centralized, business-oriented model with conformed dimensions and fact tables. The project requirement discussed is revenue analysis by restaurant and by customer. The instructor therefore focuses the demonstrated dimensional model on SQL-source Restaurants and Customers rather than modeling every Silver entity.

## 2. Create the Gold schema

1. Open Databricks Catalog Explorer.
2. Select the project catalog.
3. Create schema `gold`.
4. Grant access only to the engineering/reporting identities that need it.
5. Keep Silver access restricted because it contains enriched source-level data.

The video uses Unity Catalog grants to prevent reporting users from seeing Silver and to expose Gold data instead.

## 3. Create the Restaurant dimension using SCD Type 2

The transcript demonstrates a Restaurant dimension sourced from:

```text
<catalog>.silver.silver_restaurants
```

Target concept:

```text
<catalog>.gold.dim_restaurants
```

The dimension tracks historical changes using:
- A surrogate key (generated using a row-number/window pattern).
- Business/natural key (restaurant ID).
- Hash/SHA value built from attributes to detect changes.
- `start_date`.
- `end_date`.
- `is_current`.

Configuration/implementation sequence:
1. Read the current Silver restaurant table.
2. Identify the natural/business key (`restaurant_id`).
3. Build a hash from the descriptive attributes to detect attribute changes.
4. Compare incoming rows to current target rows by business key and hash.
5. If a business key is new, create a new dimension row.
6. If the key exists but the hash differs, expire the current row (set its end date and current flag accordingly) and insert a new version.
7. If the key and hash match, do not create a new historical version.
8. Generate a surrogate key for each new version.
9. Append new versions to the dimension table.

The transcript explicitly identifies `start_date`, `end_date`, `is_current`, SHA/hash comparison, and surrogate keys. Exact hash expression, date sentinel and merge/append code must be taken from the instructor's Gold notebook if exact reproduction is required.

## 4. Create the Customer dimension

Repeat the same SCD Type 2 pattern for Customers:
- Source: Silver customer managed table.
- Business key: `customer_id`.
- Hash of descriptive attributes.
- Surrogate key such as `customer_key`.
- `start_date`, `end_date`, `is_current`.
- Append new versions after detecting new/changed attributes.

The video demonstrates both Restaurant and Customer dimensions because revenue analysis is required by restaurant and customer.

## 5. Handle initial load and incremental load

The transcript says the Gold notebook must support both initial and incremental processing:
- Initial load: initialize/populate the dimension when the target table does not yet exist.
- Incremental load: compare incoming Silver data with the current Gold dimension and add new/changed versions.
- The notebook includes an initial-load branch and an incremental branch so it can run on repeated pipeline executions.

Ensure the initial-load path does not create duplicate dimension rows on subsequent runs.

## 6. Gold fact model

The transcript discusses combining dimensions and fact tables into a star-schema/curated model for revenue reporting. It refers to joining customer and restaurant dimensions with the fact table.

The spoken walkthrough does not provide the complete fact-table DDL, grain, measure definitions or every join key. Use the source-to-target mapping and Gold notebooks in the repository for exact facts and relationships. Before implementing, define:
- Fact grain (one row per order, order item, delivery, or another agreed grain).
- Revenue definition and currency handling.
- Customer and restaurant surrogate-key lookups.
- Event/order date key if required.
- Treatment of cancelled, failed or non-delivered orders.
- Additive measures and reconciliation rules.

Do not invent a revenue formula without the business definition.

## 7. Orchestrate Gold from the Silver job

The video attaches a Gold dispatcher notebook as a downstream task in the Databricks workflow/job.

1. Create the Gold dispatcher notebook.
2. Create the Gold dimension notebook(s).
3. Add a Gold task to the existing Silver/Gold job workflow, or create the appropriate dependent job structure used by your workspace.
4. Set task dependency so Gold runs after the relevant Silver tasks succeed.
5. Pass the entity/table parameter consistently through ADF → job → dispatcher → Gold notebook.
6. For Silver entities not included in the Gold model (the transcript mentions Menu Items as an example), skip the Gold task for that entity rather than failing the whole model.
7. Validate that Restaurant and Customer dimensions are created/updated.

The transcript shows the SQL incremental pipeline triggering the Silver job and then the Gold dispatcher. The Gold layer is not a transformation for every Silver entity.

## 8. Power BI connection

The transcript connects Power BI Desktop to Databricks using:
- Server hostname.
- HTTP path.
- Databricks authentication credential (the video temporarily creates a personal access token for the demonstration).
- DirectQuery mode rather than Import mode.

Steps:
1. In Databricks, open the SQL Warehouse/compute connection details used for BI.
2. Copy Server Hostname and HTTP Path.
3. In Power BI Desktop → Get Data → Azure Databricks.
4. Enter the hostname and HTTP path.
5. Choose the supported authentication method.
6. Select DirectQuery as demonstrated.
7. Select the Gold schema/tables or curated views.
8. Build the report on top of Gold.

Security:
- Do not expose a PAT in screenshots, notebooks or Git.
- Prefer the supported organizational identity/service principal authentication for production.
- If a PAT is used for a temporary demo, keep it scoped, short-lived and revoke it afterwards.

## 9. Unity Catalog permissions for reporting

The instructor emphasizes that reporting users should generally see Gold, not all Silver tables.

1. Catalog Explorer → Gold schema → Permissions.
2. Grant the reporting group only the required `USE CATALOG`, `USE SCHEMA` and `SELECT` permissions on approved Gold tables/views.
3. Do not grant broad Silver access to the Power BI/reporting identity.
4. Consider exposing curated views/materialized views rather than raw dimension/fact tables when that better fits the security model.

## 10. Gold validation

- Confirm SCD2 current row uniqueness per business key.
- Confirm historical rows have correct start/end dates.
- Confirm only one current version per entity key.
- Confirm hash changes create new versions.
- Reconcile dimension keys against Silver business keys.
- Validate fact grain and revenue totals.
- Validate report totals against Gold SQL queries.
