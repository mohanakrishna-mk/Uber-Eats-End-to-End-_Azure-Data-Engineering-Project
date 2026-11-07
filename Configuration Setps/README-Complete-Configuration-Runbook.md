# Uber Eats Azure Data Engineering – Complete Configuration Runbook

This expanded companion contains the original foundation guides plus transcript-specific configuration steps.

## Source of truth

- The 28-chapter transcript supplied in the conversation.
- Public repository: https://github.com/srirama9908-lang/Uber-Eats-End-to-End-_Azure-Data-Engineering-Project
- Exact metadata files: `csv_metadata_proper (2).json`, `api_metadata_v2 (2).json`.
- Supporting docs: `SQL_Migration_Step_by_Step.docx`, `Uber_Eats_SQL_Source_and_Incremental_Loading.docx`, `ADF_SHIR_CSV_PowerShell_Setup.docx`, `REST_API_Migration_Step_by_Step.docx`, `Source to target Mapping.docx`, `Uber_Eats_SQL_Insert_Statements.docx`.

## Recommended order

1. `09-Exact-Project-Inventory-and-Storage-Layout.md`
2. `02-ADLS-ADF-Linked-Services-and-Datasets.md`
3. `10-ADF-SQL-Initial-Metadata-Incremental-and-Alerts.md`
4. `11-CSV-ADF-SHIR-Exact-Configuration.md`
5. `12-REST-API-Metadata-KeyVault-Pagination.md`
6. `13-Databricks-Unity-Catalog-Access-Connector-Git.md`
7. `14-PySpark-Utilities-Business-Rules-and-Delta-Writes.md`
8. `15-Dispatcher-Notebooks-Databricks-Jobs-and-ADF-Integration.md`
9. `16-Gold-Layer-SCD2-Facts-PowerBI.md`

The earlier files `00` through `08` remain as supplementary references.

## Transcript chapter mapping

| Transcript chapter | Configuration guide |
|---|---|
| 1–5: Architecture, linked services, resource group, ADLS, SQL | 09, 02 |
| 6–8: SQL initial load, control/metadata, metadata-driven pipeline | 10 |
| 9: SQL incremental processing | 10 |
| 10: Logic Apps | 10 |
| 11–12: CSV and SHIR | 11 |
| 13: REST API migration | 12 |
| 14–16: Databricks, catalog, storage access, utilities/Git | 13, 14 |
| 17–18: Business rules, validation, overwrite/append | 14 |
| 19–26: Dispatchers, jobs, execution, source-specific jobs, compute | 15 |
| 27–28: Gold model, SCD Type 2, Power BI | 16 |

## Accuracy notes

- Repository metadata gives exact CSV filenames/target folders and API endpoint names/relative URLs/target folders. Preserve those keys when building Lookup → ForEach.
- The transcript names five SQL business tables in the SQL job output: customers, restaurants, menu_items, orders and order_items. The SQL control table is separate. Verify the complete DDL and any extra entity from the repository's SQL/mapping documents.
- The transcript's automatic captions sometimes render “bronze” as “branch”; the intended landing layer is Bronze.
- The transcript describes an API watermark mechanism but does not state every exact watermark expression/schema. Use the repository API code and supporting files for those details.
- The Gold walkthrough demonstrates Restaurant and Customer SCD Type 2 dimensions. It does not fully define the fact-table grain or revenue formula; confirm these from the Gold notebooks/business mapping.
- Example code is explanatory unless explicitly identified as a repository metadata value or transcript-specific field. Compare with the checked-out files before deployment.
