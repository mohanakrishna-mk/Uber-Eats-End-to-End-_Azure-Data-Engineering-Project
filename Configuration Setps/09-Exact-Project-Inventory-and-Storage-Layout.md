# 09 – Exact Project Inventory and ADLS Layout (Transcript Chapters 1–5)

This guide captures the project-specific source inventory and initial Azure setup. It supplements the generic foundation guide.

## 1. Confirm the source systems before provisioning

The video uses three source systems:

| Source | Objects shown in the project | Ingestion route |
|---|---|---|
| Azure SQL Database | Customers, Restaurants, Menu Items, Orders, Order Items (the SQL/control metadata is separate) | ADF Azure SQL connector |
| On-premises CSV | `drivers.csv`, `promotions.csv`, `zones.csv` | ADF File System connector through SHIR |
| REST API | `payments`, `deliveries`, `ratings`, `order-status` | ADF REST connector |

The transcript describes five SQL business entities in the control-table output. The repository also includes the SQL scripts and mapping documents; use those artifacts to confirm the complete source DDL and whether any additional SQL entity is configured in your checked-out revision.

## 2. Review source-to-target mapping first

Before creating pipelines, open the repository's `Source to target Mapping.docx`.

For every source entity, record:
- Source system.
- Source schema/table or endpoint/file.
- Source columns and data types.
- Target folder/table.
- Primary/business key.
- Foreign keys and parent-child relationship.
- Created/updated timestamp columns.
- Status columns used in business rules.
- Required reporting measures/KPIs.

Do not start with Copy activity and guess the columns. The video explicitly starts with source/table/column mapping and uses timestamp, status and key columns for downstream analytics and incremental processing.

## 3. Create the resource group and ADF

The video creates a resource group first, then deploys ADF and ADLS Gen2 into it.

1. Azure portal → Resource groups → Create.
2. Select the subscription and region.
3. Use a project-specific name (the instructor uses an Uber Eats project name; use your own unique name).
4. Open the resource group.
5. Create **Azure Data Factory**.
6. Choose the resource group, region and unique factory name.
7. The instructor keeps the other ADF creation tabs at their defaults.
8. Create and open the factory → Launch Studio.

## 4. Create ADLS Gen2

1. Resource group → Create → Storage account.
2. Choose a globally unique storage account name.
3. On the advanced settings, enable **Hierarchical namespace**. The instructor explicitly enables it so folders and subfolders can be organized.
4. The video chooses the **Cool** access tier for this project. Consider the access-frequency/cost trade-off before copying that choice for frequently accessed production data.
5. Keep other settings at defaults as demonstrated, unless your network/security requirements differ.
6. Review + create.

## 5. Create the Bronze container and source folders

The transcript's automatic captions render “branch” in places; the intended medallion layer is **Bronze**. The video uses a container named `bronze` and organizes source data below it.

Create the `bronze` container, then create these source directories:

```text
bronze/
  sql/
  csv/
    drivers/
    promotions/
    zones/
  api/
    payments/
    deliveries/
    ratings/
    order_status/
```

The exact folder targets for CSV/API are also confirmed in the repository metadata JSON files. Use the JSON values as the source of truth:
- `csv_metadata_proper (2).json`
- `api_metadata_v2 (2).json`

The video also places metadata JSON files in a metadata area so ADF can read configuration dynamically. Keep configuration/metadata separate from business landing files.

## 6. Create Azure SQL Database

1. Resource group → Create → Azure SQL.
2. Create the logical SQL server and database.
3. Record the server name, database name, region and authentication details. The instructor explicitly says to keep these values because they are needed later in ADF linked-service configuration.
4. Configure networking/firewall so the query editor and ADF can reach SQL.
5. Open Query editor.
6. Run the repository SQL scripts / insert statements to create and populate the business tables.
7. Create the additional `dbo.pipeline_control` table used for pipeline metadata, auditing and incremental watermark state.

The repository has these supporting files:
- `SQL_Migration_Step_by_Step.docx`
- `Uber_Eats_SQL_Insert_Statements.docx`
- `Uber_Eats_SQL_Source_and_Incremental_Loading.docx`
- `Source to target Mapping.docx`

Use those for exact DDL, column names, data types and sample data. This guide does not fabricate a replacement schema.

## 7. Completion validation

- [ ] Three source systems identified.
- [ ] Source-to-target mapping reviewed.
- [ ] Resource group created.
- [ ] ADF created.
- [ ] ADLS Gen2 created with hierarchical namespace.
- [ ] `bronze` container created.
- [ ] SQL/CSV/API landing directories created.
- [ ] Azure SQL database created and reachable.
- [ ] Business tables populated from repository scripts.
- [ ] `dbo.pipeline_control` created and populated.
