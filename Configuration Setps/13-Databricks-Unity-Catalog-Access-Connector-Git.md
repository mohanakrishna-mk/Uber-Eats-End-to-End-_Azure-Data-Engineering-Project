# 13 – Databricks Workspace, Unity Catalog, Access Connector and Git (Chapters 14–16)

## 1. Create the Azure Databricks workspace

1. Azure portal → Create resource → Azure Databricks.
2. Select the same project resource group.
3. Choose workspace name, region and pricing tier.
4. Deploy and open the workspace.
5. Confirm the workspace opens and the user has permission to create notebooks and compute.

The video creates a workspace for the project and then organizes notebooks by source system.

## 2. Create a dedicated Access Connector identity

The transcript uses an Azure Databricks Access Connector so Databricks can access ADLS using a managed identity.

1. Azure portal → Create resource → Access Connector for Azure Databricks.
2. Place it in the project resource group.
3. Create it.
4. Open the Access Connector → Properties.
5. Copy the full resource ID.
6. Open the storage account/container that Databricks must access.
7. Assign the Access Connector managed identity the required data role, scoped to the relevant container. The project uses storage access for the Silver/metadata areas.

## 3. Create Unity Catalog storage credential

1. Open the Databricks workspace.
2. Open **Catalog**.
3. Go to **Credentials** / **Storage credentials**.
4. Create a storage credential.
5. Select the Azure managed identity / Access Connector option.
6. Paste the Access Connector resource ID.
7. Give the credential a project-specific name.
8. Create it.

The transcript explains that the storage credential represents the Access Connector identity used by Databricks to access ADLS Gen2.

## 4. Create the external location

The video creates an external location over the ADLS container used for metadata/Silver data.

1. Catalog → External locations → Create.
2. Give it a name (example naming in the transcript is a project-specific Silver external location).
3. Storage type: Azure Data Lake Storage.
4. URL pattern:

```text
abfss://<container>@<storage-account>.dfs.core.windows.net/<optional-folder>
```

5. Select the storage credential created in the previous step.
6. Validate the location.
7. Create it.

The transcript initially encounters a path overlap with an existing external location and changes the target to a dedicated metadata container. If you see an overlap error, inspect existing external locations and avoid creating overlapping roots. Do not force-create blindly in a production workspace.

The validation should confirm that the path exists, hierarchical namespace is enabled, and the identity can perform the required operations.

## 5. Create catalog and schemas

1. Catalog → Create catalog.
2. Choose a standard catalog.
3. Provide the project catalog name.
4. Set the managed storage location if required by your metastore setup.
5. Create the catalog.
6. Create a `silver` schema for cleansed/enriched tables.
7. Create a `gold` schema later for business dimensions/facts.

The video uses a project catalog, then the `silver` schema. The transcript also distinguishes the default/serverless catalog from the catalog/storage setup used for the project. Use the catalog names in the notebooks and job configuration consistently.

## 6. Create volumes/folders only if the notebooks require them

The transcript mentions metadata and Silver storage locations. The exact project artifacts may use workspace folders and external locations rather than the illustrative volume names in the earlier guide. Inspect the notebook paths and SQL statements before creating volumes. Do not create redundant volumes with guessed names.

## 7. Connect Git to Databricks

1. Databricks user settings → Git integration.
2. Configure GitHub authentication using the supported secure method.
3. Open Workspace → Git folders → Add Git folder.
4. Paste the repository URL.
5. Clone.
6. Confirm `notebooks/`, `routing/` and utility files are present.
7. Keep notebooks under source-specific folders.

Do not hardcode or commit Git tokens. The instructor's user-folder name is personal and should not be copied.

## 8. Organize notebooks by source

The transcript explains that SQL, CSV and API sources need source-specific downstream workflows because schemas and business rules differ.

Recommended organization following the project:

```text
Workspace/
  Uber-Eats-Project/
    notebooks/
      sql/
      csv/
      api/
      gold/
    routing/
      dispatcher_sql
      dispatcher_csv
      dispatcher_api
      dispatcher_gold
    utilities/
```

Use the actual paths/names in the repository if they differ.

## 9. Permissions

- ADF identity needs permission to invoke the Databricks workspace/job.
- Databricks Access Connector identity needs ADLS data access.
- Databricks users/groups need Unity Catalog `USE CATALOG`, `USE SCHEMA`, and table/volume privileges as appropriate.
- Power BI/reporting consumers should later receive Gold-only access, not broad Silver access.

## Completion checklist

- [ ] Databricks workspace created.
- [ ] Access Connector created.
- [ ] Access Connector role assigned to ADLS.
- [ ] Storage credential created.
- [ ] External location validated.
- [ ] Catalog and Silver schema created.
- [ ] Git folder cloned.
- [ ] Notebook folders organized by source.
- [ ] ADF-to-Databricks permissions planned.
