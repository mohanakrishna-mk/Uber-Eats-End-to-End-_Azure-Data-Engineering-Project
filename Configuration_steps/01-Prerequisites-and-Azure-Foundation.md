# 01 – Prerequisites and Azure Foundation

## 1. Prepare your workstation

Install:
- A modern browser.
- Git.
- Visual Studio Code (recommended).
- Python 3.x if you will run the API/mock source locally.
- PowerShell 7 or Windows PowerShell for the SHIR/CSV preparation steps.
- Azure Storage Explorer (optional; useful for inspecting ADLS containers).
- SQL Server Management Studio or Azure Data Studio (optional).

Clone the project:

```bash
git clone https://github.com/srirama9908-lang/Uber-Eats-End-to-End-_Azure-Data-Engineering-Project.git
cd Uber-Eats-End-to-End-_Azure-Data-Engineering-Project
```

Inspect the repository before creating resources:

```bash
find . -maxdepth 3 -type f
```

On Windows PowerShell:

```powershell
Get-ChildItem -Recurse -File | Select-Object FullName
```

Review `Source to target Mapping.docx`, metadata JSON files, SQL migration documents, CSV files, and the `factory`, `linkedService`, `dataset`, `pipeline`, `integrationRuntime`, `notebooks`, and `routing` folders.

## 2. Create a resource group

1. Open https://portal.azure.com.
2. Search **Resource groups** → **Create**.
3. Select your subscription.
4. Resource group: `rg-ubereats-dev` (example).
5. Choose a region close to your users and supported by the services you need.
6. Select **Review + create** → **Create**.

Keep all development resources in this group so that cost review and cleanup are easier.

## 3. Create Azure Data Lake Storage Gen2

1. Search **Storage accounts** → **Create**.
2. Select the subscription and resource group.
3. Enter a globally unique lowercase storage account name, e.g. `stubereatsdev001`.
4. Choose a supported region.
5. Performance: Standard is generally adequate for a learning project.
6. Redundancy: LRS is usually the lowest-cost development choice; understand the durability trade-off before production.
7. On **Advanced**, enable **Hierarchical namespace**. This is the setting that makes the account ADLS Gen2.
8. Review and create.

### Create containers / directory structure

Open the storage account → **Containers**. Create a container such as `ubereats` (or follow the exact names used by the repository). Under it, create a logical layout such as:

```text
landing/
  sql/
  csv/
  api/
bronze/
  sql/
  csv/
  api/
silver/
gold/
checkpoints/
```

The video uses the lake for source ingestion and Databricks downstream processing. Keep raw landing data separate from transformed data. Do not mix checkpoint files with business datasets.

### Validate

Upload a small test file through the portal or Storage Explorer. Confirm it appears in the expected filesystem path. Remove the test file afterwards.

## 4. Create Azure Data Factory

1. Search **Data factories** → **Create**.
2. Select subscription/resource group/region.
3. Name it `adf-ubereats-dev`.
4. Choose the appropriate version (current ADF experience).
5. Create and open **Launch studio**.

In ADF Studio, note the four main areas:
- **Author**: linked services, datasets, pipelines, data flows.
- **Monitor**: pipeline and activity runs.
- **Manage**: linked services, Integration Runtimes, triggers, Git configuration.
- **Home**: entry point and documentation.

## 5. Create Azure SQL Database

1. Search **SQL databases** → **Create**.
2. Create or select a logical SQL server.
3. Configure server name, region, and an administrator identity.
4. Create a development database, e.g. `ubereatsdb`.
5. Choose a low-cost compute tier appropriate for your subscription and workload.
6. On networking, decide whether to allow selected public access for a learning setup or use private networking. Avoid broad public access in production.
7. If using your own client machine, add its public IP to the SQL server firewall temporarily.
8. Create the database.

Open Query editor or connect through SSMS/Azure Data Studio. Create/populate the source tables using the repository's SQL scripts and `Uber_Eats_SQL_Insert_Statements.docx`. Do not invent table columns: use the supplied source-to-target mapping and SQL scripts.

## 6. Security baseline

Prefer managed identities over embedded credentials:
- Enable the ADF managed identity.
- Grant it only the required Storage Blob Data Contributor/Reader roles at the narrowest suitable scope.
- For SQL, configure the supported authentication method and grant the identity/user the minimum database permissions.
- For Databricks, use the documented Access Connector/managed identity pattern where available.
- Store API secrets in Azure Key Vault and reference them from ADF/Databricks.

Never commit secrets to Git. If a secret is accidentally committed, rotate it; deleting it from the latest file alone is not sufficient.

## 7. Cost controls

- Use the smallest practical SQL tier.
- Keep Databricks clusters/jobs stopped when idle.
- Avoid leaving SHIR VMs, public IPs, or test compute running unnecessarily.
- Review Cost Management by resource group daily while learning.
- Delete the resource group when the exercise is finished, after exporting anything you need.

## Completion checklist

- [ ] Resource group created.
- [ ] ADLS Gen2 storage account created with hierarchical namespace enabled.
- [ ] Filesystem/container and directories created.
- [ ] ADF created and Studio opens.
- [ ] Azure SQL database created and reachable.
- [ ] Source mapping and SQL scripts reviewed.
- [ ] Managed identity and RBAC plan documented.
