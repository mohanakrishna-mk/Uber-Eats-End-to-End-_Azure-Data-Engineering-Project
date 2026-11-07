# 02 – ADLS Gen2, ADF Linked Services and Datasets

## 1. Configure storage access for ADF

1. Open the Data Factory → **Launch Studio**.
2. Open **Manage** → **Managed identities** (or the factory's identity settings in the Azure portal).
3. Ensure the system-assigned managed identity is enabled.
4. Open the storage account → **Access Control (IAM)** → **Add role assignment**.
5. Assign the minimum required data-plane role, commonly **Storage Blob Data Contributor**, to the Data Factory managed identity at the required container or account scope.
6. Return to ADF Studio.

If using hierarchical namespace ACLs, also check directory execute/read/write ACLs. RBAC role assignment alone may not be sufficient when ACLs are used to restrict paths.

## 2. Create the ADLS linked service

1. In ADF Studio, choose **Manage** → **Linked services** → **New**.
2. Search for **Azure Data Lake Storage Gen2**.
3. Select the storage account and choose managed identity authentication if supported by the chosen connector.
4. Select the subscription and storage account.
5. Test connection.
6. Name it clearly, e.g. `LS_ADLS_UBEREATS`.
7. Save.

If the test fails:
- Confirm the storage account endpoint and selected account.
- Confirm the ADF identity has data-plane RBAC.
- Check hierarchical namespace ACLs.
- Check firewall/network restrictions and private endpoint/DNS configuration.

## 3. Create datasets

A linked service describes the connection. A dataset describes the data location and format.

For a CSV dataset:
1. **Author** → **+** → **Dataset**.
2. Choose Azure Data Lake Storage Gen2.
3. Choose DelimitedText.
4. Select `LS_ADLS_UBEREATS`.
5. Set filesystem/container and directory/file path.
6. Configure first row as header if the file has headers.
7. Set delimiter and encoding based on the actual source.
8. Give it a name such as `DS_ADLS_CSV_LANDING`.

For Parquet:
1. Create another dataset.
2. Choose Parquet.
3. Reuse the ADLS linked service.
4. Parameterize the directory/file if the pipeline writes multiple tables.

For SQL:
1. Create an Azure SQL Database linked service.
2. Use managed identity or Key Vault-backed credentials where supported.
3. Select server/database and test connection.
4. Create a dataset for the source table or use a parameterized dataset.

## 4. Parameterize reusable datasets

For metadata-driven ingestion, avoid creating one dataset per table if the tables share a schema/connection pattern.

Example dataset parameters:
- `schemaName`
- `tableName`

In the dataset connection/table property, reference the parameters using ADF dynamic content, e.g.:

```text
@dataset().schemaName
@dataset().tableName
```

For a file dataset, useful parameters are:
- `container`
- `directory`
- `fileName`

Example dynamic path:

```text
@concat(dataset().directory, '/', dataset().fileName)
```

Use the exact parameter names required by your pipeline JSON if importing repository artifacts.

## 5. Import or recreate repository artifacts

The repository has `linkedService/`, `dataset/`, `factory/`, `pipeline/`, `integrationRuntime/`, and `routing/` folders. Inspect the JSON files before importing:
- Identify the factory name and region references.
- Replace subscription IDs, resource IDs, storage account names, server names and Key Vault references.
- Check linked service authentication settings.
- Check dataset parameter names and file paths.
- Check Integration Runtime references.
- Do not publish unchanged resource IDs from somebody else's environment.

In ADF Studio, use the repository's Git integration or import/create the JSON artifacts using the supported ADF authoring workflow. Validate each linked service before running pipelines.

## 6. Publish and test

1. Click **Validate all**.
2. Fix missing linked services, invalid expressions, and unresolved parameters.
3. Click **Publish**.
4. Run a small test copy to a temporary landing directory.
5. Verify the output in the ADLS browser.

## Troubleshooting

| Symptom | What to inspect |
|---|---|
| Authentication failed | Managed identity enabled; role assignment; ACLs |
| 403 | Data-plane role and filesystem ACLs; firewall |
| Path not found | Container, directory and filename casing |
| Dataset parameter missing | Pipeline parameter mapping and dataset parameter names |
| CSV columns shifted | Delimiter, quote character, escape character, encoding, header setting |
| Publish fails | Broken references, invalid JSON, unresolved IR or linked service |

## Completion checklist

- [ ] ADF managed identity enabled.
- [ ] Storage role/ACL assigned.
- [ ] ADLS linked service test succeeds.
- [ ] SQL linked service test succeeds.
- [ ] CSV/Parquet datasets created.
- [ ] Reusable datasets parameterized.
- [ ] Repository resource IDs replaced with your own.
- [ ] ADF validation and publish succeed.
