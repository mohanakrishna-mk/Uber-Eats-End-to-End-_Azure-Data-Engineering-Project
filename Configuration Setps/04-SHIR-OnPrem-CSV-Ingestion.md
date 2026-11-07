# 04 – Self-hosted Integration Runtime and On-Premises CSV Ingestion

## 1. Understand why SHIR is required

Azure Integration Runtime handles cloud-to-cloud data movement. Self-hosted Integration Runtime (SHIR) is used when ADF must reach a source inside a private/on-premises network or a machine that is not directly reachable by Azure. In this project, the CSV files are stored on an on-premises machine, so SHIR runs on a machine that can read those files and communicate outbound to Azure.

## 2. Prepare the machine

Use a supported Windows machine/VM that:
- Can access the CSV folder locally.
- Has stable outbound HTTPS connectivity to Azure.
- Is powered on during pipeline execution.
- Has enough CPU/RAM and disk for the workload.
- Is not routinely put to sleep.

Create a dedicated folder, e.g.:

```text
C:\UberEatsData\
  drivers.csv
  promotions.csv
  zones.csv
```

The repository includes these sample CSV files. Keep their original headers and encoding unless the mapping explicitly says otherwise.

## 3. Create SHIR in ADF

1. ADF Studio → **Manage** → **Integration runtimes**.
2. Select **New**.
3. Choose **Azure, Self-Hosted** (wording may vary).
4. Give it a name such as `IR_UBEREATS_SHIR`.
5. Create it.
6. Select the new runtime and choose **Download and install**.
7. Install on the Windows machine that has the CSV files.
8. In ADF, copy the authentication key.
9. Register the SHIR node using the key.
10. Wait until the runtime status becomes **Running/Online**.

The repository includes `ADF_SHIR_CSV_PowerShell_Setup.docx`; follow it for the project's specific PowerShell and SHIR setup instructions.

## 4. Create a file-system linked service

1. ADF Studio → **Manage** → **Linked services** → **New**.
2. Select **File System**.
3. Select the self-hosted Integration Runtime.
4. Set the folder path, e.g. `C:\UberEatsData`.
5. Configure authentication if required by the Windows share/local folder.
6. Test connection.
7. Save as e.g. `LS_ONPREM_UBEREATS_FILES`.

For a network share, use a UNC path such as `\\fileserver\share\ubereats`, not a mapped drive letter that may not exist in the SHIR Windows service context. Grant the SHIR service identity read access to the folder.

## 5. Create CSV dataset

1. Author → New dataset → File System.
2. Select DelimitedText.
3. Choose the file-system linked service.
4. Set the directory and file name, or parameterize them.
5. Configure delimiter, quote character, escape character, encoding and first-row header based on the actual files.
6. Import schema.
7. Save.

Create separate metadata rows or parameter values for `drivers.csv`, `promotions.csv`, and `zones.csv`, as appropriate to the repository's metadata.

## 6. Build the copy pipeline

1. Add a Copy activity.
2. Source: File System CSV dataset.
3. Sink: ADLS Gen2 dataset.
4. Map columns if required.
5. Use a source-relative path or dataset parameters for the file.
6. Write each file to its intended landing path.
7. Debug while the SHIR machine is online.
8. Verify row count, headers and file contents in ADLS.

## 7. PowerShell validation

Run these on the SHIR host:

```powershell
Test-Path 'C:\UberEatsData\drivers.csv'
Test-Path 'C:\UberEatsData\promotions.csv'
Test-Path 'C:\UberEatsData\zones.csv'
Get-ChildItem 'C:\UberEatsData' -File
```

Check the SHIR service status:

```powershell
Get-Service | Where-Object {$_.DisplayName -like '*Integration Runtime*'}
```

If the linked service fails, verify that the Windows service account can read the path. A successful interactive user test does not prove that the SHIR service account can access it.

## 8. Troubleshooting

| Symptom | Check |
|---|---|
| SHIR offline | Host power/network, service status, registration key |
| Linked service test fails | Folder path, service account permissions, UNC path |
| File not found | File name/case, folder path, host machine |
| CSV malformed | Delimiter, encoding, quotes, embedded newlines |
| Pipeline hangs | Host resources, file size, network throughput |
| Works manually, fails in ADF | SHIR service identity vs logged-in user permissions |

## Completion checklist

- [ ] SHIR registered and online.
- [ ] SHIR host can access all CSV files.
- [ ] File System linked service test succeeds.
- [ ] CSV dataset schema is correct.
- [ ] ADF copies each CSV to the correct ADLS landing path.
- [ ] Counts and sample records validated.
