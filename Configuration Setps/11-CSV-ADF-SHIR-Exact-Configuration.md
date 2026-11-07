# 11 – CSV Metadata Pipeline and SHIR File Share Configuration (Chapters 11–12)

The video does not copy three CSVs using three hardcoded Copy activities. It reads a JSON metadata file, loops through the records and parameterizes the file dataset.

## 1. Confirm the CSV files and metadata

The repository contains:
- `drivers.csv`
- `promotions.csv`
- `zones.csv`
- `csv_metadata_proper (2).json`

The repository metadata is:

```json
[
  {
    "file_name": "drivers.csv",
    "target_folder": "csv/drivers"
  },
  {
    "file_name": "promotions.csv",
    "target_folder": "csv/promotions"
  },
  {
    "file_name": "zones.csv",
    "target_folder": "csv/zones"
  }
]
```

Keep this file's property names (`file_name`, `target_folder`) unchanged if using the expressions below.

## 2. Put the files in a shared folder on the Windows host

The video places the CSV files in one shared folder rather than configuring each file path independently.

1. Create a folder on the Windows machine, e.g. `C:\UberEatsShare`.
2. Copy the three CSV files into it.
3. Right-click the folder → Properties → Sharing → Advanced Sharing.
4. Enable sharing.
5. Give the share a simple name, e.g. `ubereats`.
6. Grant read access to the Windows identity that SHIR will use.
7. Note the Windows computer/host name.

The resulting network path is similar to:

```text
\\<WINDOWS_HOST>\ubereats
```

Use your own host and share name. Do not use the instructor's computer name or password.

## 3. Install and register SHIR

1. ADF Studio → Manage → Integration runtimes → New.
2. Select **Azure, Self-Hosted**.
3. Give it a name such as `IR_UberEats_SHIR`.
4. Create it and download the installer.
5. Install SHIR on the same Windows machine that can access the shared CSV folder.
6. Copy the authentication key from ADF.
7. Register the SHIR node with that key.
8. Wait until the node shows Online/Running.

The transcript uses SHIR because the source is on-premises/private network. For the Azure SQL cloud source, the instructor uses AutoResolve IR instead.

## 4. Create the File System linked service

1. ADF Studio → Manage → Linked services → New.
2. Select **File System**.
3. Select the self-hosted Integration Runtime you registered.
4. Host/server: enter the Windows machine name.
5. Share/folder: enter the shared folder name/path.
6. Authentication: use the Windows identity and password that can read the share.
7. Test connection.
8. Save.

The transcript specifically validates the linked service because SHIR must be able to reach the shared folder. ADF's connection test should succeed before the pipeline is built.

Security note: use a dedicated least-privilege service account in a real environment. Do not put a personal administrator password in Git or pipeline JSON. If the UI requires credentials, use the supported secure credential/Key Vault pattern for your deployment.

## 5. Create the CSV metadata dataset

1. In ADF, create a dataset for the JSON metadata file in ADLS.
2. Use the ADLS linked service and point to the metadata JSON file.
3. Name it clearly (the transcript calls it a JSON CSV dataset).
4. Create a Lookup activity that reads this dataset.
5. Set **First row only** off so Lookup returns all three JSON objects.
6. Debug and verify `output.value` has three entries.

## 6. Create a parameterized File System CSV dataset

1. Create a File System → DelimitedText dataset.
2. Select the File System linked service created above.
3. Set the shared folder as the directory.
4. Add a dataset parameter named `fileName`.
5. Bind the dataset's file name property to:

```text
@dataset().fileName
```

6. Configure CSV header/delimiter/encoding according to the actual CSV files.
7. Save and test preview.

## 7. Build the metadata-driven CSV pipeline

Pipeline flow:

```text
Lookup CSV JSON metadata
        |
        v
ForEach (Lookup output.value)
        |
        v
Copy activity
  Source: File System CSV dataset
  Sink: ADLS Gen2 dataset
```

ForEach Items:

```text
@activity('Lookup_CSV_Metadata').output.value
```

Inside the ForEach Copy activity:

- Source dataset parameter `fileName`:

```text
@item().file_name
```

- Sink directory/folder parameter:

```text
@item().target_folder
```

Use the sink dataset's exact parameter names and make sure the destination is under the Bronze container. The metadata values already include `csv/...` relative folders.

## 8. Debug and verify

1. Validate the pipeline.
2. Debug it while SHIR is online and the Windows host is available.
3. Confirm Lookup count = 3.
4. Confirm ForEach has three iterations.
5. Confirm each Copy activity succeeds.
6. Open ADLS → `bronze/csv`.
7. Verify the `drivers`, `promotions`, and `zones` target folders contain the corresponding files.

## 9. Troubleshooting

- SHIR offline: check service status and host connectivity.
- File System linked service fails: check host name, share name, UNC path and Windows share/NTFS permissions.
- Works in File Explorer but not ADF: SHIR Windows service identity may not have access.
- Lookup returns one object: disable First row only.
- File not found: compare `file_name` in JSON with the actual file name.
- Wrong sink directory: inspect `target_folder` and dataset parameter binding.
