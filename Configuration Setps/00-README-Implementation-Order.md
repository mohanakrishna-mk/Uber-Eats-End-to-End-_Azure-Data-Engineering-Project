# Uber Eats – Azure End-to-End Data Engineering: Configuration Runbook

This folder contains hands-on configuration guides for implementing the project represented in the video transcript and the public GitHub repository.

## Architecture

```text
Azure SQL Database ───────┐
On-premises CSV files ────┼── Azure Data Factory ── ADLS Gen2 (landing/bronze)
REST API ─────────────────┘                              │
                                                        ▼
                                                Azure Databricks
                                           Bronze → Silver → Gold
                                                        │
                                                        ▼
                                                    Power BI
```

ADF handles extraction/orchestration. ADLS Gen2 is the lake. Databricks performs transformations. Power BI consumes curated Gold data. Logic Apps can send pipeline notifications.

## Repository layout observed

The repository contains these top-level folders/files: `data/`, `dataset/`, `factory/`, `integrationRuntime/`, `linkedService/`, `notebooks/`, `pipeline/`, `routing/`, plus SQL/CSV/API metadata and supporting DOCX documents. It also includes `main.py`, `requirements.txt`, `publish_config.json`, CSV files such as `drivers.csv`, `promotions.csv`, `zones.csv`, and metadata JSON files. Use the repository artifacts as the source of truth for exact names and definitions.

Repository: https://github.com/srirama9908-lang/Uber-Eats-End-to-End-_Azure-Data-Engineering-Project

## Recommended execution order

1. Read `01-Prerequisites-and-Azure-Foundation.md`.
2. Create ADLS Gen2 and Azure SQL resources.
3. Configure ADF linked services and datasets.
4. Build SQL initial-load and metadata-driven pipelines.
5. Add SQL incremental processing.
6. Configure Self-hosted Integration Runtime (SHIR) and CSV ingestion.
7. Configure REST API ingestion.
8. Configure Databricks, Unity Catalog, storage access and notebooks.
9. Orchestrate ADF → Databricks jobs.
10. Build Gold outputs and connect Power BI.

## Naming convention used in these guides

Replace placeholders with your own values. Example names are illustrative, not claimed to be the exact names in the repository.

| Resource | Example |
|---|---|
| Resource group | `rg-ubereats-dev` |
| Region | `centralindia` |
| Data Factory | `adf-ubereats-dev` |
| Storage account | `stubereatsdev001` |
| SQL server | `sql-ubereats-dev` |
| SQL database | `ubereatsdb` |
| Databricks workspace | `dbw-ubereats-dev` |
| Key Vault | `kv-ubereats-dev` |

Storage account names must be globally unique, lowercase, and 3–24 characters. Do not put passwords, API keys, SAS tokens, or access tokens in notebooks, JSON committed to Git, or screenshots.

## Important implementation notes

- The transcript describes the architecture and workflow, but it does not expose every exact SQL DDL, API endpoint, schema, or final configuration value. Treat any examples in these guides as templates and compare them with the repository's metadata and supporting documents.
- The GitHub repository includes dedicated supporting files: `SQL_Migration_Step_by_Step.docx`, `Uber_Eats_SQL_Source_and_Incremental_Loading.docx`, `ADF_SHIR_CSV_PowerShell_Setup.docx`, `REST_API_Migration_Step_by_Step.docx`, `Source to target Mapping.docx`, and SQL insert statements. Refer to these for exact project-specific definitions.
- Azure portal labels can change. If a blade has moved, use the portal search bar to find the resource/service by name.
- For a free Azure subscription, check regional availability, quotas, Databricks pricing, SQL pricing, and storage transaction costs before provisioning. Stop or delete compute when not in use.
