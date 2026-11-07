# 07 – Gold Layer, Power BI, Operations and End-to-End Validation

## 1. Define the Gold layer

Gold is the business-consumption layer. Before building it, agree on:
- Business questions and KPIs.
- Grain of each fact table.
- Dimension definitions.
- Keys and relationships.
- Refresh frequency and latency.
- Historical vs current-state requirements.

The transcript introduces and implements a Gold layer after the source-specific processing and Databricks jobs. Use the repository's business rules and source-to-target mapping to determine the actual output tables.

## 2. Build Gold transformations

1. Read validated Silver tables.
2. Standardize keys and business dates.
3. Build dimensions and facts or curated aggregate tables.
4. Apply documented business rules.
5. Validate referential integrity and totals.
6. Write Delta tables to the Gold schema.
7. Record the run ID, source version/watermark, counts and status.

Illustrative example only:

```python
# Replace table names and metrics with the actual project model.
orders = spark.table("dev.silver.orders")
restaurants = spark.table("dev.silver.restaurants")

# Build a business-ready dataset using the real mapping and rules.
gold_df = orders.join(restaurants, "restaurant_id", "left")

(gold_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("dev.gold.order_reporting"))
```

Do not use broad overwrite on production tables without a deliberate replacement/partition strategy.

## 3. Connect Power BI

Choose the supported connection mode for your workspace and governance model:
- Databricks SQL Warehouse / connector for governed SQL access.
- A curated export if the learning environment does not provide the required direct connectivity.

Steps:
1. In Databricks, create or identify a SQL Warehouse.
2. Grant the Power BI identity/user access to the required Gold tables.
3. In Power BI Desktop, select **Get data** → Azure Databricks.
4. Enter the server hostname and HTTP path from the Databricks connection details.
5. Authenticate using the supported organizational method.
6. Select only the required Gold tables.
7. Build relationships and measures.
8. Validate report totals against SQL/Delta queries.
9. Publish to the appropriate Power BI workspace if licensed and permitted.

Do not expose raw landing data to report consumers unless explicitly required.

## 4. End-to-end run sequence

Run in this order for a first-time deployment:

1. Confirm SQL source tables are populated.
2. Run SQL initial-load pipeline.
3. Confirm SQL landing files and row counts.
4. Run SHIR CSV ingestion.
5. Confirm all CSV landing files.
6. Run REST API ingestion and verify pagination.
7. Confirm all raw API data landed.
8. Run Databricks SQL-source job.
9. Run Databricks CSV-source job.
10. Run Databricks API-source job.
11. Validate Bronze and Silver tables.
12. Run Gold transformation.
13. Validate Gold metrics and row counts.
14. Refresh Power BI and reconcile report totals.

For scheduled operation, use ADF triggers or an approved scheduler. Avoid overlapping runs if the pipeline is not designed for concurrency.

## 5. Monitoring and audit

Use ADF Monitor to inspect:
- Pipeline run status and duration.
- Activity-level errors.
- Integration Runtime used.
- Copy throughput and rows read/written where available.

Use Databricks job run details and Spark UI to inspect:
- Failed tasks/stages.
- Input/output row counts.
- Cluster startup time.
- Shuffle/skew and executor failures.

Maintain an audit table with:
- Pipeline/job run ID.
- Source and target.
- Load type.
- Start/end timestamps.
- Status.
- Rows read/written/rejected.
- Watermark before/after.
- Error message.

## 6. Logic Apps notifications

The video includes Azure Logic Apps for notifications. A typical pattern:
1. Create a Logic App.
2. Choose a trigger compatible with the chosen notification integration (for example, an HTTP request trigger or a supported event/connector trigger).
3. Add an action to send an email or Teams notification using an approved connector.
4. Secure the trigger endpoint.
5. Call the Logic App from ADF on success/failure using a Web activity or the integration pattern used in the project.
6. Include pipeline name, run ID, status and a link to monitoring.
7. Test both success and failure paths.

Do not place a Logic App callback secret in source control.

## 7. End-to-end validation matrix

| Test | Expected result |
|---|---|
| SQL initial load | Every configured source table lands |
| SQL incremental load | Only changes after the prior watermark are processed |
| SQL failure and retry | Watermark is not advanced incorrectly |
| CSV ingestion | All expected files copied through SHIR |
| SHIR offline | Pipeline fails clearly and can be rerun |
| REST pagination | All pages are landed exactly as intended |
| API 429/5xx | Retry/backoff behavior is controlled |
| Databricks transformation | Bronze/Silver outputs match mapping |
| Duplicate input/replay | No unintended duplicate business records |
| Gold refresh | Business totals reconcile to Silver |
| Power BI refresh | Report totals match Gold tables |
| Notification | Correct success/failure message is sent |

## 8. Production-readiness improvements

Before calling this production-grade, add:
- Private endpoints and controlled network paths where required.
- Key Vault and managed identity for secrets.
- Least-privilege RBAC and Unity Catalog grants.
- Schema-drift detection and quarantine.
- Idempotent writes and safe rerun strategy.
- Alerting on freshness, row-count anomalies and failures.
- CI/CD promotion across dev/test/prod.
- Data retention and lifecycle policies.
- Cost budgets, cluster policies and auto-termination.
- Backup/recovery and disaster recovery requirements.

## Completion checklist

- [ ] Gold tables built from validated Silver data.
- [ ] Business KPIs reconciled.
- [ ] Power BI can query Gold data.
- [ ] ADF and Databricks monitoring reviewed.
- [ ] Logic App success/failure notification tested.
- [ ] End-to-end rerun and failure scenarios tested.
- [ ] Security, cost and production gaps documented.
