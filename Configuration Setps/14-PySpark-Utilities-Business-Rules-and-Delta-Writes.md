# 14 – PySpark Transformations, Utility Functions, Validation and Delta Write Modes (Chapters 14–18)

## 1. Use source-specific notebooks

The transcript explains why one generic transformation notebook is not always suitable: SQL tables, CSV files and API endpoints have different schemas and business rules.

For the SQL source, the instructor creates individual notebooks for the SQL entities. The spoken workflow identifies these SQL tables in the dispatcher/job run:
- customers
- restaurants
- menu_items
- orders
- order_items

The CSV and API transformations are also source-specific. The video later refers to 12 Silver managed tables across SQL, CSV and API sources.

## 2. Build a common notebook setup cell

The transcript demonstrates a common initial cell across notebooks:
1. Import required PySpark functions and project utility functions.
2. Read the source data from the appropriate Bronze/landing path.
3. Print/inspect the schema.
4. Apply transformations to the DataFrame.
5. Write to a fully qualified Unity Catalog table name.

Use three-part table names:

```text
<catalog>.<schema>.<table>
```

Example shape:

```python
target_table = "<project_catalog>.silver.silver_customers"
```

Use the catalog/schema names configured in your workspace.

## 3. Create reusable utility module

The video creates a utility module to avoid repeating common code. The transcript describes common functionality for:
- Column-name standardization.
- Data type standardization.
- Reusable data reading/handling.
- Validation helpers.

Create the module in the project utility folder, then import it into each entity notebook. Keep source-specific business rules in the entity notebook; keep reusable logic in the shared module.

Example utility skeleton (adapt names to the repository module):

```python
from pyspark.sql import functions as F

def standardize_column_names(df):
    # Apply the naming convention used in the project.
    return df

def cast_column(df, column_name, data_type):
    return df.withColumn(column_name, F.col(column_name).cast(data_type))

def remove_duplicates(df, key_columns):
    return df.dropDuplicates(key_columns)
```

The code above is a template, not a verbatim extraction of the instructor's module.

## 4. Business rules explicitly discussed in the transcript

The instructor explores the incoming data before deciding rules. The transcript explicitly mentions:
- Remove duplicate order records using `order_id` as a business key.
- Standardize `payment_status` to uppercase.
- Keep only orders whose `order_status` is `delivered` for the described downstream business use case.
- Standardize `order_id` to an integer type.
- Apply different business rules for different entities.
- Date/time values in the sample are already standardized, but the instructor notes that production pipelines often normalize timestamps/date columns.

Example implementation pattern:

```python
from pyspark.sql import functions as F

orders_df = orders_df.dropDuplicates(["order_id"])

orders_df = orders_df.withColumn(
    "payment_status",
    F.upper(F.col("payment_status"))
)

orders_df = orders_df.filter(
    F.lower(F.col("order_status")) == "delivered"
)

orders_df = orders_df.withColumn(
    "order_id",
    F.col("order_id").cast("int")
)
```

Only apply the delivered-only filter if it matches the business requirement for the target table. It is a business rule from the video, not a universal rule for every order dataset.

## 5. Data validation

The video demonstrates validating the business key and duplicate counts after transformations. Add checks such as:
- Business key null count.
- Duplicate business key count.
- Row count before and after filtering/deduplication.
- Required column presence.
- Data type checks.
- Status domain checks.

Example validation pattern:

```python
from pyspark.sql import functions as F

null_key_count = df.filter(F.col("order_id").isNull()).count()

duplicate_key_count = (
    df.groupBy("order_id")
      .count()
      .filter(F.col("count") > 1)
      .count()
)

print("Null business keys:", null_key_count)
print("Duplicate business keys:", duplicate_key_count)
```

The transcript shows a business-key null validation returning zero. Do not treat a green pipeline status as proof of data correctness; inspect the checks and compare source/target data.

## 6. Create managed Silver Delta tables

The video writes enriched data as managed tables under the Silver schema.

1. Confirm the catalog and Silver schema exist.
2. Build the transformed DataFrame.
3. Set the target table name using the three-part namespace.
4. Write Delta data to the table.
5. Refresh Catalog Explorer and confirm the managed table appears.

Example:

```python
(
    transformed_df.write
      .format("delta")
      .mode("overwrite")
      .saveAsTable("<catalog>.silver.silver_customers")
)
```

The transcript describes Delta data as Parquet files with Delta transaction-log metadata.

## 7. Overwrite vs Append – as demonstrated

The video explains this initial-load/incremental-load distinction:

| Situation | Write mode demonstrated |
|---|---|
| First/full initialization of a target table | `overwrite` |
| Subsequent incremental batch | `append` |
| Mutable/upsert-style records requiring updates | Consider a merge strategy; the transcript mentions merge as an alternative |

Example:

```python
if not spark.catalog.tableExists(target_table):
    (df.write.format("delta")
       .mode("overwrite")
       .saveAsTable(target_table))
else:
    (df.write.format("delta")
       .mode("append")
       .saveAsTable(target_table))
```

This is a simplified illustration of the video’s pattern. Blind append can create duplicates on reruns; use a deduplication/merge design when the same business keys can arrive again.

## 8. Clone notebooks carefully

The instructor clones a notebook to create the next entity notebook, reusing the common imports/read/write structure. For each clone, change:
- Source folder/table.
- DataFrame variable.
- Target table name.
- Business key.
- Business-specific filters/standardization.
- Validation rules.

The transcript explicitly warns that notebook naming and syntax errors can cause the dispatcher workflow to fail.

## 9. Separate workflows by source

The transcript uses three separate downstream jobs/workflows because SQL, CSV and API have different schemas and business logic:
- SQL workflow: one child notebook per SQL table.
- CSV workflow: drivers, promotions and zones notebooks.
- API workflow: payments, deliveries, ratings and order-status notebooks.

Do not force unrelated source schemas into one generic transformation if the rules differ.
