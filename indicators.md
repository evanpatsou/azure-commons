## Import Libraries

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, when, lit, to_date, row_number, lag, lead, date_sub
)
from pyspark.sql.window import Window
from functools import reduce
```

## Initialize Spark Session

```python
# Initialize Spark session
spark = SparkSession.builder.appName("HistoricalDataUpdateDataFrameAPI").getOrCreate()
```

## Sample Data Creation

### Historical DataFrame

```python
# Sample data for the historical DataFrame
historical_data = [
    (1, 'A', 'X', 'Alpha', 'Red', '2024-01-01', None),
    (2, 'B', 'Y', 'Beta', 'Blue', '2024-01-01', None),
    (3, 'C', 'Z', 'Gamma', 'Green', '2024-01-01', None),
    (4, 'D', 'W', 'Delta', 'Yellow', '2023-12-31', '2024-01-01')
]

# Define columns for the historical DataFrame
historical_columns = ['id', 'col1', 'col2', 'col3', 'processed', 'from_date', 'to_date']

historical_df = spark.createDataFrame(historical_data, historical_columns)
```

### Current DataFrame

```python
# Sample data for the current DataFrame
current_data = [
    (1, 'A', 'X', 'Alpha', 'Crimson', '2024-01-04'),
    (2, 'B', 'Y', 'Beta', 'Blue', '2024-01-01'),
    (3, 'C', 'Z', 'Gamma', 'Emerald', '2024-01-03'),
    (5, 'E', 'V', 'Epsilon', 'Purple', '2024-01-01')
]

# Define columns for the current DataFrame
current_columns = ['id', 'col1', 'col2', 'col3', 'processed', 'processing_date']

current_df = spark.createDataFrame(current_data, current_columns)
```

## Data Preparation and Validation

### Convert Date Columns

```python
# Convert date columns in historical_df
historical_df = historical_df.withColumn('from_date', to_date('from_date')) \
                             .withColumn('to_date', to_date('to_date'))

# Convert processing_date in current_df
current_df = current_df.withColumn('processing_date', to_date('processing_date'))
```

### Ensure Consistent Data Types and Standardize Casing

```python
# List of all columns except dates
all_columns = [c for c in historical_df.columns if c not in {'from_date', 'to_date'}]

# Cast all columns to string and standardize casing
for col_name in all_columns:
    historical_df = historical_df.withColumn(col_name, col(col_name).cast('string').lower())
    current_df = current_df.withColumn(col_name, col(col_name).cast('string').lower())
```

### Remove Duplicates and Nulls

```python
# Remove duplicates in current_df
current_df = current_df.dropDuplicates()

# Filter out records with null processing_date
current_df = current_df.filter(col('processing_date').isNotNull())

# Validate key columns are not null
key_columns = ['id']
for col_name in key_columns:
    historical_df = historical_df.filter(col(col_name).isNotNull())
    current_df = current_df.filter(col(col_name).isNotNull())
```

## Identify Key and Changing Columns

```python
# Define columns to compare for changes (excluding key columns and date columns)
non_comparable_columns = set(key_columns + ['from_date', 'to_date', 'processing_date'])
changing_columns = [c for c in historical_df.columns if c not in non_comparable_columns]

print("Key Columns:", key_columns)
print("Changing Columns:", changing_columns)
```

**Output:**

```
Key Columns: ['id']
Changing Columns: ['col1', 'col2', 'col3', 'processed']
```

## Join Historical and Current Data

```python
# Join historical and current data on key columns
joined_df = historical_df.alias('hist').join(
    current_df.alias('curr'),
    on=key_columns,
    how='inner'
)
```

## Determine Records that Need to Be Updated

### Build the Change Condition with Null Handling

```python
# Build the change_condition with null-safe equality check
change_conditions = [
    col(f'hist.{c}').eqNullSafe(col(f'curr.{c}')) == False
    for c in changing_columns
]

# Combine all conditions using logical OR
change_condition = reduce(lambda x, y: x | y, change_conditions)
```

**Explanation:**

- We use `eqNullSafe` to handle `NULL` values correctly.
- The condition checks if any of the changing columns have changed.

### Identify Records to Update

```python
# Identify records where any column has changed
records_to_update = joined_df.filter(change_condition)

# Records where no columns have changed
records_no_change = joined_df.filter(~change_condition)
```

## Update `to_date` in Historical Records

```python
# Exclude 'to_date' from historical columns
hist_columns = [c for c in historical_df.columns if c != 'to_date']

# Build the updated DataFrame with new 'to_date'
updated_to_date_df = records_to_update.select(
    *[col('hist.' + c).alias(c) for c in hist_columns],
    col('curr.processing_date').alias('to_date')
)
```

## Identify Non-Updated Historical Records

### Records Not Updated Because No Columns Changed

```python
# These records remain as they are
non_updated_historical_df_same = records_no_change.select(
    *[col('hist.' + c).alias(c) for c in historical_df.columns]
)
```

### Records Not Updated Because No Matching Current Data

```python
# Get the rest of the historical records that didn't join with current data
non_updated_historical_df_rest = historical_df.alias('hist').join(
    joined_df.select(col('hist.id')).distinct(),
    on='id',
    how='left_anti'
)
```

### Combine Non-Updated Historical Records

```python
non_updated_historical_df = non_updated_historical_df_same.unionByName(non_updated_historical_df_rest)
```

## Combine Updated and Non-Updated Historical Records

```python
# Combine updated and non-updated historical records
historical_df_updated = non_updated_historical_df.unionByName(updated_to_date_df)
```

## Add New Records from Current Data

### Prepare Current Records

```python
# Prepare to identify new records needed
current_records_needed = current_df.select(
    *key_columns,
    'processing_date',
    *changing_columns
)
```

### Identify New Records Needed

```python
# Left anti join to find current records with no matching historical record
new_records_no_hist = current_records_needed.alias('curr').join(
    historical_df.alias('hist'),
    on=key_columns,
    how='left_anti'
)

# Records where any column has changed (from records_to_update)
new_records_changed = records_to_update.select(
    *[col('curr.' + c).alias(c) for c in key_columns],
    col('curr.processing_date').alias('from_date'),
    *[col('curr.' + c).alias(c) for c in changing_columns]
)
```

### Combine New Records

```python
# New records from current data
new_records_df = new_records_no_hist.select(
    *key_columns,
    col('processing_date').alias('from_date'),
    *changing_columns
).unionByName(new_records_changed)

# Add 'to_date' as None for new records
new_records_df = new_records_df.withColumn('to_date', lit(None).cast('date'))
```

## Combine with Historical DataFrame

```python
# Combine the historical data with new records
historical_df_final = historical_df_updated.unionByName(new_records_df.select(historical_df_updated.columns))
```

## Adjust Overlapping Date Ranges

### Define Window Specification

```python
# Define window specification based on all columns except dates
partition_columns = key_columns + changing_columns

window_spec = Window.partitionBy(*partition_columns).orderBy(col('from_date').asc())
```

### Apply Window Functions

```python
# Add row number and next_from_date
historical_df_final = historical_df_final.withColumn('rn', row_number().over(window_spec))

historical_df_final = historical_df_final.withColumn(
    'next_from_date',
    lead(col('from_date')).over(window_spec)
)
```

### Adjust `to_date` to Prevent Overlaps

```python
# Adjust to_date to be one day before next_from_date
historical_df_final = historical_df_final.withColumn(
    'adjusted_to_date',
    when(
        col('to_date').isNull() & col('next_from_date').isNotNull(),
        date_sub(col('next_from_date'), 1)
    ).otherwise(col('to_date'))
)
```

## Finalize DataFrame

### Drop Temporary Columns and Rename Adjusted Columns

```python
# Drop temporary columns and rename adjusted_to_date
historical_df_final = historical_df_final.drop('to_date', 'rn', 'next_from_date')
historical_df_final = historical_df_final.withColumnRenamed('adjusted_to_date', 'to_date')
```

### Remove Duplicates

```python
# Remove duplicates if any
historical_df_final = historical_df_final.dropDuplicates(
    key_columns + changing_columns + ['from_date']
)
```

### Sort the DataFrame for Clarity

```python
# Sort the DataFrame for clarity
historical_df_final = historical_df_final.orderBy(
    key_columns + changing_columns + ['from_date'],
    ascending=[True] * (len(key_columns) + len(changing_columns) + 1)
)
```

## Show Final Historical DataFrame

```python
# Show the final historical DataFrame
historical_df_final.show(truncate=False)
```

**Expected Output:**

```
+---+----+----+-----+----------+----------+----------+
|id |col1|col2|col3 |processed |from_date |to_date   |
+---+----+----+-----+----------+----------+----------+
|1  |a   |x   |alpha|crimson   |2024-01-04|null      |
|1  |a   |x   |alpha|red       |2024-01-01|2024-01-03|
|2  |b   |y   |beta |blue      |2024-01-01|null      |
|3  |c   |z   |gamma|emerald   |2024-01-03|null      |
|3  |c   |z   |gamma|green     |2024-01-01|2024-01-02|
|4  |d   |w   |delta|yellow    |2023-12-31|2024-01-01|
|5  |e   |v   |epsilon|purple  |2024-01-01|null      |
+---+----+----+-----+----------+----------+----------+
```

# Add a unique identifier column (if not already present)
# For this example, we'll assume 'id' is the primary key

# Define a function to generate the upsert SQL statement dynamically
def generate_upsert_statement(table, columns, primary_keys):
    update_columns = [col for col in columns if col not in primary_keys]
    set_clause = ", ".join([f"{col}=EXCLUDED.{col}" for col in update_columns])
    conflict_clause = ", ".join(primary_keys)
    sql_statement = f"""
    CREATE TEMP TABLE tmp_table AS SELECT * FROM {table} WHERE 1=0;
    COPY tmp_table ({', '.join(columns)}) FROM STDIN WITH (FORMAT CSV);
    INSERT INTO {table} ({', '.join(columns)})
    SELECT * FROM tmp_table
    ON CONFLICT ({conflict_clause}) DO UPDATE SET {set_clause};
    """
    return sql_statement

# Get the list of columns dynamically
columns = historical_df_final.columns

# Define the primary key columns
primary_keys = ['id']  # Adjust based on your actual primary key columns

# Generate the upsert SQL statement
upsert_sql = generate_upsert_statement(db_table, columns, primary_keys)

# Use the upsert SQL statement in the DataFrame write operation
historical_df_final.write \
    .format("jdbc") \
    .option("url", jdbc_url) \
    .option("dbtable", db_table) \
    .option("user", db_user) \
    .option("password", db_password) \
    .option("driver", "org.postgresql.Driver") \
    .option("numPartitions", 10) \
    .option("batchsize", 10000) \
    .mode("append") \
    .option("sql", upsert_sql) \
    .save()
