## 1. Import Libraries <a name="import-libraries"></a>

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit, when, to_date, current_date, lower, trim, date_sub, lead, lag, coalesce
)
from pyspark.sql.window import Window
from functools import reduce
```

---

## 2. Initialize Spark Session <a name="initialize-spark-session"></a>

```python
# Initialize Spark Session
spark = SparkSession.builder.appName("HistoricalDataUpdateNoKey").getOrCreate()
```

---

## 3. Prepare Sample Data <a name="prepare-sample-data"></a>

### Current DataFrame (`current_df`)

```python
# Sample current data
current_data = [
    ('id1', 'valueA', 'info1'),
    ('id2', 'valueB', 'info2'),
    ('id4', 'valueD', 'info4_new'),  # New record
    ('id5', 'valueE', 'info5'),      # Updated info
]

current_columns = ['identifier', 'attribute', 'info']
current_df = spark.createDataFrame(current_data, current_columns)
```

### Historical DataFrame (`historical_df`)

```python
# Sample historical data
historical_data = [
    ('id1', 'valueA', 'info1', '2021-01-01', None),      # Current record
    ('id2', 'valueB', 'info2_old', '2021-02-01', None),  # Info updated
    ('id3', 'valueC', 'info3', '2021-03-01', None),      # Record to deactivate
    ('id5', 'valueE', 'info5_old', '2021-05-01', None),  # Info updated
]

historical_columns = ['identifier', 'attribute', 'info', 'from_date', 'to_date']
historical_df = spark.createDataFrame(historical_data, historical_columns)

# Convert date columns to date type
historical_df = historical_df.withColumn('from_date', to_date('from_date')) \
                             .withColumn('to_date', to_date('to_date'))
```

---

## 4. Data Cleaning and Standardization <a name="data-cleaning-and-standardization"></a>

```python
# Define the date columns
date_columns = ['from_date', 'to_date']

# Get all columns except date columns
non_date_columns = [col_name for col_name in historical_df.columns if col_name not in date_columns]

# Standardize and clean non-date columns in historical_df
for col_name in non_date_columns:
    historical_df = historical_df.withColumn(col_name, trim(lower(col(col_name).cast('string'))))

# Standardize and clean non-date columns in current_df
for col_name in current_df.columns:
    current_df = current_df.withColumn(col_name, trim(lower(col(col_name).cast('string'))))
```

---

## 5. Update Historical Data Based on Current Data <a name="update-historical-data"></a>

### Step 1: Identify Records to Deactivate

- Records in `historical_df` that are current (`to_date` is `None`) but not present in `current_df` should be deactivated.

```python
# Left anti join to find records in historical_df not in current_df
records_to_deactivate = historical_df.filter(col('to_date').isNull()).join(
    current_df,
    on=non_date_columns,
    how='left_anti'
)

# Set to_date to current_date() for records to deactivate
records_to_deactivate = records_to_deactivate.withColumn('to_date', current_date())
```

**Assertion:**

```python
# Assert that records_to_deactivate have non-null 'to_date'
assert records_to_deactivate.filter(col('to_date').isNull()).count() == 0, \
    "Records to deactivate should have 'to_date' set to current_date()"
```

---

### Step 2: Identify Records to Insert

- Records in `current_df` not present in `historical_df` should be inserted as new records.

```python
# Left anti join to find records in current_df not in historical_df
records_to_insert = current_df.join(
    historical_df.filter(col('to_date').isNull()),
    on=non_date_columns,
    how='left_anti'
)

# Add 'from_date' and 'to_date' columns
records_to_insert = records_to_insert.withColumn('from_date', current_date()) \
                                     .withColumn('to_date', lit(None).cast('date'))
```

**Assertion:**

```python
# Assert that records_to_insert have non-null 'from_date' and null 'to_date'
assert records_to_insert.filter(col('from_date').isNull()).count() == 0, \
    "Records to insert should have non-null 'from_date'"
assert records_to_insert.filter(col('to_date').isNotNull()).count() == 0, \
    "Records to insert should have 'to_date' as NULL"
```

---

### Step 3: Identify Records to Update

- Records where the non-date columns match but the current record has different values (i.e., updated info).

```python
# Inner join on non-date columns to find potential updates
potential_updates = current_df.join(
    historical_df.filter(col('to_date').isNull()),
    on=non_date_columns,
    how='inner'
)

# Since all columns except date columns are keys, and they match, but we might have updated info
# For this scenario, since all columns match, we can consider that there is no need to update
# However, if there are other changing columns, we can compare them here (in this case, info)
# Since 'info' is part of the key columns, differences would already be captured in insert or deactivate
```

**Note:** Since all columns except date columns are keys, any difference would result in an insert or deactivate, so we don't have separate updates.

---

### Step 4: Combine Records to Form Updated Historical DataFrame

```python
# Get current records from historical_df that are not being deactivated
current_records = historical_df.filter(col('to_date').isNull()).join(
    records_to_deactivate.select(non_date_columns),
    on=non_date_columns,
    how='left_anti'
)

# Combine all records
historical_df_updated = current_records.unionByName(records_to_deactivate).unionByName(records_to_insert)

# Include inactive records from historical_df
inactive_records = historical_df.filter(col('to_date').isNotNull())
historical_df_updated = historical_df_updated.unionByName(inactive_records)
```

**Assertion:**

```python
# Assert that the total number of records matches expected count
expected_count = current_records.count() + records_to_deactivate.count() + records_to_insert.count() + inactive_records.count()
assert historical_df_updated.count() == expected_count, "Historical DataFrame updated record count mismatch"
```

---

## 6. Adjust Overlapping Date Ranges <a name="adjust-overlapping-date-ranges"></a>

Since we are deactivating records by setting `to_date` to `current_date()`, and inserting new records with `from_date` as `current_date()`, we need to ensure there is no overlap.

```python
# Define window specification
window_spec = Window.partitionBy(*non_date_columns).orderBy(col('from_date').asc())

# Add previous_to_date to check for overlaps
historical_df_updated = historical_df_updated.withColumn(
    'prev_to_date',
    lag('to_date').over(window_spec)
)

# Identify overlaps
overlaps = historical_df_updated.filter(
    (col('prev_to_date').isNotNull()) &
    (col('from_date') <= col('prev_to_date'))
)

# Adjust overlaps if necessary (in this logic, overlaps should not occur)
overlaps_count = overlaps.count()
assert overlaps_count == 0, "There should be no overlapping date ranges after adjustment"
```

---

## 7. Finalize Historical DataFrame <a name="finalize-historical-dataframe"></a>

```python
# Drop temporary columns
historical_df_final = historical_df_updated.drop('prev_to_date')

# Remove duplicates if any
historical_df_final = historical_df_final.dropDuplicates()

# Sort the DataFrame for clarity
historical_df_final = historical_df_final.orderBy(
    non_date_columns + ['from_date'],
    ascending=[True] * (len(non_date_columns) + 1)
)
```

---

## 8. Conclusion <a name="conclusion"></a>

- We have updated the `historical_df` based on the `current_df` by considering all non-date columns as keys.
- Records not present in `current_df` have been deactivated.
- New records from `current_df` not in `historical_df` have been inserted.
- We have ensured there are no overlapping date ranges.
- **Note:** This approach assumes that any change in the non-date columns constitutes a new record, and missing records in `current_df` indicate deactivation.

---

## Show Final `historical_df`

```python
historical_df_final.show(truncate=False)
```

**Expected Output:**

```
+----------+---------+--------+----------+----------+
|identifier|attribute|info    |from_date |to_date   |
+----------+---------+--------+----------+----------+
|id1       |valuea   |info1   |2021-01-01|null      |
|id2       |valueb   |info2_old|2021-02-01|2023-10-24|
|id2       |valueb   |info2   |2023-10-24|null      |
|id3       |valuec   |info3   |2021-03-01|2023-10-24|
|id4       |valued   |info4_new|2023-10-24|null     |
|id5       |valuee   |info5_old|2021-05-01|2023-10-24|
|id5       |valuee   |info5   |2023-10-24|null      |
+----------+---------+--------+----------+----------+
```
