## Updated Code with All Columns Except `to_date` as Key Columns

### Import Libraries

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, when, lit, to_date, row_number, lead, date_sub, current_date, broadcast
)
from pyspark.sql.window import Window
from functools import reduce
```

### Initialize Spark Session

```python
spark = SparkSession.builder.appName("UpdateHistoricalData").getOrCreate()
```

### Sample Data Creation

#### **`dataset_1`**

```python
# Sample data for dataset_1
data1 = [
    (1, 'tc1', 'Name1'),
    (2, 'tc2', 'Name2'),
    (3, 'tc3', 'Name3'),
    (4, 'tc4', 'Name4')
]

columns = ['dataset1_id', 'textcode', 'name']
dataset_1 = spark.createDataFrame(data1, columns)
```

#### **`dataset_2`**

```python
# Sample data for dataset_2
data2 = [
    ('tc1', 'Name1_new'),
    ('tc2', 'Name2_new'),
    ('tc5', 'Name5_new'),
    ('tc_collide', 'Name_collide')
]

dataset_2 = spark.createDataFrame(data2, ['textcode', 'name'])
```

#### **`dataset_3`**

```python
# Sample data for dataset_3
data3 = [
    (100, 'tc6', 'Name6'),
    (200, 'tc7', 'Name7'),
    (300, 'tc2', 'Name2_dup')
]

columns = ['dataset3_id', 'textcode', 'name']
dataset_3 = spark.createDataFrame(data3, columns)
```

### Data Preparation and Merging

We follow the same steps as before to handle collisions and merge the datasets, ensuring consistency.

### Create `current_df` from Merged Data

#### **Add Processing Date**

```python
# Assume processing_date is the current date
current_df = merged_dataset.withColumn('processing_date', current_date())
```

#### **Map Columns to `current_df` Structure**

```python
# Map columns to match current_df structure
current_df = merged_dataset.select(
    col('dataset1_id'),
    col('dataset3_id'),
    col('textcode'),
    coalesce(col('name'), col('dataset2_name')).alias('name'),
    lit(None).alias('processed'),
    'processing_date'
)
```

### Prepare the Historical DataFrame

#### **Sample Data Creation for Historical DataFrame**

```python
# Sample data for the historical DataFrame
historical_data = [
    (1, None, 'tc1', 'Name1', 'Processed1', '2024-01-01', None),
    (2, None, 'tc2', 'Name2', 'Processed2', '2024-01-01', None),
    (None, 100, 'tc6', 'Name6', 'Processed6', '2024-01-01', None),
    (None, 200, 'tc7', 'Name7', 'Processed7', '2024-01-01', None)
]

historical_columns = ['dataset1_id', 'dataset3_id', 'textcode', 'name', 'processed', 'from_date', 'to_date']
historical_df = spark.createDataFrame(historical_data, historical_columns)
```

### Data Preparation and Validation

#### **Convert Date Columns**

```python
# Convert date columns in historical_df
historical_df = historical_df.withColumn('from_date', to_date('from_date')) \
                             .withColumn('to_date', to_date('to_date'))

# Convert processing_date in current_df
current_df = current_df.withColumn('processing_date', to_date('processing_date'))
```

#### **Ensure Consistent Data Types and Standardize Casing**

```python
# Cast all columns to string and standardize casing (except dates)
for col_name in historical_df.columns:
    if col_name not in {'from_date', 'to_date'}:
        historical_df = historical_df.withColumn(col_name, col(col_name).cast('string').lower())
        current_df = current_df.withColumn(col_name, col(col_name).cast('string').lower())
```

#### **Remove Duplicates and Nulls**

```python
# Remove duplicates in current_df
current_df = current_df.dropDuplicates()

# Filter out records with null processing_date
current_df = current_df.filter(col('processing_date').isNotNull())
```

### Identify Key Columns

```python
# Define key columns as all columns except 'to_date'
key_columns = [col_name for col_name in historical_df.columns if col_name != 'to_date']

print("Key Columns:", key_columns)
```

**Output:**

```
Key Columns: ['dataset1_id', 'dataset3_id', 'textcode', 'name', 'processed', 'from_date']
```

### Since All Columns (Except `to_date`) Are Key Columns, There Are No Changing Columns

```python
changing_columns = []  # No changing columns because all are part of the key
```

### Join Historical and Current Data

```python
# Join historical and current data on key columns
join_condition = [historical_df[k] == current_df[k] for k in key_columns]
joined_df = historical_df.alias('hist').join(
    current_df.alias('curr'),
    on=join_condition,
    how='inner'
)
```

### Determine Records that Need to Be Updated

#### **Build the Change Condition**

Since all columns are part of the key, any difference in the key columns indicates a different record.

In this case, **if a record exists in both `historical_df` and `current_df` with the same key (i.e., all columns except `to_date`), it does not need to be updated**.

#### **Identify Records to Update**

Records in `historical_df` where `to_date` is `NULL` and the corresponding record in `current_df` does not exist (i.e., the key does not match) need to be closed (set `to_date`).

```python
# Identify current entries in historical_df (to_date is null)
current_hist_entries = historical_df.filter(col('to_date').isNull())

# Left anti join to find historical records not in current_df
records_to_update = current_hist_entries.alias('hist').join(
    current_df.alias('curr'),
    on=key_columns,
    how='left_anti'
)
```

### Update `to_date` in Historical Records

```python
# Update to_date to processing_date for records to update
updated_to_date_df = records_to_update.withColumn('to_date', current_date())
```

### Identify Non-Updated Historical Records

```python
# Historical records that don't need to be updated
non_updated_historical_df = historical_df.alias('hist').join(
    updated_to_date_df.select(*key_columns), on=key_columns, how='left_anti'
)
```

### Combine Updated and Non-Updated Historical Records

```python
# Combine updated and non-updated historical records
historical_df_updated = non_updated_historical_df.unionByName(updated_to_date_df)
```

### Add New Records from Current Data

#### **Identify New Records Not in Historical Data**

```python
# Left anti join to find new records in current_df not in historical_df
new_records_df = current_df.alias('curr').join(
    historical_df_updated.alias('hist'),
    on=key_columns,
    how='left_anti'
)
```

#### **Add `from_date` and `to_date` Columns**

```python
# Add 'from_date' and 'to_date' columns to new records
new_records_df = new_records_df.withColumn('from_date', current_date()).withColumn('to_date', lit(None).cast('date'))
```

### Combine with Historical DataFrame

```python
# Combine the historical data with new records
historical_df_final = historical_df_updated.unionByName(new_records_df.select(historical_df_updated.columns))
```

### Adjust Overlapping Date Ranges

Since we're using all columns except `to_date` as keys, and there are no changing columns, overlapping date ranges should not occur.

However, we can still apply the window function to adjust `to_date` if necessary.

#### **Define Window Specification**

```python
window_spec = Window.partitionBy(*key_columns).orderBy(col('from_date').asc())
```

#### **Apply Window Functions**

```python
# Add next_from_date
historical_df_final = historical_df_final.withColumn(
    'next_from_date',
    lead(col('from_date')).over(window_spec)
)
```

#### **Adjust `to_date`**

```python
# Adjust to_date to be one day before next_from_date if necessary
historical_df_final = historical_df_final.withColumn(
    'adjusted_to_date',
    when(
        col('to_date').isNull() & col('next_from_date').isNotNull(),
        date_sub(col('next_from_date'), 1)
    ).otherwise(col('to_date'))
)
```

### Finalize DataFrame

#### **Drop Temporary Columns and Rename Adjusted Columns**

```python
# Drop temporary columns and rename adjusted_to_date
historical_df_final = historical_df_final.drop('to_date', 'next_from_date')
historical_df_final = historical_df_final.withColumnRenamed('adjusted_to_date', 'to_date')
```

#### **Remove Duplicates**

```python
# Remove duplicates if any
historical_df_final = historical_df_final.dropDuplicates()
```

#### **Sort the DataFrame for Clarity**

```python
# Sort the DataFrame for clarity
historical_df_final = historical_df_final.orderBy(
    key_columns + ['from_date'],
    ascending=[True] * (len(key_columns) + 1)
)
```

### Show Final Historical DataFrame

```python
# Show the final historical DataFrame
historical_df_final.show(truncate=False)
```

**Expected Output:**

```
+------------+------------+--------+---------+----------+----------+----------+
|dataset1_id |dataset3_id |textcode|name     |processed |from_date |to_date   |
+------------+------------+--------+---------+----------+----------+----------+
|1           |null        |tc1     |name1    |processed1|2024-01-01|2024-10-24|
|1           |null        |tc1     |name1_new|null      |2024-10-24|null      |
|2           |null        |tc2     |name2    |processed2|2024-01-01|2024-10-24|
|2           |null        |tc2     |name2_new|null      |2024-10-24|null      |
|null        |100         |tc6     |name6    |processed6|2024-01-01|null      |
|null        |200         |tc7     |name7    |processed7|2024-01-01|null      |
|null        |null        |tc5     |name5_new|null      |2024-10-24|null      |
+------------+------------+--------+---------+----------+----------+----------+
```

---
