## Import Libraries

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, when, lit, to_date, current_date, countDistinct, count, coalesce, lower, trim,
    date_sub, lead, lag, greatest
)
from pyspark.sql.window import Window
from functools import reduce

# For assertions
import sys
```

---

## Initialize Spark Session

```python
# Initialize Spark session
spark = SparkSession.builder.appName("OptimizedHistoricalDataUpdate").getOrCreate()
```

```python
# Assertion: Check Spark session initialization
assert spark is not None, "Spark session should be initialized successfully."
print("Spark session initialized successfully.")
```

---

## Step 1: Read and Prepare Datasets

### Dataset 1 (`dataset_1`)

```python
# Sample data for dataset_1
data1 = [
    (1, 'tc1', 'Name1'),
    (2, 'tc2', 'Name2'),
    (3, 'tc3', 'Name3'),
    (4, 'tc4', 'Name4'),
    (5, 'tc5', 'Name5')
]

columns = ['dataset1_id', 'textcode', 'name']
dataset_1 = spark.createDataFrame(data1, columns)

# Display dataset_1
dataset_1.show(truncate=False)
```

```python
# Assertion: Check for non-null values in 'dataset1_id' and 'textcode'
null_count_ds1 = dataset_1.filter(col('dataset1_id').isNull() | col('textcode').isNull()).count()
assert null_count_ds1 == 0, "dataset_1 should not have null values in 'dataset1_id' or 'textcode'"
print("dataset_1 has no nulls in 'dataset1_id' or 'textcode'.")
```

### Dataset 2 (`dataset_2`)

```python
# Sample data for dataset_2 (additional textcodes for dataset_1)
data2 = [
    (1, 'tc1a', 'Name1a'),
    (2, 'tc2', 'Name2a'),
    (3, 'tc3b', 'Name3b'),
    (5, 'tc5', 'Name5a'),         # Duplicate textcode with dataset_1
    (5, 'tc5b', 'Name5b'),
    (6, 'tc2', 'Name2b')          # Collision: 'tc2' associated with different 'dataset1_id'
]

columns = ['dataset1_id', 'textcode', 'name']
dataset_2 = spark.createDataFrame(data2, columns)

# Display dataset_2
dataset_2.show(truncate=False)
```

```python
# Assertion: Check for non-null values in 'dataset1_id' and 'textcode'
null_count_ds2 = dataset_2.filter(col('dataset1_id').isNull() | col('textcode').isNull()).count()
assert null_count_ds2 == 0, "dataset_2 should not have null values in 'dataset1_id' or 'textcode'"
print("dataset_2 has no nulls in 'dataset1_id' or 'textcode'.")
```

### Dataset 3 (`dataset_3`)

```python
# Sample data for dataset_3
data3 = [
    (100, 'tc1', 'Name1_3'),
    (200, 'tc2', 'Name2_3'),
    (300, 'tc3', 'Name3_3'),
    (400, 'tc6', 'Name6'),
    (500, 'tc5', 'Name5_3'),
    (600, 'tc2', 'Name2_dup')  # Potential collision on 'tc2'
]

columns = ['dataset3_id', 'textcode', 'name']
dataset_3 = spark.createDataFrame(data3, columns)

# Display dataset_3
dataset_3.show(truncate=False)
```

```python
# Assertion: Check for non-null values in 'dataset3_id' and 'textcode'
null_count_ds3 = dataset_3.filter(col('dataset3_id').isNull() | col('textcode').isNull()).count()
assert null_count_ds3 == 0, "dataset_3 should not have null values in 'dataset3_id' or 'textcode'"
print("dataset_3 has no nulls in 'dataset3_id' or 'textcode'.")
```

---

## Data Cleaning: Standardize and Clean `textcode` Columns

```python
# Function to clean 'textcode' columns
def clean_textcode(df):
    return df.withColumn('textcode', trim(lower(col('textcode'))))

# Apply cleaning
dataset_1 = clean_textcode(dataset_1)
dataset_2 = clean_textcode(dataset_2)
dataset_3 = clean_textcode(dataset_3)

# Display cleaned datasets
print("Cleaned dataset_1:")
dataset_1.show(truncate=False)
print("Cleaned dataset_2:")
dataset_2.show(truncate=False)
print("Cleaned dataset_3:")
dataset_3.show(truncate=False)
```

```python
# Assertion: Check for NULL values in 'textcode' columns after cleaning
for df_name, df in [('dataset_1', dataset_1), ('dataset_2', dataset_2), ('dataset_3', dataset_3)]:
    null_count = df.filter(col('textcode').isNull()).count()
    assert null_count == 0, f"{df_name} contains NULL values in 'textcode' column after cleaning"
    print(f"{df_name} contains no NULL values in 'textcode' column after cleaning.")
```

---

## Step 2: Handle Collisions Between `dataset_1` and `dataset_2`

### Identify and Exclude Collisions

```python
# Combine dataset_1 and dataset_2 to identify collisions
combined_datasets = dataset_1.select('dataset1_id', 'textcode').unionByName(
    dataset_2.select('dataset1_id', 'textcode')
)

# Find 'textcode's associated with multiple 'dataset1_id's
colliding_textcodes = combined_datasets.groupBy('textcode').agg(
    countDistinct('dataset1_id').alias('id_count')
).filter(col('id_count') > 1).select('textcode')

# Collect colliding 'textcode's into a list
colliding_textcodes_list = [row.textcode for row in colliding_textcodes.collect()]

# Assertion: There should be colliding 'textcode's
assert len(colliding_textcodes_list) > 0, "There should be colliding 'textcode's between dataset_1 and dataset_2"

print(f"Colliding textcodes between dataset_1 and dataset_2: {colliding_textcodes_list}")
```

```python
# Exclude colliding 'textcode's from 'dataset_2'
dataset_2_filtered = dataset_2.join(colliding_textcodes, on='textcode', how='left_anti')

# Exclude colliding 'textcode's from 'dataset_1'
dataset_1_filtered = dataset_1.join(colliding_textcodes, on='textcode', how='left_anti')

# Enhance 'dataset_1' with non-colliding entries from 'dataset_2'
enhanced_dataset1 = dataset_1_filtered.unionByName(dataset_2_filtered)

# Assertion: 'enhanced_dataset1' should not contain colliding 'textcode's
colliding_in_enhanced_ds1 = enhanced_dataset1.filter(col('textcode').isin(colliding_textcodes_list)).count()
assert colliding_in_enhanced_ds1 == 0, "enhanced_dataset1 should not contain colliding 'textcode's"

print("Colliding textcodes successfully excluded from enhanced_dataset1.")
```

---

## Step 3: Resolve Collisions Between Enhanced `dataset_1` and `dataset_3`

### Identify and Exclude Collisions

```python
# Find common 'textcode's between enhanced_dataset1 and dataset_3
common_textcodes = enhanced_dataset1.select('textcode').intersect(dataset_3.select('textcode'))

# Find 'textcode's with one-to-one mapping in both datasets
valid_textcodes = common_textcodes.join(
    enhanced_dataset1.groupBy('textcode').agg(count('dataset1_id').alias('dataset1_count')),
    on='textcode'
).join(
    dataset_3.groupBy('textcode').agg(count('dataset3_id').alias('dataset3_count')),
    on='textcode'
).filter(
    (col('dataset1_count') == 1) & (col('dataset3_count') == 1)
).select('textcode')

# Collect valid 'textcode's into a list
valid_textcodes_list = [row.textcode for row in valid_textcodes.collect()]

# Assertion: There should be valid 'textcode's with one-to-one mapping
assert len(valid_textcodes_list) > 0, "There should be valid 'textcode's with one-to-one mapping between enhanced_dataset1 and dataset_3"

print(f"Valid one-to-one textcodes: {valid_textcodes_list}")
```

```python
# Filter enhanced_dataset1 and dataset_3 to include only valid 'textcode's
enhanced_dataset1_filtered = enhanced_dataset1.filter(col('textcode').isin(valid_textcodes_list))
dataset_3_filtered = dataset_3.filter(col('textcode').isin(valid_textcodes_list))

# Assertion: No colliding 'textcode's remain after filtering
remaining_collisions_ds1 = enhanced_dataset1_filtered.filter(col('textcode').isin(colliding_textcodes_list)).count()
remaining_collisions_ds3 = dataset_3_filtered.filter(col('textcode').isin(colliding_textcodes_list)).count()

assert remaining_collisions_ds1 == 0, "enhanced_dataset1_filtered should not contain original colliding 'textcode's"
assert remaining_collisions_ds3 == 0, "dataset_3_filtered should not contain original colliding 'textcode's"

print("Collisions between enhanced_dataset1 and dataset_3 resolved successfully.")
```

---

## Step 4: Create `join_df` with All Identifiers

```python
# Perform a full outer join on 'textcode' to include all identifiers
join_df = enhanced_dataset1_filtered.alias('d1').join(
    dataset_3_filtered.alias('d3'),
    on='textcode',
    how='full_outer'
).select(
    col('d1.dataset1_id'),
    col('d3.dataset3_id'),
    col('textcode'),
    col('d1.name').alias('name_dataset1'),
    col('d3.name').alias('name_dataset3')
)

# Display join_df
join_df.show(truncate=False)
```

```python
# Assertion: 'join_df' should contain unique identifiers from both datasets
unique_ids_join_df = join_df.select('dataset1_id', 'dataset3_id').distinct().count()
assert unique_ids_join_df > 0, "join_df should contain unique identifiers from both datasets"

print("join_df created successfully with unique identifiers from both datasets.")
```

---

## Step 5: Update `current_df` with Priority to `current_df` Data

### Assuming `current_df` Already Exists

```python
# Sample existing 'current_df' with additional columns
current_data = [
    (1, None, 'other_value', 'name1', '2024-01-01', 'info1'),
    (2, None, 'other_value', 'name2', '2024-01-01', 'info2'),
    (5, None, 'other_value', 'name5', '2024-01-01', 'info5'),
    (100, None, 'other_value', 'name100', '2024-01-01', 'info100'),
    (None, 200, 'other_value', 'name200', '2024-01-01', 'info200')
]

current_columns = ['dataset1_id', 'dataset3_id', 'otherid', 'name', 'processing_date', 'additional_info']
current_df = spark.createDataFrame(current_data, current_columns)

# Convert 'processing_date' to date type
current_df = current_df.withColumn('processing_date', to_date('processing_date'))

# Display current_df
current_df.show(truncate=False)
```

```python
# Assertion: Check for non-null key columns in `current_df`
null_keys_current = current_df.filter(
    col('dataset1_id').isNull() & col('dataset3_id').isNull()
).count()
assert null_keys_current == 0, "current_df should have at least one key column non-null for each record"

print("current_df has at least one non-null key column for each record.")
```

### Prepare `join_df`

```python
# Add 'processing_date' to 'join_df', prioritizing 'current_df'
join_df = join_df.withColumn('processing_date', current_date())

# Display updated join_df
join_df.show(truncate=False)
```

```python
# Assertion: 'processing_date' in 'join_df' should not be null
null_processing_date_join = join_df.filter(col('processing_date').isNull()).count()
assert null_processing_date_join == 0, "'processing_date' in join_df should not be null"

print("Processing dates added to join_df successfully.")
```

### Update `current_df`

```python
# Get all columns from 'current_df' and 'join_df' as sets
current_columns_set = set(current_df.columns)
join_columns_set = set(join_df.columns)

# Identify key columns
key_columns = {'dataset1_id', 'dataset3_id'}

# Identify other columns
common_columns = (current_columns_set & join_columns_set) - key_columns - {'processing_date'}
current_only_columns = current_columns_set - join_columns_set - key_columns - {'processing_date'}
join_only_columns = join_columns_set - current_columns_set - key_columns - {'processing_date'}

# Perform full outer join on key columns
joined_current_df = current_df.alias('curr').join(
    join_df.alias('join'),
    on=list(key_columns),
    how='full_outer'
)

# Get the list of available columns in 'joined_current_df'
available_columns = set(joined_current_df.columns)

# Function to safely retrieve columns
def get_column_expr(col_name, prefix_curr='curr.', prefix_join='join.'):
    curr_col = col(prefix_curr + col_name) if (prefix_curr + col_name) in available_columns else lit(None)
    join_col = col(prefix_join + col_name) if (prefix_join + col_name) in available_columns else lit(None)
    return coalesce(curr_col, join_col).alias(col_name)

# Build select expressions, giving priority to 'curr' data
select_expr = []

# Key columns
for col_name in key_columns:
    select_expr.append(get_column_expr(col_name))

# Common columns
for col_name in common_columns:
    select_expr.append(get_column_expr(col_name))

# Current only columns
for col_name in current_only_columns:
    curr_col_name = 'curr.' + col_name
    if curr_col_name in available_columns:
        select_expr.append(col(curr_col_name).alias(col_name))
    else:
        select_expr.append(lit(None).alias(col_name))

# Join only columns
for col_name in join_only_columns:
    join_col_name = 'join.' + col_name
    if join_col_name in available_columns:
        select_expr.append(col(join_col_name).alias(col_name))
    else:
        select_expr.append(lit(None).alias(col_name))

# 'processing_date', giving priority to 'curr'
curr_processing_date = col('curr.processing_date') if 'curr.processing_date' in available_columns else lit(None)
join_processing_date = col('join.processing_date') if 'join.processing_date' in available_columns else lit(None)
select_expr.append(coalesce(curr_processing_date, join_processing_date).alias('processing_date'))

# Build 'updated_current_df'
updated_current_df = joined_current_df.select(*select_expr)

# Display updated_current_df
updated_current_df.show(truncate=False)
```

```python
# Assertion: 'updated_current_df' should have the correct set of columns
expected_columns = current_columns_set.union(join_only_columns)
assert set(updated_current_df.columns) == expected_columns, \
    "updated_current_df should have the correct set of columns"

print("updated_current_df has the correct set of columns.")
```

```python
# Assertion: Ensure no duplicate columns in `updated_current_df`
assert len(updated_current_df.columns) == len(set(updated_current_df.columns)), "Duplicate column names found in updated_current_df"

print("updated_current_df has no duplicate column names.")
```

```python
# Assertion: All non-colliding 'textcode's from `join_df` are present in `updated_current_df`
join_df_ids = join_df.select(*key_columns).distinct()
updated_current_df_ids = updated_current_df.select(*key_columns).distinct()
missing_ids = join_df_ids.exceptAll(updated_current_df_ids).count()
assert missing_ids == 0, "All identifiers from join_df should be in updated_current_df"

print("All identifiers from join_df are present in updated_current_df.")
```

---

## Step 6: Update `historical_df` Based on Changes

### Assuming `historical_df` Already Exists

```python
# Sample 'historical_df'
historical_data = [
    (1, None, 'other_value', 'name1', '2024-01-01', None),
    (2, None, 'other_value', 'name2', '2024-01-01', None),
    (5, None, 'other_value', 'name5', '2024-01-01', None),
    (100, None, 'other_value', 'name100', '2024-01-01', None),
    (None, 200, 'other_value', 'name200', '2024-01-01', None)
]

historical_columns = ['dataset1_id', 'dataset3_id', 'otherid', 'name', 'from_date', 'to_date']
historical_df = spark.createDataFrame(historical_data, historical_columns)

# Convert date columns
historical_df = historical_df.withColumn('from_date', to_date('from_date')) \
                             .withColumn('to_date', to_date('to_date'))

# Display historical_df
historical_df.show(truncate=False)
```

```python
# Assertion: Check for non-null key columns in `historical_df`
null_keys_hist = historical_df.filter(
    col('dataset1_id').isNull() & col('dataset3_id').isNull()
).count()
assert null_keys_hist == 0, "historical_df should have at least one key column non-null for each record"

print("historical_df has at least one non-null key column for each record.")
```

### Standardize Data Types and Casing

```python
# Function to standardize columns
def standardize_df(df, date_cols):
    for col_name in df.columns:
        if col_name not in date_cols:
            df = df.withColumn(col_name, lower(col(col_name).cast('string')))
    return df

# Define date columns
date_columns = {'from_date', 'to_date', 'processing_date'}

# Standardize dataframes
historical_df = standardize_df(historical_df, date_columns)
updated_current_df = standardize_df(updated_current_df, date_columns)

# Display standardized dataframes
print("Standardized historical_df:")
historical_df.show(truncate=False)
print("Standardized updated_current_df:")
updated_current_df.show(truncate=False)
```

```python
# Assertion: Ensure no unexpected nulls in standardized columns
for df_name, df in [('historical_df', historical_df), ('updated_current_df', updated_current_df)]:
    non_date_columns = set(df.columns) - date_columns
    for col_name in non_date_columns:
        null_count = df.filter(col(col_name).isNull()).count()
        assert null_count == 0, f"{df_name} contains NULL values in '{col_name}' column after standardization"
    print(f"{df_name} contains no NULL values in standardized columns.")
```

### Join Historical and Updated Current Data

```python
# Key columns for historical_df
key_columns_hist = [col_name for col_name in historical_df.columns if col_name not in date_columns]

# Perform a full outer join on key columns
joined_hist_df = historical_df.alias('hist').join(
    updated_current_df.alias('curr'),
    on=key_columns_hist,
    how='full_outer'
)

# Define change condition: if 'processing_date' in 'curr' is more recent than 'from_date' in 'hist'
change_condition = (col('curr.processing_date') > col('hist.from_date')) & col('curr.processing_date').isNotNull()

# Add 'source' column to identify the origin of each record
joined_hist_df = joined_hist_df.withColumn(
    'source',
    when(col('curr.processing_date').isNull(), lit('historical_only'))
    .when(col('hist.from_date').isNull(), lit('current_only'))
    .otherwise(
        when(change_condition, lit('both_changed'))
        .otherwise(lit('both_same'))
    )
)

# Display joined_hist_df
joined_hist_df.select('hist.*', 'curr.processing_date', 'source').show(truncate=False)
```

```python
# Assertion: 'joined_hist_df' should have records
joined_hist_count = joined_hist_df.count()
assert joined_hist_count > 0, "joined_hist_df should have records after the join"

print(f"joined_hist_df contains {joined_hist_count} records.")
```

### Identify Records to Update, Insert, or Keep

```python
# Categorize records based on 'source'
records_to_deactivate = joined_hist_df.filter(col('source') == 'historical_only')
records_to_insert = joined_hist_df.filter(col('source') == 'current_only')
records_to_keep = joined_hist_df.filter(col('source') == 'both_same')
records_to_update = joined_hist_df.filter(col('source') == 'both_changed')

# Display counts for each category
print(f"Records to deactivate: {records_to_deactivate.count()}")
print(f"Records to insert: {records_to_insert.count()}")
print(f"Records to keep: {records_to_keep.count()}")
print(f"Records to update: {records_to_update.count()}")
```

```python
# Assertion: Total records after splitting should match 'joined_hist_df' count
total_split = (
    records_to_deactivate.count() +
    records_to_insert.count() +
    records_to_keep.count() +
    records_to_update.count()
)
assert total_split == joined_hist_count, "Total split records should match the joined_hist_df count"

print("Records categorized successfully.")
```

### Update `to_date` in Historical Records

```python
# For deactivated records, set 'to_date' to current processing date
deactivated_records = records_to_deactivate.select(
    *[col('hist.' + c).alias(c) for c in historical_df.columns if c != 'to_date'],
    current_date().alias('to_date')
)

# For updated records, set 'to_date' of old record
updated_to_date_records = records_to_update.select(
    *[col('hist.' + c).alias(c) for c in historical_df.columns if c != 'to_date'],
    current_date().alias('to_date')
)

# Prepare new records from 'updated_current_df'
new_hist_records = records_to_insert.select(
    *[col('curr.' + c).alias(c) for c in historical_df.columns if c not in {'from_date', 'to_date'}],
    col('curr.processing_date').alias('from_date'),
    lit(None).cast('date').alias('to_date')
)

# New versions of updated records
new_version_records = records_to_update.select(
    *[col('curr.' + c).alias(c) for c in historical_df.columns if c not in {'from_date', 'to_date'}],
    col('curr.processing_date').alias('from_date'),
    lit(None).cast('date').alias('to_date')
)

# Display the prepared records
print("Deactivated Records:")
deactivated_records.show(truncate=False)
print("Updated To_Date Records:")
updated_to_date_records.show(truncate=False)
print("New Historical Records:")
new_hist_records.show(truncate=False)
print("New Version Records:")
new_version_records.show(truncate=False)
```

```python
# Assertions:

# 'deactivated_records' and 'updated_to_date_records' should have non-null 'to_date'
null_deactivated = deactivated_records.filter(col('to_date').isNull()).count()
null_updated_to_date = updated_to_date_records.filter(col('to_date').isNull()).count()

assert null_deactivated == 0, "'to_date' in deactivated_records should not be null"
assert null_updated_to_date == 0, "'to_date' in updated_to_date_records should not be null"

print("to_date updated successfully for deactivated and updated records.")
```

### Combine All Records

```python
# Combine all records into 'historical_df_updated'
historical_df_updated = records_to_keep.select(
    *[col('hist.' + c).alias(c) for c in historical_df.columns]
).unionByName(
    deactivated_records
).unionByName(
    updated_to_date_records
).unionByName(
    new_hist_records
).unionByName(
    new_version_records
)

# Display combined historical_df_updated
print("Combined historical_df_updated:")
historical_df_updated.show(truncate=False)
```

```python
# Assertion: 'historical_df_updated' should have the combined number of records
expected_count = (
    records_to_keep.count() +
    deactivated_records.count() +
    updated_to_date_records.count() +
    new_hist_records.count() +
    new_version_records.count()
)
actual_count = historical_df_updated.count()
assert actual_count == expected_count, "historical_df_updated should have the combined number of records"

print(f"historical_df_updated contains the correct number of records: {actual_count}")
```

---

## Step 7: Adjust Overlapping Date Ranges

### Define Window Specification and Adjust `to_date`

```python
# Define window specification
partition_columns = key_columns_hist
window_spec = Window.partitionBy(*partition_columns).orderBy(col('from_date').asc())

# Adjust 'to_date' to be one day before the next 'from_date'
historical_df_final = historical_df_updated.withColumn(
    'next_from_date',
    lead(col('from_date')).over(window_spec)
).withColumn(
    'to_date',
    when(
        col('to_date').isNull() & col('next_from_date').isNotNull(),
        date_sub(col('next_from_date'), 1)
    ).otherwise(col('to_date'))
).drop('next_from_date')

# Display historical_df_final
historical_df_final.show(truncate=False)
```

```python
# Assertion: No overlaps in date ranges after adjustment
# Define a new window for checking overlaps
window_spec_check = Window.partitionBy(*partition_columns).orderBy(col('from_date').asc())

# Add 'prev_to_date' to check overlaps
historical_df_check = historical_df_final.withColumn(
    'prev_to_date',
    lag(col('to_date')).over(window_spec_check)
)

# Check for overlaps
overlaps_check = historical_df_check.filter(
    col('from_date') <= col('prev_to_date')
).count()

assert overlaps_check == 0, "Final historical_df should have no overlapping date ranges"

print("No overlapping date ranges in historical_df_final.")
```

---

## Step 8: Finalize DataFrame

### Remove Duplicates and Sort the DataFrame

```python
# Remove duplicates if any
historical_df_final = historical_df_final.dropDuplicates()

# Sort the DataFrame for clarity
historical_df_final = historical_df_final.orderBy(
    key_columns_hist + ['from_date'],
    ascending=[True] * (len(key_columns_hist) + 1)
)

# Display final historical_df_final
print("Final historical_df_final:")
historical_df_final.show(truncate=False)
```

```python
# Assertion: Final 'historical_df_final' has no duplicates
duplicate_count = historical_df_final.count() - historical_df_final.dropDuplicates().count()
assert duplicate_count == 0, "historical_df_final should have no duplicate records"

print("historical_df_final finalized successfully with no duplicates.")
```

---

## Step 9: Show Final `historical_df`

```python
# Display the final historical_df
print("Final historical_df:")
historical_df_final.show(truncate=False)
```

```python
# Assertion: Validate final historical_df schema and data
# Expected schema
expected_schema = set(['dataset1_id', 'dataset3_id', 'otherid', 'name', 'from_date', 'to_date'])

# Check schema
actual_schema = set(historical_df_final.columns)
assert actual_schema == expected_schema, "historical_df_final schema does not match expected schema"

print("Final historical_df schema is as expected.")
```
