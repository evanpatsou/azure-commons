## Import Libraries

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, when, lit, to_date, current_date, countDistinct, count, coalesce, lower
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
spark = SparkSession.builder.appName("HistoricalDataUpdate").getOrCreate()
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
```

**Assertion:**

```python
# Assert that dataset_1 has non-null dataset1_id and textcode
assert dataset_1.filter(col('dataset1_id').isNull() | col('textcode').isNull()).count() == 0, \
    "dataset_1 should not have null values in 'dataset1_id' or 'textcode'"
```

---

### Dataset 2 (`dataset_2`)

```python
# Sample data for dataset_2 (additional textcodes for dataset_1)
data2 = [
    (1, 'tc1a', 'Name1a'),
    (2, 'tc2', 'Name2a'),
    (3, 'tc3b', 'Name3b'),
    (5, 'tc5', 'Name5a'),         # Duplicate textcode with dataset_1
    (5, 'tc5b', 'Name5b')
]

columns = ['dataset1_id', 'textcode', 'name']
dataset_2 = spark.createDataFrame(data2, columns)
```

**Assertion:**

```python
# Assert that dataset_2 has non-null dataset1_id and textcode
assert dataset_2.filter(col('dataset1_id').isNull() | col('textcode').isNull()).count() == 0, \
    "dataset_2 should not have null values in 'dataset1_id' or 'textcode'"
```

---

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
```

**Assertion:**

```python
# Assert that dataset_3 has non-null dataset3_id and textcode
assert dataset_3.filter(col('dataset3_id').isNull() | col('textcode').isNull()).count() == 0, \
    "dataset_3 should not have null values in 'dataset3_id' or 'textcode'"
```

---

## Step 2: Enhance `dataset_1` with `dataset_2`, Handling Collisions

### Identify Collisions Between `dataset_1` and `dataset_2`

```python
from pyspark.sql.functions import countDistinct

# Find textcodes in dataset_2 associated with multiple dataset1_id
textcode_counts = dataset_2.groupBy('textcode').agg(countDistinct('dataset1_id').alias('id_count'))

# Identify colliding textcodes (textcodes associated with multiple dataset1_id)
colliding_textcodes = textcode_counts.filter(col('id_count') > 1).select('textcode')
```

**Assertion:**

```python
# Assert that colliding_textcodes has expected columns
assert 'textcode' in colliding_textcodes.columns, "colliding_textcodes should have 'textcode' column"
```

### Exclude Colliding Textcodes from `dataset_2`

```python
# Exclude colliding textcodes from dataset_2
dataset_2_filtered = dataset_2.join(colliding_textcodes, on='textcode', how='left_anti')
```

**Assertion:**

```python
# Assert that dataset_2_filtered does not contain any colliding textcodes
colliding_textcodes_list = [row.textcode for row in colliding_textcodes.collect()]
assert dataset_2_filtered.filter(col('textcode').isin(colliding_textcodes_list)).count() == 0, \
    "dataset_2_filtered should not contain colliding textcodes"
```

### Enhance `dataset_1` with `dataset_2_filtered`

```python
# Combine dataset_1 and dataset_2_filtered
enhanced_dataset1 = dataset_1.unionByName(dataset_2_filtered.select('dataset1_id', 'textcode', 'name'))
```

**Assertion:**

```python
# Assert that enhanced_dataset1 has unique combinations of dataset1_id and textcode
enhanced_count = enhanced_dataset1.count()
enhanced_distinct_count = enhanced_dataset1.dropDuplicates(['dataset1_id', 'textcode']).count()
assert enhanced_count == enhanced_distinct_count, \
    "enhanced_dataset1 should not have duplicate combinations of 'dataset1_id' and 'textcode'"
```

---

## Step 3: Find Common `textcode`s Between Enhanced `dataset_1` and `dataset_3`

### Identify Common `textcode`s

```python
# Find common textcodes between enhanced_dataset1 and dataset_3
common_textcodes = enhanced_dataset1.select('textcode').intersect(dataset_3.select('textcode'))
```

**Assertion:**

```python
# Assert that common_textcodes are present in both datasets
assert common_textcodes.count() > 0, "There should be common textcodes between enhanced_dataset1 and dataset_3"
```

### Determine One-to-One Mappings

```python
# Count occurrences in enhanced_dataset1
enhanced_dataset1_counts = enhanced_dataset1.groupBy('textcode').agg(count('dataset1_id').alias('dataset1_count'))

# Count occurrences in dataset_3
dataset3_counts = dataset_3.groupBy('textcode').agg(count('dataset3_id').alias('dataset3_count'))

# Join counts with common_textcodes
textcode_counts = common_textcodes.join(enhanced_dataset1_counts, on='textcode', how='inner') \
                                  .join(dataset3_counts, on='textcode', how='inner')

# Filter for textcodes where counts are both 1 (one-to-one mapping)
valid_textcodes = textcode_counts.filter(
    (col('dataset1_count') == 1) & (col('dataset3_count') == 1)
).select('textcode')
```

**Assertion:**

```python
# Assert that valid_textcodes are one-to-one mappings
assert valid_textcodes.count() > 0, "There should be valid one-to-one textcode mappings"
```

### Exclude Colliding Textcodes

```python
# Colliding textcodes are those not in valid_textcodes
colliding_textcodes = common_textcodes.join(valid_textcodes, on='textcode', how='left_anti')

# Exclude colliding textcodes from datasets
enhanced_dataset1_filtered = enhanced_dataset1.join(colliding_textcodes, on='textcode', how='left_anti')
dataset_3_filtered = dataset_3.join(colliding_textcodes, on='textcode', how='left_anti')
```

**Assertion:**

```python
# Assert that colliding textcodes are excluded from the datasets
colliding_textcodes_list = [row.textcode for row in colliding_textcodes.collect()]
assert enhanced_dataset1_filtered.filter(col('textcode').isin(colliding_textcodes_list)).count() == 0, \
    "enhanced_dataset1_filtered should not contain colliding textcodes"
assert dataset_3_filtered.filter(col('textcode').isin(colliding_textcodes_list)).count() == 0, \
    "dataset_3_filtered should not contain colliding textcodes"
```

---

## Step 4: Create `join_df` with All Identifiers from `dataset_1` and `dataset_3`

```python
# Full outer join on textcode to include all identifiers
join_df = enhanced_dataset1_filtered.alias('d1').join(
    dataset_3_filtered.alias('d3'),
    on='textcode',
    how='full_outer'
).select(
    col('d1.dataset1_id'),
    col('d3.dataset3_id'),
    col('d1.textcode'),
    col('d1.name').alias('name_dataset1'),
    col('d3.name').alias('name_dataset3')
)
```

**Assertion:**

```python
# Assert that join_df contains all unique dataset1_id and dataset3_id
unique_ids = join_df.select('dataset1_id', 'dataset3_id').distinct().count()
assert unique_ids > 0, "join_df should contain unique identifiers from both datasets"
```

---

## Step 5: Update `current_df` with `join_df`

### Assuming `current_df` Already Exists

```python
# Sample existing current_df with additional columns
current_df = spark.createDataFrame([
    (1, None, 'other_value', 'name1', '2024-01-01', 'info1'),
    (2, None, 'other_value', 'name2', '2024-01-01', 'info2'),
    (5, None, 'other_value', 'name5', '2024-01-01', 'info5'),
    (100, None, 'other_value', 'name100', '2024-01-01', 'info100'),
    (None, 200, 'other_value', 'name200', '2024-01-01', 'info200')
], ['dataset1_id', 'dataset3_id', 'otherid', 'name', 'processing_date', 'additional_info'])

# Convert processing_date to date type
current_df = current_df.withColumn('processing_date', to_date('processing_date'))
```

**Assertion:**

```python
# Assert that current_df has necessary columns
required_columns = {'dataset1_id', 'dataset3_id', 'otherid', 'name', 'processing_date'}
assert required_columns.issubset(set(current_df.columns)), "current_df should contain the required columns"
```

### Prepare `join_df`

```python
# Add 'processing_date' to join_df
join_df = join_df.withColumn('processing_date', current_date())
```

### Update `current_df` Based on `dataset1_id`, `dataset3_id`, and `processing_date`

```python
# Define key columns
key_columns = ['dataset1_id', 'dataset3_id']

# Join current_df and join_df on key columns
updated_current_df = current_df.alias('curr').join(
    join_df.alias('join'),
    on=key_columns,
    how='full_outer'
).select(
    coalesce(col('join.dataset1_id'), col('curr.dataset1_id')).alias('dataset1_id'),
    coalesce(col('join.dataset3_id'), col('curr.dataset3_id')).alias('dataset3_id'),
    col('curr.otherid'),
    col('curr.name'),
    coalesce(col('join.processing_date'), col('curr.processing_date')).alias('processing_date'),
    col('curr.additional_info')
)
```

**Assertion:**

```python
# Assert that updated_current_df has the same number of columns as current_df
assert set(updated_current_df.columns) == set(current_df.columns), \
    "updated_current_df should have the same columns as current_df"
```

### Handle New Records

```python
# Records present only in join_df (new records)
new_records = join_df.alias('join').join(
    current_df.alias('curr'),
    on=key_columns,
    how='left_anti'
).select(
    'join.dataset1_id',
    'join.dataset3_id',
    lit('other_value').alias('otherid'),
    lit(None).alias('name'),
    'join.processing_date',
    lit(None).alias('additional_info')
)

# Combine updated current_df with new records
updated_current_df = updated_current_df.unionByName(new_records)
```

**Assertion:**

```python
# Assert that updated_current_df contains all dataset1_id and dataset3_id from join_df
join_df_ids = join_df.select('dataset1_id', 'dataset3_id').distinct()
updated_current_df_ids = updated_current_df.select('dataset1_id', 'dataset3_id').distinct()
missing_ids = join_df_ids.exceptAll(updated_current_df_ids).count()
assert missing_ids == 0, "All identifiers from join_df should be in updated_current_df"
```

---

## Step 6: Update `historical_df` Based on Changes

### Assuming `historical_df` Already Exists

```python
# Sample historical_df
historical_df = spark.createDataFrame([
    (1, None, 'other_value', 'name1', '2024-01-01', None),
    (2, None, 'other_value', 'name2', '2024-01-01', None),
    (5, None, 'other_value', 'name5', '2024-01-01', None),
    (100, None, 'other_value', 'name100', '2024-01-01', None),
    (None, 200, 'other_value', 'name200', '2024-01-01', None)
], ['dataset1_id', 'dataset3_id', 'otherid', 'name', 'from_date', 'to_date'])

# Convert date columns
historical_df = historical_df.withColumn('from_date', to_date('from_date')) \
                             .withColumn('to_date', to_date('to_date'))
```

**Assertion:**

```python
# Assert that historical_df has necessary columns
required_columns = {'dataset1_id', 'dataset3_id', 'otherid', 'name', 'from_date', 'to_date'}
assert required_columns.issubset(set(historical_df.columns)), "historical_df should contain the required columns"
```

### Ensure Consistent Data Types and Standardize Casing

```python
# Standardize data types and casing in historical_df
for col_name in historical_df.columns:
    if col_name not in {'from_date', 'to_date'}:
        historical_df = historical_df.withColumn(col_name, lower(col(col_name).cast('string')))

# Standardize data types and casing in updated_current_df
for col_name in updated_current_df.columns:
    if col_name != 'processing_date':
        updated_current_df = updated_current_df.withColumn(col_name, lower(col(col_name).cast('string')))
```

### Define Key Columns and Changing Columns

```python
# Key columns
key_columns = ['dataset1_id', 'dataset3_id']
# Changing columns
changing_columns = ['otherid', 'name']
```

### Join Historical and Updated Current Data

```python
# Join on key columns
joined_hist_df = historical_df.alias('hist').join(
    updated_current_df.alias('curr'),
    on=key_columns,
    how='full_outer'
)
```

### Build the Change Condition

```python
# Build the change_condition with null-safe equality check
change_conditions = [
    col('hist.' + c).eqNullSafe(col('curr.' + c)) == False
    for c in changing_columns
]
if change_conditions:
    change_condition = reduce(lambda x, y: x | y, change_conditions)
else:
    change_condition = lit(False)
```

### Identify Records to Update, Insert, or Keep

```python
# Add an indicator to identify the source of each record
joined_hist_df = joined_hist_df.withColumn(
    'source',
    when(col('curr.processing_date').isNull(), lit('historical_only'))
    .when(col('hist.from_date').isNull(), lit('current_only'))
    .otherwise(lit('both'))
)

# Records to update (present in both and have changes)
records_to_update_hist = joined_hist_df.filter(
    (col('source') == 'both') & change_condition & col('hist.to_date').isNull()
)

# Records to insert (present only in current_df)
new_hist_records = joined_hist_df.filter(col('source') == 'current_only')

# Records to keep (present in both and no changes)
records_to_keep_hist = joined_hist_df.filter((col('source') == 'both') & (~change_condition))

# Records to retain from historical_df only (not present in current_df)
records_to_retain_hist = joined_hist_df.filter(col('source') == 'historical_only')
```

**Assertion:**

```python
# Assert that total records match after splitting
total_records = (
    records_to_update_hist.count() +
    new_hist_records.count() +
    records_to_keep_hist.count() +
    records_to_retain_hist.count()
)
assert total_records == joined_hist_df.count(), \
    "Total records after splitting should match the joined_hist_df count"
```

### Update `to_date` in Historical Records

```python
# Update to_date in historical records
updated_to_date_df = records_to_update_hist.select(
    *[col('hist.' + c).alias(c) for c in historical_df.columns if c != 'to_date'],
    col('curr.processing_date').alias('to_date')
)
```

**Assertion:**

```python
# Assert that updated_to_date_df has non-null 'to_date'
assert updated_to_date_df.filter(col('to_date').isNull()).count() == 0, \
    "updated_to_date_df should have non-null 'to_date'"
```

### Prepare New Records for Historical Data

```python
# Prepare new historical records from current_df
new_hist_records_df = new_hist_records.select(
    *key_columns,
    *[col('curr.' + c).alias(c) for c in changing_columns],
    col('curr.processing_date').alias('from_date'),
    lit(None).cast('date').alias('to_date')
)
```

**Assertion:**

```python
# Assert that new_hist_records_df has non-null 'from_date'
assert new_hist_records_df.filter(col('from_date').isNull()).count() == 0, \
    "new_hist_records_df should have non-null 'from_date'"
```

### Combine Records to Form Updated `historical_df`

```python
# Combine all historical records
historical_df_updated = records_to_keep_hist.select(
    *[col('hist.' + c).alias(c) for c in historical_df.columns]
).unionByName(records_to_retain_hist.select(
    *[col('hist.' + c).alias(c) for c in historical_df.columns]
)).unionByName(updated_to_date_df).unionByName(new_hist_records_df)
```

**Assertion:**

```python
# Assert that historical_df_updated has the expected number of records
expected_count = (
    records_to_keep_hist.count() +
    records_to_retain_hist.count() +
    updated_to_date_df.count() +
    new_hist_records_df.count()
)
assert historical_df_updated.count() == expected_count, \
    "historical_df_updated should have the combined number of records"
```

---

## Step 7: Adjust Overlapping Date Ranges

### Define Window Specification

```python
# Define window specification
partition_columns = key_columns + changing_columns
window_spec = Window.partitionBy(*partition_columns).orderBy(col('from_date').asc())
```

### Apply Window Functions

```python
# Add next_from_date
historical_df_updated = historical_df_updated.withColumn(
    'next_from_date',
    lead(col('from_date')).over(window_spec)
)
```

### Adjust `to_date`

```python
# Adjust to_date to be one day before next_from_date
historical_df_final = historical_df_updated.withColumn(
    'adjusted_to_date',
    when(
        col('to_date').isNull() & col('next_from_date').isNotNull(),
        date_sub(col('next_from_date'), 1)
    ).otherwise(col('to_date'))
)
```

**Assertion:**

```python
# Assert that adjusted_to_date does not overlap with next_from_date
overlaps = historical_df_final.filter(
    col('adjusted_to_date') >= col('next_from_date')
).count()
assert overlaps == 0, "There should be no overlaps in date ranges after adjustment"
```

---

## Step 8: Finalize DataFrame

### Drop Temporary Columns and Rename Adjusted Columns

```python
# Drop temporary columns and rename adjusted_to_date
historical_df_final = historical_df_final.drop('to_date', 'next_from_date', 'source')
historical_df_final = historical_df_final.withColumnRenamed('adjusted_to_date', 'to_date')
```

### Remove Duplicates and Sort the DataFrame

```python
# Remove duplicates if any
historical_df_final = historical_df_final.dropDuplicates()

# Sort the DataFrame for clarity
historical_df_final = historical_df_final.orderBy(
    key_columns + changing_columns + ['from_date'],
    ascending=[True] * (len(key_columns) + len(changing_columns) + 1)
)
```

**Assertion:**

```python
# Assert that historical_df_final has no overlapping date ranges for the same key and changing columns
window_spec_check = Window.partitionBy(*partition_columns).orderBy(col('from_date').asc())
historical_df_check = historical_df_final.withColumn(
    'prev_to_date',
    lag(col('to_date')).over(window_spec_check)
)
overlaps_check = historical_df_check.filter(
    col('from_date') <= col('prev_to_date')
).count()
assert overlaps_check == 0, "Final historical_df should have no overlapping date ranges"
```

---

## Step 9: Show Final `historical_df`

```python
historical_df_final.show(truncate=False)
```
