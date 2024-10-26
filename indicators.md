## Import Libraries

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, when, lit, to_date, current_date, countDistinct, count, coalesce, lower, trim,
    date_sub, lead, lag
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

# Assertion: Check for non-null values in 'dataset1_id' and 'textcode'
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
    (5, 'tc5b', 'Name5b'),
    (6, 'tc2', 'Name2b')          # Collision: 'tc2' associated with different 'dataset1_id'
]

columns = ['dataset1_id', 'textcode', 'name']
dataset_2 = spark.createDataFrame(data2, columns)

# Assertion: Check for non-null values in 'dataset1_id' and 'textcode'
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

# Assertion: Check for non-null values in 'dataset3_id' and 'textcode'
assert dataset_3.filter(col('dataset3_id').isNull() | col('textcode').isNull()).count() == 0, \
    "dataset_3 should not have null values in 'dataset3_id' or 'textcode'"
```

---

## Data Cleaning: Standardize and Clean `textcode` Columns

```python
# Function to clean 'textcode' columns
def clean_textcode(df):
    return df.withColumn('textcode', trim(lower(col('textcode'))))

dataset_1 = clean_textcode(dataset_1)
dataset_2 = clean_textcode(dataset_2)
dataset_3 = clean_textcode(dataset_3)

# Assertion: Check for NULL values in 'textcode' columns after cleaning
for df_name, df in [('dataset_1', dataset_1), ('dataset_2', dataset_2), ('dataset_3', dataset_3)]:
    null_count = df.filter(col('textcode').isNull()).count()
    assert null_count == 0, f"{df_name} contains NULL values in 'textcode' column after cleaning"
```

---

## Step 2: Correctly Handle Collisions Between `dataset_1` and `dataset_2`

### Identify Collisions

```python
from pyspark.sql.functions import countDistinct

# Combine datasets to identify collisions
combined_datasets = dataset_1.select('dataset1_id', 'textcode').unionByName(
    dataset_2.select('dataset1_id', 'textcode')
)

# Find 'textcode's associated with multiple 'dataset1_id's
textcode_counts = combined_datasets.groupBy('textcode').agg(countDistinct('dataset1_id').alias('id_count'))

# Identify colliding 'textcode's
colliding_textcodes = textcode_counts.filter(col('id_count') > 1).select('textcode')

# Collect colliding 'textcode's into a list
colliding_textcodes_list = [row.textcode for row in colliding_textcodes.collect()]

# Assertion: There should be colliding 'textcode's
assert len(colliding_textcodes_list) > 0, "There should be colliding 'textcode's between dataset_1 and dataset_2"
```

---

### Exclude Colliding `textcode`s

```python
# Exclude colliding 'textcode's from 'dataset_2'
dataset_2_filtered = dataset_2.join(colliding_textcodes, on='textcode', how='left_anti')

# Exclude colliding 'textcode's from 'dataset_1'
dataset_1_filtered = dataset_1.join(colliding_textcodes, on='textcode', how='left_anti')

# Enhance 'dataset_1' with 'dataset_2_filtered'
enhanced_dataset1 = dataset_1_filtered.unionByName(dataset_2_filtered.select('dataset1_id', 'textcode', 'name'))

# Exclude colliding 'textcode's from 'enhanced_dataset1'
enhanced_dataset1_filtered = enhanced_dataset1.join(colliding_textcodes, on='textcode', how='left_anti')

# Assertion: 'enhanced_dataset1_filtered' should not contain colliding 'textcode's
assert enhanced_dataset1_filtered.filter(col('textcode').isin(colliding_textcodes_list)).count() == 0, \
    "enhanced_dataset1_filtered should not contain colliding 'textcode's"

# Exclude colliding 'textcode's from 'dataset_3'
dataset_3_filtered = dataset_3.join(colliding_textcodes, on='textcode', how='left_anti')

# Assertion: 'dataset_3_filtered' should not contain colliding 'textcode's
assert dataset_3_filtered.filter(col('textcode').isin(colliding_textcodes_list)).count() == 0, \
    "dataset_3_filtered should not contain colliding 'textcode's"
```

---

## Step 3: Find Common `textcode`s Between Enhanced `dataset_1` and `dataset_3`

### Identify Common `textcode`s

```python
# Find common 'textcode's between 'enhanced_dataset1_filtered' and 'dataset_3_filtered'
common_textcodes = enhanced_dataset1_filtered.select('textcode').intersect(dataset_3_filtered.select('textcode'))

# Assertion: There should be common 'textcode's
assert common_textcodes.count() > 0, "There should be common 'textcode's between enhanced_dataset1_filtered and dataset_3_filtered"
```

---

### Determine One-to-One Mappings

```python
# Count occurrences in 'enhanced_dataset1_filtered'
enhanced_dataset1_counts = enhanced_dataset1_filtered.groupBy('textcode').agg(count('dataset1_id').alias('dataset1_count'))

# Count occurrences in 'dataset_3_filtered'
dataset3_counts = dataset_3_filtered.groupBy('textcode').agg(count('dataset3_id').alias('dataset3_count'))

# Join counts with 'common_textcodes'
textcode_counts = common_textcodes.join(enhanced_dataset1_counts, on='textcode', how='inner') \
                                  .join(dataset3_counts, on='textcode', how='inner')

# Filter for 'textcode's where counts are both 1 (one-to-one mapping)
valid_textcodes = textcode_counts.filter(
    (col('dataset1_count') == 1) & (col('dataset3_count') == 1)
).select('textcode')

# Collect valid 'textcode's into a list
valid_textcodes_list = [row.textcode for row in valid_textcodes.collect()]

# Exclude colliding 'textcode's between 'enhanced_dataset1_filtered' and 'dataset_3_filtered'
colliding_textcodes_2 = common_textcodes.join(valid_textcodes, on='textcode', how='left_anti')

# Collect colliding 'textcode's into a list
colliding_textcodes_list_2 = [row.textcode for row in colliding_textcodes_2.collect()]

# Exclude colliding 'textcode's from 'enhanced_dataset1_filtered' and 'dataset_3_filtered'
enhanced_dataset1_filtered = enhanced_dataset1_filtered.join(colliding_textcodes_2, on='textcode', how='left_anti')
dataset_3_filtered = dataset_3_filtered.join(colliding_textcodes_2, on='textcode', how='left_anti')

# Assertions
assert enhanced_dataset1_filtered.filter(col('textcode').isin(colliding_textcodes_list_2)).count() == 0, \
    "enhanced_dataset1_filtered should not contain colliding 'textcode's from step 3"
assert dataset_3_filtered.filter(col('textcode').isin(colliding_textcodes_list_2)).count() == 0, \
    "dataset_3_filtered should not contain colliding 'textcode's from step 3"
```

---

## Step 4: Create `join_df` with All Identifiers from Enhanced `dataset_1` and `dataset_3`

```python
# Full outer join on 'textcode' to include all identifiers
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

# Assertion: 'join_df' should contain unique identifiers from both datasets
unique_ids = join_df.select('dataset1_id', 'dataset3_id').distinct().count()
assert unique_ids > 0, "join_df should contain unique identifiers from both datasets"
```

---

## Step 5: Update `current_df` with `join_df` by Handling Column Mismatch

### Assuming `current_df` Already Exists

```python
# Sample existing 'current_df' with additional columns
current_df = spark.createDataFrame([
    (1, None, 'other_value', 'name1', '2024-01-01', 'info1'),
    (2, None, 'other_value', 'name2', '2024-01-01', 'info2'),
    (5, None, 'other_value', 'name5', '2024-01-01', 'info5'),
    (100, None, 'other_value', 'name100', '2024-01-01', 'info100'),
    (None, 200, 'other_value', 'name200', '2024-01-01', 'info200')
], ['dataset1_id', 'dataset3_id', 'otherid', 'name', 'processing_date', 'additional_info'])

# Convert 'processing_date' to date type
current_df = current_df.withColumn('processing_date', to_date('processing_date'))
```

---

### Prepare `join_df`

```python
# Add 'processing_date' to 'join_df'
join_df = join_df.withColumn('processing_date', current_date())
```

---

### Update `current_df` While Handling Column Mismatch

```python
# Get all columns from 'current_df' and 'join_df'
current_columns = current_df.columns
join_columns = join_df.columns

# Identify date columns
date_columns = ['processing_date']

# Identify key columns (assuming known keys)
key_columns = ['dataset1_id', 'dataset3_id']

# Identify columns common to both DataFrames
common_columns = list(set(current_columns).intersection(set(join_columns)) - set(date_columns))

# Identify columns unique to 'current_df' and 'join_df'
current_only_columns = list(set(current_columns) - set(join_columns) - set(date_columns))
join_only_columns = list(set(join_columns) - set(current_columns) - set(date_columns))

# Join 'current_df' and 'join_df' on key columns
updated_current_df = current_df.alias('curr').join(
    join_df.alias('join'),
    on=key_columns,
    how='full_outer'
)

# Build select expressions
select_expr = []

# For key columns
for col_name in key_columns:
    curr_col = col('curr.' + col_name)
    join_col = col('join.' + col_name)
    select_expr.append(
        coalesce(join_col, curr_col).alias(col_name)
    )

# For common columns (excluding keys and dates)
for col_name in common_columns:
    curr_col = col('curr.' + col_name)
    join_col = col('join.' + col_name)
    select_expr.append(
        coalesce(join_col, curr_col).alias(col_name)
    )

# For columns only in 'current_df'
for col_name in current_only_columns:
    select_expr.append(col('curr.' + col_name).alias(col_name))

# For columns only in 'join_df'
for col_name in join_only_columns:
    select_expr.append(col('join.' + col_name).alias(col_name))

# Add the 'processing_date' column
select_expr.append(
    coalesce(col('join.processing_date'), col('curr.processing_date')).alias('processing_date')
)

# Build the 'updated_current_df'
updated_current_df = updated_current_df.select(*select_expr)

# Assertion: 'updated_current_df' should have the same columns as 'current_df' plus any new columns from 'join_df'
expected_columns = set(current_columns).union(set(join_only_columns))
assert set(updated_current_df.columns) == expected_columns, \
    "updated_current_df should have the correct set of columns"
```

---

### Handle New Entries

```python
# Identify new records present only in 'join_df'
new_records = join_df.alias('join').join(
    current_df.alias('curr'),
    on=key_columns,
    how='left_anti'
)

# Build select expressions for 'new_records'
new_select_expr = []

# For key columns
for col_name in key_columns:
    new_select_expr.append(col('join.' + col_name))

# For common columns (excluding keys and dates)
for col_name in common_columns:
    new_select_expr.append(col('join.' + col_name))

# For columns only in 'current_df', fill with None
for col_name in current_only_columns:
    new_select_expr.append(lit(None).cast('string').alias(col_name))

# For columns only in 'join_df'
for col_name in join_only_columns:
    new_select_expr.append(col('join.' + col_name))

# Add the 'processing_date' column
new_select_expr.append(col('join.processing_date'))

new_records = new_records.select(*new_select_expr)

# Combine 'updated_current_df' with 'new_records'
updated_current_df = updated_current_df.unionByName(new_records, allowMissingColumns=True)

# Assertion: All identifiers from 'join_df' should be in 'updated_current_df'
join_df_ids = join_df.select(*key_columns).distinct()
updated_current_df_ids = updated_current_df.select(*key_columns).distinct()
missing_ids = join_df_ids.exceptAll(updated_current_df_ids).count()
assert missing_ids == 0, "All identifiers from join_df should be in updated_current_df"
```

---

### Handle Missing Columns in 'updated_current_df'

Since `join_df` may introduce new columns not present in `current_df`, we need to ensure that `updated_current_df` has a consistent schema. If we prefer to keep the same schema as `current_df`, we can drop any extra columns after the union.

```python
# If desired, align 'updated_current_df' columns to match 'current_df'
updated_current_df = updated_current_df.select(current_columns)
```

---

## Step 6: Update `historical_df` Based on Changes

### Assuming `historical_df` Already Exists

```python
# Sample 'historical_df'
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

---

### Standardize Data Types and Casing

```python
# Standardize data types and casing in 'historical_df'
for col_name in historical_df.columns:
    if col_name not in {'from_date', 'to_date'}:
        historical_df = historical_df.withColumn(col_name, lower(col(col_name).cast('string')))

# Standardize data types and casing in 'updated_current_df'
for col_name in updated_current_df.columns:
    if col_name != 'processing_date':
        updated_current_df = updated_current_df.withColumn(col_name, lower(col(col_name).cast('string')))
```

---

### Define Key Columns

```python
# Key columns are all columns except date columns
date_columns = {'from_date', 'to_date', 'processing_date'}
key_columns_hist = [col_name for col_name in historical_df.columns if col_name not in date_columns]
```

---

### Join Historical and Updated Current Data

```python
# Join on all key columns
joined_hist_df = historical_df.alias('hist').join(
    updated_current_df.alias('curr'),
    on=key_columns_hist,
    how='full_outer'
)

# Build the change condition: since all columns except date columns are keys, any difference indicates a change
change_condition = col('curr.processing_date').isNotNull() & col('hist.from_date').isNotNull()

# Add an indicator to identify the source of each record
joined_hist_df = joined_hist_df.withColumn(
    'source',
    when(col('curr.processing_date').isNull(), lit('historical_only'))
    .when(col('hist.from_date').isNull(), lit('current_only'))
    .otherwise(
        when(change_condition, lit('both_changed'))
        .otherwise(lit('both_same'))
    )
)

# Assertion: 'joined_hist_df' should have records
assert joined_hist_df.count() > 0, "joined_hist_df should have records after the join"
```

---

### Identify Records to Update, Insert, or Keep

```python
# Records to deactivate (present in historical but not in current)
records_to_deactivate = joined_hist_df.filter(col('source') == 'historical_only')

# Records to insert (present only in current)
records_to_insert = joined_hist_df.filter(col('source') == 'current_only')

# Records to keep as is (present in both and same)
records_to_keep = joined_hist_df.filter(col('source') == 'both_same')

# Records to update (present in both but considered changed)
records_to_update = joined_hist_df.filter(col('source') == 'both_changed')

# Assertion: Total records after splitting should match 'joined_hist_df' count
total_records = (
    records_to_deactivate.count() +
    records_to_insert.count() +
    records_to_keep.count() +
    records_to_update.count()
)
assert total_records == joined_hist_df.count(), \
    "Total records after splitting should match the joined_hist_df count"
```

---

### Update `to_date` in Historical Records

```python
# For deactivated records, set 'to_date' to current processing date
deactivated_records = records_to_deactivate.select(
    *[col('hist.' + c).alias(c) for c in historical_df.columns if c != 'to_date'],
    current_date().alias('to_date')
)

# For updated records, set 'to_date' of old record and create a new record
updated_to_date_records = records_to_update.select(
    *[col('hist.' + c).alias(c) for c in historical_df.columns if c != 'to_date'],
    current_date().alias('to_date')
)

# Prepare new records from 'updated_current_df'
new_hist_records = records_to_insert.select(
    *[col('curr.' + c).alias(c) for c in historical_df.columns if c != 'from_date' and c != 'to_date'],
    col('curr.processing_date').alias('from_date'),
    lit(None).cast('date').alias('to_date')
)

# New versions of updated records
new_version_records = records_to_update.select(
    *[col('curr.' + c).alias(c) for c in historical_df.columns if c != 'from_date' and c != 'to_date'],
    col('curr.processing_date').alias('from_date'),
    lit(None).cast('date').alias('to_date')
)

# Assertion: 'deactivated_records' and 'updated_to_date_records' should have non-null 'to_date'
assert deactivated_records.filter(col('to_date').isNull()).count() == 0, \
    "deactivated_records should have non-null 'to_date'"
assert updated_to_date_records.filter(col('to_date').isNull()).count() == 0, \
    "updated_to_date_records should have non-null 'to_date'"
```

---

### Combine All Records

```python
# Combine all records
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

# Assertion: 'historical_df_updated' should have the combined number of records
expected_count = (
    records_to_keep.count() +
    deactivated_records.count() +
    updated_to_date_records.count() +
    new_hist_records.count() +
    new_version_records.count()
)
assert historical_df_updated.count() == expected_count, \
    "historical_df_updated should have the combined number of records"
```

---

## Step 7: Adjust Overlapping Date Ranges

### Define Window Specification

```python
# Define window specification
partition_columns = key_columns_hist
window_spec = Window.partitionBy(*partition_columns).orderBy(col('from_date').asc())
```

---

### Apply Window Functions

```python
# Add 'next_from_date'
historical_df_updated = historical_df_updated.withColumn(
    'next_from_date',
    lead(col('from_date')).over(window_spec)
)
```

---

### Adjust `to_date`

```python
# Adjust 'to_date' to be one day before 'next_from_date'
historical_df_final = historical_df_updated.withColumn(
    'adjusted_to_date',
    when(
        col('to_date').isNull() & col('next_from_date').isNotNull(),
        date_sub(col('next_from_date'), 1)
    ).otherwise(col('to_date'))
)

# Assertion: No overlaps in date ranges after adjustment
overlaps = historical_df_final.filter(
    col('adjusted_to_date') >= col('next_from_date')
).count()
assert overlaps == 0, "There should be no overlaps in date ranges after adjustment"
```

---

## Step 8: Finalize DataFrame

### Drop Temporary Columns and Rename Adjusted Columns

```python
# Drop temporary columns and rename 'adjusted_to_date'
historical_df_final = historical_df_final.drop('to_date', 'next_from_date', 'source')
historical_df_final = historical_df_final.withColumnRenamed('adjusted_to_date', 'to_date')
```

---

### Remove Duplicates and Sort the DataFrame

```python
# Remove duplicates if any
historical_df_final = historical_df_final.dropDuplicates()

# Sort the DataFrame for clarity
historical_df_final = historical_df_final.orderBy(
    key_columns_hist + ['from_date'],
    ascending=[True] * (len(key_columns_hist) + 1)
)

# Assertion: Final 'historical_df' should have no overlapping date ranges
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
