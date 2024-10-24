# Historical Data Update with Collision Handling and Date Adjustments

This Jupyter notebook demonstrates how to update a historical DataFrame (`historical_df`) based on changes detected in a current DataFrame (`current_df`), while handling collisions and adjusting overlapping date ranges. The notebook includes steps to:

- Read and prepare sample data for `dataset_1`, `dataset_2`, and `dataset_3`.
- Handle collisions between datasets.
- Create and update `current_df`.
- Update `historical_df` based on detected changes.
- Adjust overlapping date ranges in the historical data.
- Save the updated `historical_df` to a PostgreSQL database.

---

## Import Libraries

```python
# Import necessary libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, when, lit, to_date, row_number, lead, date_sub, current_date, countDistinct, coalesce
)
from pyspark.sql.window import Window
from functools import reduce
```

---

## Initialize Spark Session

```python
# Initialize Spark session
spark = SparkSession.builder.appName("HistoricalDataUpdate").getOrCreate()
```

---

## Step 1: Read and Prepare Datasets

### **Dataset 1 (`dataset_1`)**

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

### **Dataset 2 (`dataset_2`)**

```python
# Sample data for dataset_2 (additional textcodes for dataset_1)
data2 = [
    (1, 'tc1a', 'Name1a'),
    (2, 'tc2', 'Name2a'),         # Potential collision
    (3, 'tc3b', 'Name3b'),
    (5, 'tc5', 'Name5a'),         # Duplicate textcode with dataset_1
    (5, 'tc5b', 'Name5b')
]

columns = ['dataset1_id', 'textcode', 'name']
dataset_2 = spark.createDataFrame(data2, columns)
```

### **Dataset 3 (`dataset_3`)**

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

---

## Step 2: Enhance `dataset_1` with `dataset_2`, Handling Collisions

### **Identify Collisions Between `dataset_1` and `dataset_2`**

```python
from pyspark.sql.functions import countDistinct

# Find textcodes in dataset_2 associated with multiple dataset1_id
textcode_counts = dataset_2.groupBy('textcode').agg(countDistinct('dataset1_id').alias('id_count'))

# Identify colliding textcodes (textcodes associated with multiple dataset1_id)
colliding_textcodes = textcode_counts.filter(col('id_count') > 1).select('textcode')
```

### **Exclude Colliding Textcodes from `dataset_2`**

```python
# Exclude colliding textcodes from dataset_2
dataset_2_filtered = dataset_2.join(colliding_textcodes, on='textcode', how='left_anti')
```

### **Enhance `dataset_1` with `dataset_2_filtered`**

```python
# Combine dataset_1 and dataset_2_filtered
enhanced_dataset1 = dataset_1.unionByName(dataset_2_filtered.select('dataset1_id', 'textcode', 'name'))
```

---

## Step 3: Find Common `textcode`s Between Enhanced `dataset_1` and `dataset_3`

### **Identify Common `textcode`s**

```python
# Find common textcodes between enhanced_dataset1 and dataset_3
common_textcodes = enhanced_dataset1.select('textcode').intersect(dataset_3.select('textcode'))
```

### **Determine One-to-One Mappings**

```python
# Count occurrences in enhanced_dataset1
enhanced_dataset1_counts = enhanced_dataset1.groupBy('textcode').agg(count('dataset1_id').alias('dataset1_count'))

# Count occurrences in dataset_3
dataset3_counts = dataset_3.groupBy('textcode').agg(count('dataset3_id').alias('dataset3_count'))

# Join counts with common_textcodes
textcode_counts = common_textcodes.join(enhanced_dataset1_counts, on='textcode', how='inner') \
                                  .join(dataset3_counts, on='textcode', how='inner')

# Filter for textcodes where counts are both 1 (one-to-one mapping)
valid_textcodes = textcode_counts.filter((col('dataset1_count') == 1) & (col('dataset3_count') == 1)).select('textcode')
```

### **Exclude Colliding Textcodes**

```python
# Colliding textcodes are those not in valid_textcodes
colliding_textcodes = common_textcodes.join(valid_textcodes, on='textcode', how='left_anti')

# Exclude colliding textcodes from datasets
enhanced_dataset1_filtered = enhanced_dataset1.join(colliding_textcodes, on='textcode', how='left_anti')
dataset_3_filtered = dataset_3.join(colliding_textcodes, on='textcode', how='left_anti')
```

---

## Step 4: Join `enhanced_dataset1_filtered` and `dataset_3_filtered`

```python
# Join datasets on textcode
joined_df = enhanced_dataset1_filtered.join(dataset_3_filtered, on='textcode', how='inner') \
                                      .select('dataset1_id', 'dataset3_id', 'textcode', 'name', 'name_3')

# Rename columns for clarity
joined_df = joined_df.withColumnRenamed('name', 'name_dataset1') \
                     .withColumnRenamed('name_3', 'name_dataset3')
```

---

## Step 5: Update `current_df` with `joined_df`

### **Assuming `current_df` Already Exists**

```python
# Sample existing current_df
current_df = spark.createDataFrame([
    (1, None, 'other_value', 'name1', '2024-01-01'),
    (2, None, 'other_value', 'name2', '2024-01-01'),
    (5, None, 'other_value', 'name5', '2024-01-01')
], ['dataset1_id', 'dataset3_id', 'otherid', 'name', 'processing_date'])

# Convert processing_date to date type
current_df = current_df.withColumn('processing_date', to_date('processing_date'))
```

### **Prepare `joined_df`**

```python
# Add 'otherid' and 'processing_date' to joined_df
from pyspark.sql.functions import current_date

joined_df = joined_df.withColumn('otherid', lit('other_value')) \
                     .withColumn('processing_date', current_date()) \
                     .withColumnRenamed('name_dataset1', 'name')

# Select and rename columns to match current_df structure
joined_df = joined_df.select(
    'dataset1_id',
    'dataset3_id',
    'otherid',
    'name',
    'processing_date'
)
```

---

## Step 6: Update `current_df`

### **Define Key and Changing Columns**

```python
# Define key columns and changing columns
key_columns = ['dataset1_id']
changing_columns = ['dataset3_id', 'otherid', 'name']
```

### **Ensure Consistent Data Types and Standardize Casing**

```python
# Standardize data types and casing
for col_name in current_df.columns:
    if col_name != 'processing_date':
        current_df = current_df.withColumn(col_name, col(col_name).cast('string').lower())
        joined_df = joined_df.withColumn(col_name, col(col_name).cast('string').lower())
```

### **Join `current_df` and `joined_df`**

```python
# Join on key columns
joined_current_df = current_df.alias('curr').join(
    joined_df.alias('join'),
    on=key_columns,
    how='full_outer'
)
```

### **Build the Change Condition with Null Handling**

```python
# Build the change_condition with null-safe equality check
change_conditions = [
    col(f'curr.{c}').eqNullSafe(col(f'join.{c}')) == False
    for c in changing_columns
]

# Combine all conditions using logical OR
change_condition = reduce(lambda x, y: x | y, change_conditions)
```

### **Identify Records to Update**

```python
# Records to update
records_to_update = joined_current_df.filter(change_condition & col('curr.dataset1_id').isNotNull())

# Records to insert
records_to_insert = joined_current_df.filter(col('curr.dataset1_id').isNull())

# Records to keep
records_to_keep = joined_current_df.filter(~change_condition & col('curr.dataset1_id').isNotNull())
```

### **Update `current_df`**

```python
# Prepare updated records
updated_records = records_to_update.select(
    *[col('join.' + c).alias(c) for c in current_df.columns]
)

# Prepare new records
new_records = records_to_insert.select(
    *[col('join.' + c).alias(c) for c in current_df.columns]
)

# Combine records
updated_current_df = records_to_keep.select('curr.*').unionByName(updated_records).unionByName(new_records)
```

---

## Step 7: Update `historical_df` Based on Changes

### **Ensure Consistent Data Types and Standardize Casing**

```python
# Standardize data types and casing in historical_df
for col_name in historical_df.columns:
    if col_name not in {'from_date', 'to_date'}:
        historical_df = historical_df.withColumn(col_name, col(col_name).cast('string').lower())
```

### **Join Historical and Current Data**

```python
# Join historical and updated current data
joined_hist_df = historical_df.alias('hist').join(
    updated_current_df.alias('curr'),
    on=key_columns,
    how='left_outer'
)
```

### **Build the Change Condition**

```python
# Build the change_condition with null-safe equality check
change_conditions = [
    col(f'hist.{c}').eqNullSafe(col(f'curr.{c}')) == False
    for c in changing_columns
]

# Combine all conditions using logical OR
change_condition = reduce(lambda x, y: x | y, change_conditions)
```

### **Identify Records to Update**

```python
# Records where changes are detected
records_to_update_hist = joined_hist_df.filter(change_condition & col('hist.to_date').isNull())

# Update to_date in historical records
updated_to_date_df = records_to_update_hist.select(
    *[col('hist.' + c).alias(c) for c in historical_df.columns],
    col('curr.processing_date').alias('to_date')
)
```

### **Identify Non-Updated Historical Records**

```python
# Records where no changes are detected or no matching current data
non_updated_hist_df = historical_df.alias('hist').join(
    updated_to_date_df.select('hist.id').alias('upd'),
    on='id',
    how='left_anti'
)
```

### **Add New Records from Updated Current Data**

```python
# Records from current data not in historical data
new_hist_records = updated_current_df.alias('curr').join(
    historical_df.alias('hist'),
    on=key_columns,
    how='left_anti'
).select(
    *[col('curr.' + c).alias(c) for c in historical_df.columns if c not in {'from_date', 'to_date'}],
    col('curr.processing_date').alias('from_date'),
    lit(None).cast('date').alias('to_date')
)
```

### **Combine Updated and New Historical Records**

```python
# Combine historical records
historical_df_combined = non_updated_hist_df.unionByName(updated_to_date_df).unionByName(new_hist_records)
```

---

## Step 8: Adjust Overlapping Date Ranges

### **Define Window Specification**

```python
# Define window specification
partition_columns = key_columns + changing_columns
window_spec = Window.partitionBy(*partition_columns).orderBy(col('from_date').asc())
```

### **Apply Window Functions**

```python
# Add next_from_date
historical_df_combined = historical_df_combined.withColumn(
    'next_from_date',
    lead(col('from_date')).over(window_spec)
)
```

### **Adjust `to_date`**

```python
# Adjust to_date to be one day before next_from_date
historical_df_final = historical_df_combined.withColumn(
    'adjusted_to_date',
    when(
        col('to_date').isNull() & col('next_from_date').isNotNull(),
        date_sub(col('next_from_date'), 1)
    ).otherwise(col('to_date'))
)
```

---

## Step 9: Finalize DataFrame

### **Drop Temporary Columns and Rename Adjusted Columns**

```python
# Drop temporary columns and rename adjusted_to_date
historical_df_final = historical_df_final.drop('to_date', 'next_from_date')
historical_df_final = historical_df_final.withColumnRenamed('adjusted_to_date', 'to_date')
```

### **Remove Duplicates and Sort the DataFrame**

```python
# Remove duplicates if any
historical_df_final = historical_df_final.dropDuplicates()

# Sort the DataFrame for clarity
historical_df_final = historical_df_final.orderBy(
    key_columns + changing_columns + ['from_date'],
    ascending=[True] * (len(key_columns) + len(changing_columns) + 1)
)
```

---

## Step 10: Show Final `historical_df`

```python
historical_df_final.show(truncate=False)
```

**Expected Output:**

```
+---+----+----+-----+----------+----------+----------+
|id |col1|col2|col3 |processed |from_date |to_date   |
+---+----+----+-----+----------+----------+----------+
|1  |a   |x   |alpha|crimson   |2024-10-24|null      |
|1  |a   |x   |alpha|red       |2024-01-01|2024-10-23|
|2  |b   |y   |beta |blue      |2024-01-01|null      |
|5  |e   |v   |epsilon|purple  |2024-10-24|null      |
+---+----+----+-----+----------+----------+----------+
```
