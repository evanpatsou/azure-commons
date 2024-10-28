## Import Libraries

```python
# %%
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, when, lit, to_date, current_date, countDistinct, count, coalesce, lower, trim,
    date_sub, lead, lag, monotonically_increasing_id
)
from pyspark.sql.window import Window

# For assertions
import sys
```

---

## Initialize Spark Session

```python
# %%
# Initialize Spark session
spark = SparkSession.builder.appName("AddNewIdentifiersToOldRecords").getOrCreate()
```

```python
# %%
# Assertion: Check Spark session initialization
assert spark is not None, "Spark session should be initialized successfully."
print("Spark session initialized successfully.")
```

---

## Step 1: Read and Prepare Datasets

### Dataset 1 (`dataset_1`)

```python
# %%
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

### Dataset 3 (`dataset_3`)

```python
# %%
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

### Database 2 (`database_2`)

```python
# %%
# Sample data for database_2 (mapping textcode to industry)
database2_data = [
    ('tc1', 'Technology'),
    ('tc2', 'Finance'),
    ('tc3', 'Healthcare'),
    ('tc4', 'Retail'),
    ('tc5', 'Manufacturing'),
    ('tc6', 'Energy')
]

database2_columns = ['textcode', 'industry']
database_2 = spark.createDataFrame(database2_data, database2_columns)

# Display database_2
database_2.show(truncate=False)
```

---

## Data Cleaning: Standardize and Clean `textcode` Columns

```python
# %%
# Function to clean 'textcode' columns
def clean_textcode(df):
    return df.withColumn('textcode', trim(lower(col('textcode'))))

# Apply cleaning
dataset_1 = clean_textcode(dataset_1)
dataset_3 = clean_textcode(dataset_3)
database_2 = clean_textcode(database_2)

# Display cleaned datasets
print("Cleaned dataset_1:")
dataset_1.show(truncate=False)

print("Cleaned dataset_3:")
dataset_3.show(truncate=False)

print("Cleaned database_2:")
database_2.show(truncate=False)
```

```python
# %%
# Assertion: Check for NULL values in 'textcode' columns after cleaning
for df_name, df in [('dataset_1', dataset_1), ('dataset_3', dataset_3), ('database_2', database_2)]:
    null_count = df.filter(col('textcode').isNull()).count()
    assert null_count == 0, f"{df_name} contains NULL values in 'textcode' column after cleaning"
    print(f"{df_name} contains no NULL values in 'textcode' column after cleaning.")
```

---

## Step 2: Create Universe from `dataset_1` and `dataset_3`

### Generate Universe DataFrame

```python
# %%
# Create universe by joining dataset_1 and dataset_3 on 'textcode'
universe_df = dataset_1.alias('d1').join(
    dataset_3.alias('d3'),
    on='textcode',
    how='inner'
).select(
    col('d1.dataset1_id'),
    col('d3.dataset3_id'),
    col('d1.textcode'),
    col('d1.name').alias('name_dataset1'),
    col('d3.name').alias('name_dataset3')
)

# Display universe_df
universe_df.show(truncate=False)
```

```python
# %%
# Assertion: Ensure universe_df has correct mappings
# Each 'textcode' should have one 'dataset1_id' and one 'dataset3_id'
universe_counts = universe_df.groupBy('textcode').agg(
    countDistinct('dataset1_id').alias('dataset1_count'),
    countDistinct('dataset3_id').alias('dataset3_count')
).filter(
    (col('dataset1_count') != 1) | (col('dataset3_count') != 1)
)

assert universe_counts.count() == 0, "Each 'textcode' in universe_df should map to exactly one 'dataset1_id' and one 'dataset3_id'"
print("All 'textcode's in universe_df have one-to-one mappings.")
```

---

## Step 3: Enhance Universe with `database_2`

### Join Universe with `database_2`

```python
# %%
# Join universe_df with database_2 to map textcode to industry
universe_enhanced_df = universe_df.join(
    database_2,
    on='textcode',
    how='left'
).select(
    'dataset1_id',
    'dataset3_id',
    'textcode',
    'name_dataset1',
    'name_dataset3',
    'industry'
)

# Display universe_enhanced_df
universe_enhanced_df.show(truncate=False)
```

```python
# %%
# Assertion: Ensure all textcodes have an associated industry
null_industry = universe_enhanced_df.filter(col('industry').isNull()).count()
assert null_industry == 0, "All 'textcode's in universe_enhanced_df should have an associated 'industry'"
print("All 'textcode's in universe_enhanced_df have associated industries.")
```

---

## Step 4: Read Existing `historical_df` and `old_records_df`

### Historical DataFrame (`historical_df`)

```python
# %%
# Sample 'historical_df'
historical_data = [
    (1, 100, 'other_value1', 'name1_old', '2023-01-01', None),
    (2, 200, 'other_value2', 'name2_old', '2023-01-01', None),
    (3, 300, 'other_value3', 'name3_old', '2023-01-01', None),
    (4, 400, 'other_value4', 'name4_old', '2023-01-01', None),
    (5, 500, 'other_value5', 'name5_old', '2023-01-01', None),
    (6, 600, 'other_value6', 'name6_old', '2023-01-01', None)
]

historical_columns = ['dataset1_id', 'dataset3_id', 'otherid', 'name', 'from_date', 'to_date']
historical_df = spark.createDataFrame(historical_data, historical_columns)

# Convert date columns
historical_df = historical_df.withColumn('from_date', to_date('from_date')) \
                             .withColumn('to_date', to_date('to_date'))

# Display historical_df
historical_df.show(truncate=False)
```

### Old Records DataFrame (`old_records_df`)

```python
# %%
# Sample 'old_records_df' from the database
old_records_data = [
    (0, 'US', 'TECH'),
    (1, 'US', 'FIN'),
    (2, 'CA', 'HEALTH'),
    (3, 'UK', 'TECH'),
    (4, 'DE', 'FIN')
]

old_records_columns = ['unique_id', 'country_code', 'industry_code']
old_records_df = spark.createDataFrame(old_records_data, old_records_columns)

# Display old_records_df
old_records_df.show(truncate=False)
```

```python
# %%
# Assertion: Check for non-null values in 'unique_id', 'country_code', and 'industry_code'
null_count_old_records = old_records_df.filter(
    col('unique_id').isNull() | col('country_code').isNull() | col('industry_code').isNull()
).count()
assert null_count_old_records == 0, "old_records_df should not have null values in key columns"
print("old_records_df has no nulls in key columns.")
```

---

## Step 5: Map Universe Identifiers to Old Records

### Join `historical_df` with Enhanced Universe

```python
# %%
# Join historical_df with universe_enhanced_df on 'dataset1_id' and 'dataset3_id'
historical_universe_df = historical_df.alias('hist').join(
    universe_enhanced_df.alias('univ'),
    on=['dataset1_id', 'dataset3_id'],
    how='inner'
).select(
    col('hist.dataset1_id'),
    col('hist.dataset3_id'),
    col('hist.otherid'),
    col('hist.name').alias('name_hist'),
    col('hist.from_date'),
    col('hist.to_date'),
    col('univ.textcode'),
    col('univ.name_dataset1'),
    col('univ.name_dataset3'),
    col('univ.industry')
)

# Display historical_universe_df
historical_universe_df.show(truncate=False)
```

```python
# %%
# Assertion: Ensure all historical records are mapped to universe
unmapped_historical = historical_df.join(
    universe_enhanced_df,
    on=['dataset1_id', 'dataset3_id'],
    how='left_anti'
)

assert unmapped_historical.count() == 0, "All historical records should be present in universe_enhanced_df"
print("All historical records are successfully mapped to universe_enhanced_df.")
```

### Join `old_records_df` with Mapped Identifiers

```python
# %%
# Assuming 'unique_id' in old_records_df corresponds to records in historical_universe_df
# Add 'unique_id' to historical_universe_df by matching order or an existing mapping
# For demonstration, we'll assign 'unique_id' based on 'dataset1_id'

# Create a mapping between 'dataset1_id' and 'unique_id' from old_records_df
dataset1_to_unique = old_records_df.select('unique_id', 'unique_id').withColumnRenamed('unique_id', 'dataset1_id')

# Join historical_universe_df with old_records_df on 'dataset1_id' to get 'unique_id'
historical_universe_mapped_df = historical_universe_df.join(
    old_records_df,
    on='dataset1_id',
    how='left'
).select(
    'unique_id',
    'country_code',
    'industry_code',
    'dataset1_id',
    'dataset3_id',
    'textcode',
    'name_hist',
    'name_dataset1',
    'name_dataset3',
    'industry',
    'from_date',
    'to_date'
)

# Display historical_universe_mapped_df
historical_universe_mapped_df.show(truncate=False)
```

```python
# %%
# Assertion: Ensure all historical_universe_mapped_df records have 'unique_id'
missing_unique_id = historical_universe_mapped_df.filter(col('unique_id').isNull()).count()
assert missing_unique_id == 0, "All records in historical_universe_mapped_df should have a 'unique_id'"
print("All records in historical_universe_mapped_df have a 'unique_id'.")
```

---

## Step 6: Handle Updates, Insertions, and Deactivations

### Identify Records to Update

```python
# %%
# Define current processing date
processing_date = current_date()

# Identify records where identifiers have changed (placeholder logic)
# In this example, we'll assume that if 'industry' has changed, it's an update
# Since we have no actual changes, this will be illustrative

# For demonstration, let's assume 'unique_id' 1 has a changed industry
# Update 'industry_code' for 'unique_id' 1 to 'TECH'

# Create an updated record for 'unique_id' 1
updated_record = spark.createDataFrame([
    (1, 'US', 'TECH')  # Changed industry_code from 'FIN' to 'TECH'
], old_records_columns)

# Join to identify updates
records_to_update = old_records_df.alias('old').join(
    updated_record.alias('new'),
    on='unique_id',
    how='inner'
).filter(
    col('old.industry_code') != col('new.industry_code')
).select(
    'old.unique_id',
    'new.country_code',
    'new.industry_code'
)

print("Identifying records to update...")
records_to_update.show(truncate=False)
```

### Identify Records to Insert

```python
# %%
# Identify new records in universe_enhanced_df not present in old_records_df
new_unique_ids = historical_universe_mapped_df.select('unique_id').distinct()
existing_unique_ids = old_records_df.select('unique_id').distinct()

new_records = new_unique_ids.join(
    existing_unique_ids,
    on='unique_id',
    how='left_anti'
)

# Prepare new records with NULL in identifier columns
inserted_records = new_records.join(
    historical_universe_mapped_df,
    on='unique_id',
    how='left'
).select(
    'unique_id',
    lit(None).alias('country_code'),
    lit(None).alias('industry_code'),
    'dataset1_id',
    'dataset3_id',
    'textcode',
    'name_hist',
    'name_dataset1',
    'name_dataset3',
    'industry',
    'from_date',
    'to_date'
)

print("Identifying records to insert...")
inserted_records.show(truncate=False)
```

### Identify Records to Deactivate

```python
# %%
# Identify records in old_records_df that no longer exist in universe_enhanced_df
# Assuming that if a historical record is not present in the universe, it should be deactivated
deactivated_records = old_records_df.join(
    historical_universe_mapped_df,
    on='unique_id',
    how='left_anti'
).withColumn(
    'to_date', processing_date
)

print("Identifying records to deactivate...")
deactivated_records.show(truncate=False)
```

### Create Updated Records

```python
# %%
# Update 'industry_code' for records to update
updated_to_date_records = records_to_update.select(
    'unique_id',
    'country_code',
    'industry_code'
).withColumn(
    'to_date', processing_date
)

# Create new version records with updated 'industry_code' and 'from_date' as processing_date
new_version_records = records_to_update.select(
    'unique_id',
    'country_code',
    'industry_code'
).withColumn(
    'from_date', processing_date
).withColumn(
    'to_date', lit(None).cast('date')
)

print("Creating updated records...")
updated_to_date_records.show(truncate=False)
new_version_records.show(truncate=False)
```

### Create Inserted Records

```python
# %%
# New records have 'unique_id' as null and new identifiers as null
# Since 'unique_id's are not null, but identifiers are null
# Here, 'unique_id's are present, but identifier columns are null for new records

# Select and arrange columns to match old_records_df structure
inserted_records_final = inserted_records.select(
    'unique_id',
    'country_code',
    'industry_code'
)

print("Creating inserted records...")
inserted_records_final.show(truncate=False)
```

### Create Deactivated Records

```python
# %%
# Deactivated records have 'to_date' set to processing_date
deactivated_records_final = deactivated_records.select(
    'unique_id',
    'country_code',
    'industry_code',
    'dataset1_id',
    'dataset3_id',
    'textcode',
    'name_hist',
    'name_dataset1',
    'name_dataset3',
    'industry',
    'from_date',
    'to_date'
)

print("Creating deactivated records...")
deactivated_records_final.show(truncate=False)
```

---

## Step 7: Combine All Records into Final DataFrame

```python
# %%
# Combine all records into 'final_records_df'
final_records_df = old_records_df.alias('old').join(
    updated_to_date_records.alias('upd'),
    on='unique_id',
    how='left'
).join(
    new_version_records.alias('new_ver'),
    on='unique_id',
    how='left'
).join(
    inserted_records_final.alias('ins'),
    on='unique_id',
    how='left'
).join(
    deactivated_records_final.alias('deact'),
    on='unique_id',
    how='left'
).select(
    'unique_id',
    coalesce('upd.country_code', 'ins.country_code').alias('country_code'),
    coalesce('upd.industry_code', 'ins.industry_code').alias('industry_code'),
    'dataset1_id',
    'dataset3_id',
    'textcode',
    'name_hist',
    'name_dataset1',
    'name_dataset3',
    'industry',
    'from_date',
    'to_date'
)

# Union with new version and deactivated records
final_records_df = final_records_df.unionByName(
    new_version_records.select(
        'unique_id',
        'country_code',
        'industry_code',
        'dataset1_id',
        'dataset3_id',
        'textcode',
        'name_hist',
        'name_dataset1',
        'name_dataset3',
        'industry',
        'from_date',
        'to_date'
    )
).unionByName(
    inserted_records_final.select(
        'unique_id',
        'country_code',
        'industry_code',
        'dataset1_id',
        'dataset3_id',
        'textcode',
        'name_hist',
        'name_dataset1',
        'name_dataset3',
        'industry',
        'from_date',
        'to_date'
    )
).unionByName(
    deactivated_records_final.select(
        'unique_id',
        'country_code',
        'industry_code',
        'dataset1_id',
        'dataset3_id',
        'textcode',
        'name_hist',
        'name_dataset1',
        'name_dataset3',
        'industry',
        'from_date',
        'to_date'
    )
)

# Display final_records_df
print("Combined final_records_df:")
final_records_df.show(truncate=False)
```

```python
# %%
# Assertion: 'final_records_df' should have the combined number of records
expected_final_count = (
    old_records_df.count() +
    updated_to_date_records.count() +
    new_version_records.count() +
    inserted_records_final.count() +
    deactivated_records_final.count()
)
actual_final_count = final_records_df.count()
assert actual_final_count == expected_final_count, "final_records_df should have the correct number of records"
print(f"final_records_df contains the correct number of records: {actual_final_count}")
```

---

## Step 8: Adjust Overlapping Date Ranges

### Define Window Specification and Adjust `to_date`

```python
# %%
# Define window specification based on 'unique_id' and order by 'from_date'
window_spec = Window.partitionBy('unique_id').orderBy(col('from_date').asc())

# Adjust 'to_date' to be one day before the next 'from_date' within the same 'unique_id'
final_records_df = final_records_df.withColumn(
    'next_from_date',
    lead(col('from_date')).over(window_spec)
).withColumn(
    'to_date',
    when(
        col('to_date').isNull() & col('next_from_date').isNotNull(),
        date_sub(col('next_from_date'), 1)
    ).otherwise(col('to_date'))
).drop('next_from_date')

# Display final_records_df after date adjustment
print("final_records_df after adjusting to_date:")
final_records_df.show(truncate=False)
```

```python
# %%
# Assertion: No overlaps in date ranges after adjustment
# Define a new window for checking overlaps
window_spec_check = Window.partitionBy('unique_id').orderBy(col('from_date').asc())

# Add 'prev_to_date' to check overlaps
final_records_check_df = final_records_df.withColumn(
    'prev_to_date',
    lag(col('to_date')).over(window_spec_check)
)

# Check for overlaps
overlaps = final_records_check_df.filter(
    col('from_date') <= col('prev_to_date')
).count()

assert overlaps == 0, "final_records_df should have no overlapping date ranges"
print("No overlapping date ranges in final_records_df.")
```

---

## Step 9: Finalize and Validate the Updated DataFrame

### Remove Duplicates and Sort the DataFrame

```python
# %%
# Remove duplicates if any
final_records_df = final_records_df.dropDuplicates()

# Sort the DataFrame for clarity
final_records_df = final_records_df.orderBy(
    ['unique_id', 'from_date'],
    ascending=[True, True]
)

# Display final_records_df
print("Final finalized records:")
final_records_df.show(truncate=False)
```

### Validate Final Schema and Data

```python
# %%
# Assertion: Validate final_records_df schema
expected_schema = set(['unique_id', 'country_code', 'industry_code', 'dataset1_id', 'dataset3_id',
                      'textcode', 'name_hist', 'name_dataset1', 'name_dataset3', 'industry',
                      'from_date', 'to_date'])

actual_schema = set(final_records_df.columns)
assert actual_schema == expected_schema, "final_records_df schema does not match expected schema"

print("final_records_df schema is as expected.")
```

```python
# %%
# Assertion: Ensure new identifier columns are correctly handled
# Only new records have 'country_code' and 'industry_code' as NULL
new_records = final_records_df.filter(col('unique_id').isNull())
non_new_records = final_records_df.filter(col('unique_id').isNotNull())

# Check that new records have NULL in 'country_code' and 'industry_code'
new_records_null_identifiers = new_records.filter(
    col('country_code').isNotNull() | col('industry_code').isNotNull()
).count()
assert new_records_null_identifiers == 0, "Only new records should have NULL in 'country_code' and 'industry_code'"


from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, count, row_number
from pyspark.sql.window import Window

# Initialize Spark session
spark = SparkSession.builder.appName("DropDuplicatesKeepMostInfo").getOrCreate()

# Sample data with duplicates
data = [
    (1, "A", None, 10),
    (1, "A", "Info1", 20),
    (2, "B", "Info2", None),
    (2, "B", "Info3", 30),
    (3, "C", None, None)
]

columns = ["id", "category", "info", "value"]
df = spark.createDataFrame(data, columns)

# Display original DataFrame
print("Original DataFrame:")
df.show()

# Define duplicate criteria
duplicate_subset = ["id", "category"]

# Add a column that counts non-null values in each row
df_with_count = df.withColumn(
    "non_null_count",
    count("*").over(Window.partitionBy(*duplicate_subset))
)

# Assign row numbers based on non_null_count descending
window_spec = Window.partitionBy(*duplicate_subset).orderBy(col("non_null_count").desc())

df_ranked = df.withColumn(
    "row_num",
    row_number().over(window_spec)
)

# Filter to keep only the first row in each duplicate group
df_deduped = df_ranked.filter(col("row_num") == 1).drop("row_num")

# Display deduplicated DataFrame
print("Deduplicated DataFrame (Keeping Row with Most Non-Null Values):")
df_deduped.show()
