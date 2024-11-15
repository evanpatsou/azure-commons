```python
# Import required libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, when, lit, to_date, date_format, concat_ws, sha2, coalesce,
    row_number, max as spark_max
)
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType
import pyspark.sql.functions as F
from pyspark.sql.window import Window
from functools import reduce
```

**Explanation:**

- **Importing Required Libraries:** We import all necessary modules and functions from `pyspark.sql` and other packages that will be used throughout the notebook.

```python
# Initialize Spark Session
spark = SparkSession.builder.appName("OptimizedSCDType2Update").getOrCreate()
```

**Explanation:**

- **Initializing Spark Session:** We create a Spark session, which is the entry point for working with DataFrames in PySpark.

**Assertion:**

```python
# Assert that SparkSession is active
assert spark is not None, "SparkSession was not created successfully."
```

**Explanation:**

- **Assertion:** We assert that the Spark session has been created successfully to proceed with data processing.

---

<a id='dataframes'></a>
## **2. Creating Sample DataFrames**

We set up sample historical and current DataFrames to simulate the data we are working with.

### **Schema and Data for Historical DataFrame**

```python
# Schema for historical DataFrame
schema_historical = StructType([
    StructField("otherkey", IntegerType(), True),
    StructField("dataset1_key", IntegerType(), True),
    StructField("dataset3_key", IntegerType(), True),
    StructField("otherid", StringType(), True),
    StructField("from_date", DateType(), True),
    StructField("to_date", DateType(), True)
])

# Data for historical DataFrame
data_historical = [
    (1, 1, None, "otherid1", "2024-01-01", None),
    (2, 2, None, "otherid2", "2024-10-29", None),
    (3, 3, None, "otherid3", "2024-01-01", None),
    (4, 4, 6, "otherid4", "2024-01-01", None),
    (5, 4, None, "otherid4", "2023-01-01", "2024-01-01"),
    (6, None, None, None, "2023-01-01", None)
]

# Create historical DataFrame
historical_df = spark.createDataFrame(data_historical, schema_historical) \
    .withColumn("from_date", to_date(col("from_date"), "yyyy-MM-dd")) \
    .withColumn("to_date", to_date(col("to_date"), "yyyy-MM-dd"))
```

**Explanation:**

- **Defining Schema and Data:** We define the schema and data for the historical DataFrame, which represents the existing records.
- **Creating DataFrame:** We create the `historical_df` DataFrame using the defined schema and data.
- **Date Conversion:** We convert the `from_date` and `to_date` columns from strings to `DateType` for accurate date comparisons.

**Assertion:**

```python
# Assert that historical_df has been created with expected number of rows
assert historical_df.count() == 6, f"historical_df should have 6 rows but has {historical_df.count()}."

# Assert that historical_df has the correct schema
expected_columns = ["otherkey", "dataset1_key", "dataset3_key", "otherid", "from_date", "to_date"]
assert historical_df.columns == expected_columns, f"historical_df columns are incorrect: {historical_df.columns}"
```

**Explanation:**

- **Assertions:** We verify that `historical_df` has been created correctly with the expected number of rows and columns.

---

### **Schema and Data for Current DataFrame**

```python
# Schema for current DataFrame
schema_current = StructType([
    StructField("dataset1_key", IntegerType(), True),
    StructField("dataset3_key", IntegerType(), True),
    StructField("otherid", StringType(), True),
    StructField("from_date", DateType(), True)
])

# Data for current DataFrame
data_current = [
    (1, None, "otherid1", "2024-01-01"),
    (2, 5, "otherid2", "2024-10-30"),
    (3, None, "otherid3", "2024-01-01"),
    (4, 6, "otherid4", "2024-10-30"),
    (7, None, None, "2024-10-30"),
    (8, None, None, "2024-10-30")
]

# Create current DataFrame
current_df = spark.createDataFrame(data_current, schema_current) \
    .withColumn("from_date", to_date(col("from_date"), "yyyy-MM-dd"))
```

**Explanation:**

- **Defining Schema and Data:** We define the schema and data for the current DataFrame, representing the new records to compare with the historical data.
- **Creating DataFrame:** We create the `current_df` DataFrame.
- **Date Conversion:** We convert the `from_date` column to `DateType`.

**Assertion:**

```python
# Assert that current_df has been created with expected number of rows
assert current_df.count() == 6, f"current_df should have 6 rows but has {current_df.count()}."

# Assert that current_df has the correct schema
expected_columns_current = ["dataset1_key", "dataset3_key", "otherid", "from_date"]
assert current_df.columns == expected_columns_current, f"current_df columns are incorrect: {current_df.columns}"
```

**Explanation:**

- **Assertions:** We verify that `current_df` has been created correctly.

---

<a id='update-function'></a>
## **3. Defining the Update Function**

We perform the SCD Type 2 update by comparing the historical and current DataFrames.

<a id='current-date'></a>
### **Setting the Current Date**

```python
# Set the current date
current_date_str = '2024-10-30'
current_date = F.lit(current_date_str).cast(DateType())
```

**Explanation:**

- **Setting Current Date:** We define the current date to simulate the date of the update, which will be used to set the `to_date` for expired records.

**Assertion:**

```python
# Since current_date is a Column expression, we can test its string representation
assert str(current_date) == "CAST(2024-10-30 AS DATE)", f"Current date column is incorrect: {str(current_date)}"
```

**Explanation:**

- **Assertion:** We verify that `current_date` is correctly defined as a date column.

---

<a id='common-columns'></a>
### **Identifying Common Columns**

```python
# Identify non-date columns
historical_fields = historical_df.schema.fields
current_fields = current_df.schema.fields

historical_non_date_cols = [f.name for f in historical_fields if not isinstance(f.dataType, DateType)]
current_non_date_cols = [f.name for f in current_fields if not isinstance(f.dataType, DateType)]

# Common columns excluding date columns
common_cols = list(set(historical_non_date_cols).intersection(set(current_non_date_cols)))
```

**Explanation:**

- **Dynamic Column Identification:** We dynamically identify the common non-date columns between the historical and current DataFrames.
- **Excluding Date Columns:** We exclude date columns from the common columns list, as they are handled separately.

**Assertion:**

```python
# Assert that common_cols are identified correctly
expected_common_cols = ['dataset1_key', 'dataset3_key', 'otherid']
assert set(common_cols) == set(expected_common_cols), f"common_cols are incorrect: {common_cols}"
```

**Explanation:**

- **Assertion:** We verify that the common columns are correctly identified.

---

<a id='composite-keys'></a>
### **Creating Composite Keys**

Exclude the `from_date` from the composite key.

```python
# Function to handle null values in composite key
def null_placeholder(column):
    return when(col(column).isNull(), lit('__NULL__')).otherwise(col(column).cast('string'))

# Create composite key in historical DataFrame (exclude 'from_date')
historical_df = historical_df.withColumn(
    'composite_key',
    concat_ws('_', *[null_placeholder(c) for c in common_cols])
)

# Create composite key in current DataFrame
current_df = current_df.withColumn(
    'composite_key',
    concat_ws('_', *[null_placeholder(c) for c in common_cols])
)
```

**Explanation:**

- **Null Handling in Composite Key:** We replace null values with a unique placeholder to prevent incorrect matches.
- **Creating Composite Keys:** By excluding `from_date` from the composite key, we ensure that records are matched based on their natural keys, allowing us to track changes over time.

**Assertion:**

```python
# Assert that composite_key column has been added to historical_df and current_df
assert 'composite_key' in historical_df.columns, "composite_key not found in historical_df."
assert 'composite_key' in current_df.columns, "composite_key not found in current_df."

# Optionally, display some composite keys to verify
print("Composite keys in historical_df:")
historical_df.select('composite_key').show(5)
print("Composite keys in current_df:")
current_df.select('composite_key').show(5)
```

**Explanation:**

- **Assertions:** We ensure that the `composite_key` column exists in both DataFrames.
- **Verification:** We can display some composite keys to verify correctness.

---

<a id='hash-values'></a>
### **Computing Hash Values of Attributes**

```python
# Attribute columns to include in hash calculation
# Exclude 'otherkey', 'from_date', 'to_date', and 'composite_key'
attribute_cols = [c for c in historical_df.columns if c not in ('otherkey', 'from_date', 'to_date', 'composite_key')]

# Compute hash_value in historical DataFrame
historical_df = historical_df.withColumn(
    'hash_value',
    sha2(concat_ws('||', *[coalesce(col(c).cast('string'), lit('')) for c in attribute_cols]), 256)
)

# Compute hash_value in current DataFrame
current_df = current_df.withColumn(
    'hash_value',
    sha2(concat_ws('||', *[coalesce(col(c).cast('string'), lit('')) for c in attribute_cols]), 256)
)
```

**Explanation:**

- **Hashing Attribute Columns:** We compute a hash value based on attribute columns to efficiently detect changes between records.
- **Excluding Keys and Dates:** Keys and date columns are excluded from the hash calculation.

**Assertion:**

```python
# Assert that hash_value column has been added to historical_df and current_df
assert 'hash_value' in historical_df.columns, "hash_value not found in historical_df."
assert 'hash_value' in current_df.columns, "hash_value not found in current_df."

# Optionally, display some hash values to verify
print("Hash values in historical_df:")
historical_df.select('hash_value').show(5)
print("Hash values in current_df:")
current_df.select('hash_value').show(5)
```

**Explanation:**

- **Assertions:** We verify that the `hash_value` column exists in both DataFrames.
- **Verification:** We can display hash values to ensure they are computed.

---

<a id='full-outer-join'></a>
### **Performing Full Outer Join**

```python
# Perform full outer join on composite_key
joined_df = historical_df.alias('hist').join(
    current_df.alias('curr'),
    on='composite_key',
    how='full_outer'
)
```

**Explanation:**

- **Joining DataFrames:** We perform a full outer join on `composite_key` to combine records, capturing all possible matches.

**Assertion:**

```python
# Assert that joined_df has been created
assert joined_df is not None, "joined_df was not created successfully."

# Assert that joined_df has the expected number of rows
expected_min_rows = max(historical_df.select('composite_key').distinct().count(), current_df.select('composite_key').distinct().count())
actual_rows = joined_df.select('composite_key').distinct().count()
assert actual_rows >= expected_min_rows, f"joined_df has fewer rows than expected: {actual_rows} < {expected_min_rows}"
```

**Explanation:**

- **Assertions:** We verify that `joined_df` exists and contains at least as many unique composite keys as the larger DataFrame.

---

<a id='source-identification'></a>
### **Identifying Source of Records**

Exclude `from_date` from the null-check in source identification.

```python
# Function to check if all common columns are not null in a DataFrame
def all_common_cols_not_null(prefix):
    return reduce(lambda a, b: a & b, [col(f"{prefix}.{c}").isNotNull() for c in common_cols])

# Identify source of records
joined_df = joined_df.withColumn(
    'source',
    when(all_common_cols_not_null('hist') & all_common_cols_not_null('curr'), lit('both'))
    .when(all_common_cols_not_null('hist'), lit('hist_only'))
    .when(all_common_cols_not_null('curr'), lit('curr_only'))
    .otherwise(lit('unknown'))
)
```

**Explanation:**

- **Determining Record Source:** We classify records based on their presence in the historical and current DataFrames.
- **Dynamic Column Handling:** We use common columns dynamically without hardcoding column names.
- **Excluding `from_date`:** We exclude `from_date` from the null-check to prevent misclassification of records.

**Assertion:**

```python
# Assert that 'source' column has been added
assert 'source' in joined_df.columns, "'source' column not found in joined_df."

# Optionally, show count of each source type
source_counts = joined_df.groupBy('source').count().collect()
print("Source counts:")
for row in source_counts:
    print(f"{row['source']}: {row['count']}")
```

**Explanation:**

- **Assertion:** We verify that the `source` column exists.
- **Verification:** We can display counts of each source type.

---

<a id='processing-records'></a>
### **Processing Records**

#### **Unchanged Records**

```python
# Unchanged records
unchanged_hist = joined_df.where(
    (col('source') == 'both') &
    (col('hist.hash_value') == col('curr.hash_value'))
).select(
    [col('hist.' + c) for c in historical_df.columns if c not in ('composite_key', 'hash_value')]
)
```

**Explanation:**

- **Selecting Unchanged Records:** Records present in both DataFrames with identical hash values are considered unchanged.

**Assertion:**

```python
# Assert that unchanged_hist is created
assert unchanged_hist is not None, "unchanged_hist DataFrame was not created."

# Optionally, count the number of unchanged records
unchanged_count = unchanged_hist.count()
print(f"Number of unchanged records: {unchanged_count}")
```

**Explanation:**

- **Assertion:** We ensure that `unchanged_hist` is created.
- **Verification:** We can count the number of unchanged records.

---

#### **Updated Records**

```python
# Updated records - expire old records
updated_hist = joined_df.where(
    (col('source') == 'both') &
    (col('hist.hash_value') != col('curr.hash_value'))
).withColumn(
    'to_date',
    current_date
).select(
    [col('hist.' + c) if c not in ('to_date', 'composite_key', 'hash_value') else col(c) for c in historical_df.columns]
)
```

**Explanation:**

- **Expiring Old Records:** We mark old records as expired by setting their `to_date` to the current date.

**Assertion:**

```python
# Assert that updated_hist is created
assert updated_hist is not None, "updated_hist DataFrame was not created."

# Optionally, count the number of updated records
updated_count = updated_hist.count()
print(f"Number of updated records: {updated_count}")
```

**Explanation:**

- **Assertion:** We ensure that `updated_hist` is created.
- **Verification:** We can count the number of updated records.

---

#### **Generating Unique `otherkey` Values**

```python
# Get the maximum otherkey value from historical_df
max_otherkey_row = historical_df.select(spark_max('otherkey')).collect()[0]
max_otherkey = max_otherkey_row[0] if max_otherkey_row[0] is not None else 0
```

**Explanation:**

- **Determining Maximum `otherkey`:** We find the maximum `otherkey` in `historical_df` to offset new IDs.

**Assertion:**

```python
# Assert that max_otherkey is retrieved
assert isinstance(max_otherkey, int), f"max_otherkey should be an integer but is {type(max_otherkey)}"
```

**Explanation:**

- **Assertion:** We verify that `max_otherkey` is correctly retrieved and is an integer.

---

#### **Defining the Window for `row_number()`**

```python
# Define a window for row_number()
window = Window.orderBy(F.lit(1))
```

**Explanation:**

- **Window Specification:** We use `F.lit(1)` in the orderBy to create a global window for assigning unique IDs.

---

#### **Function to Generate Unique `otherkey` Values**

```python
# Function to generate unique otherkey values with offset
def generate_unique_otherkey(df, offset):
    return df.withColumn('otherkey', row_number().over(window) + offset)
```

**Explanation:**

- **Generating Unique IDs:** We define a function that assigns unique `otherkey` values by offsetting `row_number()` with `max_otherkey`.

---

#### **New Records from Current Only**

```python
# Process new records from curr_only
new_records_from_current = joined_df.where(
    col('source') == 'curr_only'
).select(
    *[col('curr.' + c) for c in current_df.columns if c not in ('composite_key', 'hash_value')],
    lit(None).cast(DateType()).alias('to_date')
)

new_records_from_current = generate_unique_otherkey(new_records_from_current, max_otherkey)
```

**Explanation:**

- **Adding New Records:** We add records that are only present in the current DataFrame, indicating new entries.
- **Assigning Unique IDs:** We assign unique `otherkey` values to these new records.

**Updating `max_otherkey`:**

```python
# Update max_otherkey
max_otherkey += new_records_from_current.count()
```

**Explanation:**

- **Updating Offset:** We update `max_otherkey` to ensure subsequent IDs are unique.

**Assertion:**

```python
# Assert that new_records_from_current is created
assert new_records_from_current is not None, "new_records_from_current DataFrame was not created."

# Optionally, count the number of new records from curr_only
new_current_count = new_records_from_current.count()
print(f"Number of new records from curr_only: {new_current_count}")
```

**Explanation:**

- **Assertion:** We ensure that `new_records_from_current` is created.
- **Verification:** We can count the new records added.

---

#### **New Records from Updated Records**

```python
# Process new records from updates
new_records_from_updated = joined_df.where(
    (col('source') == 'both') &
    (col('hist.hash_value') != col('curr.hash_value'))
).select(
    *[col('curr.' + c) for c in current_df.columns if c not in ('composite_key', 'hash_value')],
    lit(None).cast(DateType()).alias('to_date')
)

# Set 'from_date' to current_date for new records from updates
new_records_from_updated = new_records_from_updated.withColumn('from_date', current_date)

new_records_from_updated = generate_unique_otherkey(new_records_from_updated, max_otherkey)
```

**Explanation:**

- **Adding Updated Records:** We add new records representing the updated state after changes.
- **Setting `from_date`:** We set the `from_date` to the current date to reflect the change effective date.
- **Assigning Unique IDs:** We assign unique `otherkey` values to these new records.

**Updating `max_otherkey`:**

```python
# Update max_otherkey
max_otherkey += new_records_from_updated.count()
```

**Assertion:**

```python
# Assert that new_records_from_updated is created
assert new_records_from_updated is not None, "new_records_from_updated DataFrame was not created."

# Optionally, count the number of new records from updates
new_updated_count = new_records_from_updated.count()
print(f"Number of new records from updates: {new_updated_count}")
```

**Explanation:**

- **Assertion:** We ensure that `new_records_from_updated` is created.
- **Verification:** We can count the updated records added.

---

#### **Combining All Records**

```python
# Union all records
historical_df_updated = unchanged_hist.union(updated_hist).union(new_records_from_current).union(new_records_from_updated)
```

**Explanation:**

- **Combining Records:** We merge all processed records into a single DataFrame representing the updated historical data.

**Assertion:**

```python
# Assert that historical_df_updated is created
assert historical_df_updated is not None, "historical_df_updated DataFrame was not created."

# Optionally, count the total number of records
total_records = historical_df_updated.count()
print(f"Total number of records in historical_df_updated: {total_records}")
```

**Explanation:**

- **Assertion:** We ensure that `historical_df_updated` is created.
- **Verification:** We can count the total number of records.

---

#### **Cleaning Up Auxiliary Columns**

```python
# Remove auxiliary columns if present
historical_df_updated = historical_df_updated.select(
    [c for c in historical_df_updated.columns if c not in ('composite_key', 'hash_value', 'source')]
)
```

**Explanation:**

- **Cleaning DataFrame:** We remove temporary columns used during processing to clean up the DataFrame.

**Assertion:**

```python
# Assert that auxiliary columns have been removed
assert 'composite_key' not in historical_df_updated.columns, "composite_key should have been removed."
assert 'hash_value' not in historical_df_updated.columns, "hash_value should have been removed."
assert 'source' not in historical_df_updated.columns, "source should have been removed."
```

**Explanation:**

- **Assertion:** We verify that the auxiliary columns have been removed.

---

<a id='post-processing'></a>
## **4. Post-Processing and Display**

### **Sorting the DataFrame**

```python
# Sorting
historical_df_sorted = historical_df_updated.withColumn(
    "otherkey_sort",
    when(col("otherkey").isNull(), 99999).otherwise(col("otherkey"))
).orderBy(
    "otherkey_sort",
    *common_cols,
    "from_date"
).drop("otherkey_sort")
```

**Explanation:**

- **Sorting Records:** We sort the DataFrame for better readability.
- **Handling Nulls in Sorting:** We replace null `otherkey` values to ensure they are sorted at the end.

**Assertion:**

```python
# Assert that historical_df_sorted is created
assert historical_df_sorted is not None, "historical_df_sorted DataFrame was not created."

# Optionally, check the number of records remains the same
assert historical_df_sorted.count() == total_records, "Record count changed after sorting."
```

**Explanation:**

- **Assertion:** We ensure that the sorted DataFrame is created and the record count remains the same.

---

### **Formatting Dates**

```python
# Convert dates to desired string format
final_display_df = historical_df_sorted \
    .withColumn("from_date", date_format(col("from_date"), "dd/MM/yyyy")) \
    .withColumn("to_date", date_format(col("to_date"), "dd/MM/yyyy")) \
    .na.fill({"to_date": ""})
```

**Explanation:**

- **Formatting Dates:** We format date columns for display purposes.
- **Handling Null Dates:** We replace null `to_date` values with empty strings.

**Assertion:**

```python
# Assert that date columns are formatted correctly
sample_row = final_display_df.select('from_date', 'to_date').limit(1).collect()[0]
from_date = sample_row['from_date']
to_date = sample_row['to_date']
import re
date_pattern = re.compile(r'\d{2}/\d{2}/\d{4}')
assert date_pattern.match(from_date), f"'from_date' date is not formatted correctly: {from_date}"
if to_date:
    assert date_pattern.match(to_date), f"'to_date' date is not formatted correctly: {to_date}"
```

**Explanation:**

- **Assertion:** We verify that the date columns are formatted correctly.

---

### **Displaying the Updated DataFrame**

```python
# Show the updated DataFrame
final_display_df.show(truncate=False)
```

**Explanation:**

- **Displaying Results:** We display the final DataFrame to review the results.

---

<a id='conclusion'></a>
## **5. Conclusion**

In this notebook, we addressed the errors in the previous code by:

- **Excluding `from_date` from the Composite Key:** Ensuring records are matched based on natural keys.
- **Correcting Source Identification Logic:** Using only natural keys to determine the source of records.
- **Including All Relevant Attributes in Hash Calculation:** Ensuring changes are detected accurately.
- **Generating Unique `otherkey` Values Correctly:** Maintaining data integrity by assigning unique identifiers.
- **Correcting Use of `row_number()` with Window Specification:** Using a proper window to generate sequential IDs.
- **Setting `from_date` Correctly in New Records:** Reflecting the effective date of changes accurately.
- **Including Assertions and Explanations:** Ensuring the code works correctly and is understandable.

By making these adjustments and providing explanations after each cell, we have made the code more robust, accurate, and readable. This ensures that records are correctly matched and that changes are detected even when there are multiple records with the same keys but different effective dates.

---

**Note:** Ensure that the Spark session is stopped after the notebook execution to release resources.

```python
# Stop the Spark session
spark.stop()
```