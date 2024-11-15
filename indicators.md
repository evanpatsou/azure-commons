```python
# Import required libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, when, lit, to_date, date_format, concat_ws, sha2, coalesce,
    row_number
)
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType
import pyspark.sql.functions as F
from pyspark.sql.window import Window

# Initialize Spark Session
spark = SparkSession.builder.appName("OptimizedSCDType2Update").getOrCreate()
```

**Assertion:**

```python
# Assert that SparkSession is active
assert spark is not None, "SparkSession was not created successfully."
```

**Decision Explanation:**

- **Importing Necessary Functions:** Import all functions and modules that will be used throughout the notebook to ensure smooth execution.
- **Spark Session Initialization:** Create a Spark session to work with PySpark DataFrames.
- **Assertion:** We assert that the `spark` object is not `None` to ensure that the Spark session was initialized successfully.

---

<a id='dataframes'></a>
## **2. Creating Sample DataFrames**

We set up sample historical and final DataFrames to simulate the data we are working with.

### **Schema and Data for Historical DataFrame**

```python
# Schema for historical DataFrame
schema_historical = StructType([
    StructField("otherkey", IntegerType(), True),
    StructField("dataset1_key", IntegerType(), True),
    StructField("dataset3_key", IntegerType(), True),
    StructField("otherid", StringType(), True),
    StructField("from", DateType(), True),
    StructField("to", DateType(), True)
])

# Data for historical DataFrame
data_historical = [
    (1, 1, None, "otherid1", "2024-01-01", None),
    (2, 2, None, "otherid2", "2024-10-29", None),
    (3, 3, None, "otherid3", "2024-01-01", None),
    (4, 4, 6, "otherid4", "2024-01-01", None),
    (4, 4, None, "otherid4", "2023-01-01", "2024-01-01"),
    (10, None, None, None, "2023-01-01", None)
]

# Create historical DataFrame
df_historical = spark.createDataFrame(data_historical, schema_historical) \
    .withColumn("from", to_date(col("from"), "yyyy-MM-dd")) \
    .withColumn("to", to_date(col("to"), "yyyy-MM-dd"))
```

**Assertion:**

```python
# Assert that df_historical has been created with expected number of rows
assert df_historical.count() == 6, f"df_historical should have 6 rows but has {df_historical.count()}."

# Assert that df_historical has the correct schema
expected_columns = ["otherkey", "dataset1_key", "dataset3_key", "otherid", "from", "to"]
assert df_historical.columns == expected_columns, f"df_historical columns are incorrect: {df_historical.columns}"
```

**Decision Explanation:**

- **Data Representation:** Use explicit schemas and sample data to ensure that the DataFrames are created correctly with appropriate data types.
- **Date Conversion:** Convert date strings to `DateType` for accurate date comparisons and operations.
- **Assertions:**
  - Ensure that `df_historical` has 6 rows as expected.
  - Check that the columns in `df_historical` match the expected schema.

---

### **Schema and Data for Final DataFrame**

```python
# Schema for final DataFrame
schema_final = StructType([
    StructField("dataset1_key", IntegerType(), True),
    StructField("dataset3_key", IntegerType(), True),
    StructField("otherid", StringType(), True),
    StructField("date", DateType(), True)
])

# Data for final DataFrame
data_final = [
    (1, None, "otherid1", "2024-01-01"),
    (2, 5, "otherid2", "2024-10-30"),
    (3, None, "otherid3", "2024-01-01"),
    (4, 6, "otherid4", "2024-10-30"),
    (7, None, None, "2024-10-30"),
    (8, None, None, "2024-10-30")
]

# Create final DataFrame
df_final = spark.createDataFrame(data_final, schema_final) \
    .withColumn("date", to_date(col("date"), "yyyy-MM-dd"))
```

**Assertion:**

```python
# Assert that df_final has been created with expected number of rows
assert df_final.count() == 6, f"df_final should have 6 rows but has {df_final.count()}."

# Assert that df_final has the correct schema
expected_columns_final = ["dataset1_key", "dataset3_key", "otherid", "date"]
assert df_final.columns == expected_columns_final, f"df_final columns are incorrect: {df_final.columns}"
```

**Decision Explanation:**

- **Data Representation:** Use explicit schemas and sample data to ensure that the DataFrames are created correctly with appropriate data types.
- **Date Conversion:** Convert date strings to `DateType` for accurate date comparisons and operations.
- **Assertions:**
  - Ensure that `df_final` has 6 rows as expected.
  - Check that the columns in `df_final` match the expected schema.

---

<a id='update-function'></a>
## **3. Defining the Update Function**

We perform the SCD Type 2 update by comparing the historical and final DataFrames.

<a id='current-date'></a>
### **Setting the Current Date**

```python
# Set the current date
current_date_str = '2024-10-30'
current_date = F.lit(current_date_str).cast(DateType())
```

**Assertion:**

```python
# Since current_date is a Column expression, we can test its string representation
assert str(current_date) == "CAST(2024-10-30 AS DATE)", f"Current date column is incorrect: {str(current_date)}"
```

**Decision Explanation:**

- **Current Date Handling:** Define a current date to simulate the date of the update. This is used for setting the 'from' and 'to' dates in the historical records.
- **Assertion:** Since `current_date` is a Column, we check its string representation.

---

<a id='common-columns'></a>
### **Identifying Common Columns**

```python
# Identify non-date columns
historical_fields = df_historical.schema.fields
final_fields = df_final.schema.fields

historical_non_date_cols = [f.name for f in historical_fields if not isinstance(f.dataType, DateType)]
final_non_date_cols = [f.name for f in final_fields if not isinstance(f.dataType, DateType)]

# Common columns excluding date columns
common_cols = list(set(historical_non_date_cols).intersection(set(final_non_date_cols)))
```

**Assertion:**

```python
# Assert that common_cols are identified correctly
expected_common_cols = ['dataset1_key', 'dataset3_key', 'otherid']
assert set(common_cols) == set(expected_common_cols), f"common_cols are incorrect: {common_cols}"
```

**Decision Explanation:**

- **Dynamic Column Identification:** Dynamically identify common columns between the two DataFrames to make the code adaptable to changes in the schema.
- **Excluding Date Columns:** Date columns are handled separately, so we focus on key columns for comparison.
- **Assertion:** Verify that `common_cols` are correctly identified.

---

<a id='composite-keys'></a>
### **Creating Composite Keys**

```python
# Function to handle null values in composite key
def null_placeholder(column):
    return when(col(column).isNull(), lit('__NULL__')).otherwise(col(column).cast('string'))

# Create composite key in historical DataFrame
df_historical = df_historical.withColumn(
    'composite_key',
    concat_ws('_', *[null_placeholder(c) for c in common_cols])
)

# Create composite key in final DataFrame
df_final = df_final.withColumn(
    'composite_key',
    concat_ws('_', *[null_placeholder(c) for c in common_cols])
)
```

**Assertion:**

```python
# Assert that composite_key column has been added to df_historical and df_final
assert 'composite_key' in df_historical.columns, "composite_key not found in df_historical."
assert 'composite_key' in df_final.columns, "composite_key not found in df_final."

# Optionally, display some composite keys to verify
print("Composite keys in df_historical:")
df_historical.select('composite_key').show(5)
print("Composite keys in df_final:")
df_final.select('composite_key').show(5)
```

**Decision Explanation:**

- **Handling Nulls in Composite Keys:** Use a unique placeholder `'__NULL__'` for null values to prevent collisions with actual data and ensure accurate joins.
- **Composite Key Creation:** By creating a composite key, we simplify the join condition and improve performance by avoiding complex multi-column joins.
- **Assertion:** Ensure that the `composite_key` column is added to both DataFrames.

---

<a id='hash-values'></a>
### **Computing Hash Values of Attributes**

```python
# Attribute columns to include in hash calculation
attribute_cols = [c for c in df_historical.columns if c not in ('otherkey', 'from', 'to', 'composite_key')]

# Compute hash_value in historical DataFrame
df_historical = df_historical.withColumn(
    'hash_value',
    sha2(concat_ws('||', *[coalesce(col(c).cast('string'), lit(''))) for c in attribute_cols]), 256)
)

# Compute hash_value in final DataFrame
df_final = df_final.withColumn(
    'hash_value',
    sha2(concat_ws('||', *[coalesce(col(c).cast('string'), lit(''))) for c in attribute_cols]), 256)
)
```

**Assertion:**

```python
# Assert that hash_value column has been added to df_historical and df_final
assert 'hash_value' in df_historical.columns, "hash_value not found in df_historical."
assert 'hash_value' in df_final.columns, "hash_value not found in df_final."

# Optionally, display some hash values to verify
print("Hash values in df_historical:")
df_historical.select('hash_value').show(5)
print("Hash values in df_final:")
df_final.select('hash_value').show(5)
```

**Decision Explanation:**

- **Hashing Attributes:** By hashing the attribute columns, we can efficiently detect changes between records without comparing each column individually.
- **Excluding Keys and Dates:** Exclude key and date columns from the hash calculation as they are identifiers and validity periods, not attributes.
- **Assertion:** Ensure that the `hash_value` column is added to both DataFrames.

---

<a id='full-outer-join'></a>
### **Performing Full Outer Join**

```python
# Perform full outer join on composite_key
joined_df = df_historical.alias('hist').join(
    df_final.alias('final'),
    on='composite_key',
    how='full_outer'
)
```

**Assertion:**

```python
# Assert that joined_df has been created
assert joined_df is not None, "joined_df was not created successfully."

# Assert that joined_df has the expected number of rows
# Since it's a full outer join, the number of rows should be at least the maximum of the two DataFrames
expected_min_rows = max(df_historical.select('composite_key').distinct().count(), df_final.select('composite_key').distinct().count())
actual_rows = joined_df.select('composite_key').distinct().count()
assert actual_rows >= expected_min_rows, f"joined_df has fewer rows than expected: {actual_rows} < {expected_min_rows}"
```

**Decision Explanation:**

- **Full Outer Join:** Use a full outer join to capture all possible combinations of records from both DataFrames, ensuring no data is missed.
- **Assertion:** Verify that `joined_df` is created and has at least the expected number of rows based on distinct `composite_key` values.

---

<a id='source-identification'></a>
### **Identifying Source of Records**

```python
# Identify source of records
joined_df = joined_df.withColumn(
    'source',
    when(col('hist.otherkey').isNotNull() & col('final.dataset1_key').isNotNull(), lit('both'))
    .when(col('hist.otherkey').isNotNull(), lit('hist_only'))
    .when(col('final.dataset1_key').isNotNull(), lit('final_only'))
    .otherwise(lit('unknown'))
)
```

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

**Decision Explanation:**

- **Record Categorization:** Categorize records based on their presence in historical and/or final DataFrames to determine the appropriate action (e.g., update, insert).
- **Assertion:** Ensure that the `source` column is added and optionally display counts of each source type to verify.

---

<a id='processing-records'></a>
### **Processing Records**

#### **Unchanged Records**

```python
# Unchanged records
unchanged_hist = joined_df.where(
    (col('source') == 'both') &
    (col('hist.hash_value') == col('final.hash_value'))
).select(
    [col('hist.' + c) for c in df_historical.columns if c not in ('composite_key', 'hash_value')]
)
```

**Assertion:**

```python
# Assert that unchanged_hist is created
assert unchanged_hist is not None, "unchanged_hist DataFrame was not created."

# Optionally, count the number of unchanged records
unchanged_count = unchanged_hist.count()
print(f"Number of unchanged records: {unchanged_count}")
```

**Decision Explanation:**

- **Selecting Unchanged Records:** Records present in both DataFrames with identical attribute hashes are considered unchanged and are retained as is.
- **Assertion:** Ensure that `unchanged_hist` is created and optionally print the count of unchanged records.

---

#### **Updated Records**

```python
# Updated records - expire old records
updated_hist = joined_df.where(
    (col('source') == 'both') &
    (col('hist.hash_value') != col('final.hash_value'))
).withColumn(
    'to',
    current_date
).select(
    [col('hist.' + c) if c not in ('to', 'composite_key', 'hash_value') else col(c) for c in df_historical.columns]
)
```

**Assertion:**

```python
# Assert that updated_hist is created
assert updated_hist is not None, "updated_hist DataFrame was not created."

# Optionally, count the number of updated records
updated_count = updated_hist.count()
print(f"Number of updated records: {updated_count}")
```

**Decision Explanation:**

- **Expiring Old Records:** For records that have changed, set the 'to' date to the current date to mark them as expired.
- **Assertion:** Ensure that `updated_hist` is created and optionally print the count of updated records.

---

#### **Generating Unique IDs for New Records**

```python
# Generate unique IDs using row_number()
window = Window.orderBy(F.monotonically_increasing_id())
```

**Assertion:**

```python
# Since this is setup for window function, we proceed without assertion here
```

**Decision Explanation:**

- **Unique ID Generation:** Use `row_number()` over a window to generate unique identifiers for new records, ensuring data integrity and avoiding duplicates.
- **Assertion:** No assertion needed here as it's a window definition.

---

#### **New Records from Final Only**

```python
# New records from final_only
new_records_from_final = joined_df.where(
    col('source') == 'final_only'
).select(
    row_number().over(window).alias('otherkey'),
    *[col('final.' + c) for c in df_final.columns if c not in ('composite_key', 'date', 'hash_value')],
    col('final.date').alias('from'),
    lit(None).cast(DateType()).alias('to')
)
```

**Assertion:**

```python
# Assert that new_records_from_final is created
assert new_records_from_final is not None, "new_records_from_final DataFrame was not created."

# Optionally, count the number of new records from final only
new_final_count = new_records_from_final.count()
print(f"Number of new records from final_only: {new_final_count}")
```

**Decision Explanation:**

- **Adding New Records:** Records present only in the final DataFrame are new and are added with the 'from' date from the final DataFrame.
- **Assertion:** Ensure that `new_records_from_final` is created and optionally print the count.

---

#### **New Records from Updated Records**

```python
# New records from updated records
new_records_from_updated = joined_df.where(
    (col('source') == 'both') &
    (col('hist.hash_value') != col('final.hash_value'))
).select(
    row_number().over(window).alias('otherkey'),
    *[col('final.' + c) for c in df_final.columns if c not in ('composite_key', 'date', 'hash_value')],
    col('final.date').alias('from'),
    lit(None).cast(DateType()).alias('to')
)
```

**Assertion:**

```python
# Assert that new_records_from_updated is created
assert new_records_from_updated is not None, "new_records_from_updated DataFrame was not created."

# Optionally, count the number of new records from updates
new_updated_count = new_records_from_updated.count()
print(f"Number of new records from updates: {new_updated_count}")
```

**Decision Explanation:**

- **Adding Updated Records:** For changed records, add new records with updated attributes and the 'from' date from the final DataFrame, representing the new version.
- **Assertion:** Ensure that `new_records_from_updated` is created and optionally print the count.

---

#### **Combining All Records**

```python
# Union all records
df_historical_updated = unchanged_hist.union(updated_hist).union(new_records_from_final).union(new_records_from_updated)
```

**Assertion:**

```python
# Assert that df_historical_updated is created
assert df_historical_updated is not None, "df_historical_updated DataFrame was not created."

# Optionally, count the total number of records
total_records = df_historical_updated.count()
print(f"Total number of records in df_historical_updated: {total_records}")
```

**Decision Explanation:**

- **Consolidating Results:** Combine all processed records into a single updated historical DataFrame for consistency and completeness.
- **Assertion:** Ensure that `df_historical_updated` is created and optionally print the total number of records.

---

#### **Cleaning Up Auxiliary Columns**

```python
# Remove auxiliary columns if present
df_historical_updated = df_historical_updated.select(
    [c for c in df_historical_updated.columns if c not in ('composite_key', 'hash_value', 'source')]
)
```

**Assertion:**

```python
# Assert that auxiliary columns have been removed
assert 'composite_key' not in df_historical_updated.columns, "composite_key should have been removed."
assert 'hash_value' not in df_historical_updated.columns, "hash_value should have been removed."
assert 'source' not in df_historical_updated.columns, "source should have been removed."
```

**Decision Explanation:**

- **Cleaning DataFrame:** Remove auxiliary columns used during processing to keep the DataFrame clean and focused on relevant data.
- **Assertion:** Ensure that the auxiliary columns are removed from `df_historical_updated`.

---

<a id='post-processing'></a>
## **4. Post-Processing and Display**

We sort the DataFrame and format the date columns for display.

### **Sorting the DataFrame**

```python
# Sorting
df_historical_sorted = df_historical_updated.withColumn(
    "otherkey_sort",
    when(col("otherkey").isNull(), 99999).otherwise(col("otherkey"))
).orderBy(
    "otherkey_sort",
    *common_cols,
    "from"
).drop("otherkey_sort")
```

**Assertion:**

```python
# Assert that df_historical_sorted is created
assert df_historical_sorted is not None, "df_historical_sorted DataFrame was not created."

# Optionally, check the number of records remains the same
assert df_historical_sorted.count() == total_records, "Record count changed after sorting."
```

**Decision Explanation:**

- **Handling Nulls in Sorting:** Replace null 'otherkey' values with a large number to ensure they are sorted at the end.
- **Ordering the DataFrame:** Sort the DataFrame based on 'otherkey' and other relevant columns for better readability and easier verification.
- **Assertion:** Ensure that `df_historical_sorted` is created and the record count remains the same.

---

### **Formatting Dates**

```python
# Convert dates to desired string format
df_final_display = df_historical_sorted \
    .withColumn("from", date_format(col("from"), "dd/MM/yyyy")) \
    .withColumn("to", date_format(col("to"), "dd/MM/yyyy")) \
    .na.fill({"to": ""})
```

**Assertion:**

```python
# Assert that date columns are formatted correctly
sample_row = df_final_display.select('from', 'to').limit(1).collect()[0]
from_date = sample_row['from']
to_date = sample_row['to']
import re
date_pattern = re.compile(r'\d{2}/\d{2}/\d{4}')
assert date_pattern.match(from_date), f"'from' date is not formatted correctly: {from_date}"
if to_date:
    assert date_pattern.match(to_date), f"'to' date is not formatted correctly: {to_date}"
```

**Decision Explanation:**

- **Date Formatting:** Format the 'from' and 'to' dates to 'dd/MM/yyyy' for consistency and readability.
- **Handling Null 'to' Dates:** Replace null 'to' dates with empty strings to signify that the records are currently active.
- **Assertion:** Ensure that the date columns are formatted as 'dd/MM/yyyy'.

---

### **Displaying the Updated DataFrame**

```python
# Show the updated DataFrame
df_final_display.show(truncate=False)
```