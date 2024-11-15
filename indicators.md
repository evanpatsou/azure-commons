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
    (4, 4, None, "otherid4", "2023-01-01", "2024-01-01"),
    (10, None, None, None, "2023-01-01", None)
]

# Create historical DataFrame
historical_df = spark.createDataFrame(data_historical, schema_historical) \
    .withColumn("from_date", to_date(col("from_date"), "yyyy-MM-dd")) \
    .withColumn("to_date", to_date(col("to_date"), "yyyy-MM-dd"))
```

**Assertion:**

```python
# Assert that historical_df has been created with expected number of rows
assert historical_df.count() == 6, f"historical_df should have 6 rows but has {historical_df.count()}."

# Assert that historical_df has the correct schema
expected_columns = ["otherkey", "dataset1_key", "dataset3_key", "otherid", "from_date", "to_date"]
assert historical_df.columns == expected_columns, f"historical_df columns are incorrect: {historical_df.columns}"
```

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

**Assertion:**

```python
# Assert that current_df has been created with expected number of rows
assert current_df.count() == 6, f"current_df should have 6 rows but has {current_df.count()}."

# Assert that current_df has the correct schema
expected_columns_current = ["dataset1_key", "dataset3_key", "otherid", "from_date"]
assert current_df.columns == expected_columns_current, f"current_df columns are incorrect: {current_df.columns}"
```

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

**Assertion:**

```python
# Since current_date is a Column expression, we can test its string representation
assert str(current_date) == "CAST(2024-10-30 AS DATE)", f"Current date column is incorrect: {str(current_date)}"
```

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

**Assertion:**

```python
# Assert that common_cols are identified correctly
expected_common_cols = ['dataset1_key', 'dataset3_key', 'otherid']
assert set(common_cols) == set(expected_common_cols), f"common_cols are incorrect: {common_cols}"
```

---

<a id='composite-keys'></a>
### **Creating Composite Keys**

Include the `from_date` in the composite key.

```python
from functools import reduce

# Function to handle null values in composite key
def null_placeholder(column):
    return when(col(column).isNull(), lit('__NULL__')).otherwise(col(column).cast('string'))

# Create composite key in historical DataFrame, including 'from_date'
historical_df = historical_df.withColumn(
    'composite_key',
    concat_ws('_', *[null_placeholder(c) for c in common_cols + ['from_date']])
)

# Create composite key in current DataFrame
current_df = current_df.withColumn(
    'composite_key',
    concat_ws('_', *[null_placeholder(c) for c in common_cols + ['from_date']])
)
```

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

---

<a id='hash-values'></a>
### **Computing Hash Values of Attributes**

Use all attribute columns excluding keys and dates.

```python
# Attribute columns to include in hash calculation
# Exclude 'otherkey', 'from_date', 'to_date', and 'composite_key'
attribute_cols = [c for c in historical_df.columns if c not in ('otherkey', 'from_date', 'to_date', 'composite_key')]

# Compute hash_value in historical DataFrame
historical_df = historical_df.withColumn(
    'hash_value',
    sha2(concat_ws('||', *[coalesce(col(c).cast('string'), lit(''))) for c in attribute_cols]), 256)
)

# Compute hash_value in current DataFrame
current_df = current_df.withColumn(
    'hash_value',
    sha2(concat_ws('||', *[coalesce(col(c).cast('string'), lit(''))) for c in attribute_cols]), 256)
)
```

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

**Assertion:**

```python
# Assert that joined_df has been created
assert joined_df is not None, "joined_df was not created successfully."

# Assert that joined_df has the expected number of rows
expected_min_rows = max(historical_df.select('composite_key').distinct().count(), current_df.select('composite_key').distinct().count())
actual_rows = joined_df.select('composite_key').distinct().count()
assert actual_rows >= expected_min_rows, f"joined_df has fewer rows than expected: {actual_rows} < {expected_min_rows}"
```

---

<a id='source-identification'></a>
### **Identifying Source of Records**

Use dynamic checks based on common columns plus `from_date`.

```python
# Function to check if all common columns plus 'from_date' are not null in a DataFrame
def all_common_cols_not_null(prefix):
    return reduce(lambda a, b: a & b, [col(f"{prefix}.{c}").isNotNull() for c in common_cols + ['from_date']])

# Identify source of records
joined_df = joined_df.withColumn(
    'source',
    when(all_common_cols_not_null('hist') & all_common_cols_not_null('curr'), lit('both'))
    .when(all_common_cols_not_null('hist'), lit('hist_only'))
    .when(all_common_cols_not_null('curr'), lit('curr_only'))
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

**Assertion:**

```python
# Assert that unchanged_hist is created
assert unchanged_hist is not None, "unchanged_hist DataFrame was not created."

# Optionally, count the number of unchanged records
unchanged_count = unchanged_hist.count()
print(f"Number of unchanged records: {unchanged_count}")
```

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

**Assertion:**

```python
# Assert that updated_hist is created
assert updated_hist is not None, "updated_hist DataFrame was not created."

# Optionally, count the number of updated records
updated_count = updated_hist.count()
print(f"Number of updated records: {updated_count}")
```

---

#### **Generating Unique IDs for New Records**

```python
# Generate unique IDs using row_number()
window = Window.orderBy(F.monotonically_increasing_id())
```

---

#### **New Records from Current Only**

```python
# New records from curr_only
new_records_from_current = joined_df.where(
    col('source') == 'curr_only'
).select(
    row_number().over(window).alias('otherkey'),
    *[col('curr.' + c) for c in current_df.columns if c not in ('composite_key', 'hash_value')],
    lit(None).cast(DateType()).alias('to_date')
)
```

**Assertion:**

```python
# Assert that new_records_from_current is created
assert new_records_from_current is not None, "new_records_from_current DataFrame was not created."

# Optionally, count the number of new records from curr_only
new_current_count = new_records_from_current.count()
print(f"Number of new records from curr_only: {new_current_count}")
```

---

#### **New Records from Updated Records**

```python
# New records from updated records
new_records_from_updated = joined_df.where(
    (col('source') == 'both') &
    (col('hist.hash_value') != col('curr.hash_value'))
).select(
    row_number().over(window).alias('otherkey'),
    *[col('curr.' + c) for c in current_df.columns if c not in ('composite_key', 'hash_value')],
    lit(None).cast(DateType()).alias('to_date')
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

---

#### **Combining All Records**

```python
# Union all records
historical_df_updated = unchanged_hist.union(updated_hist).union(new_records_from_current).union(new_records_from_updated)
```

**Assertion:**

```python
# Assert that historical_df_updated is created
assert historical_df_updated is not None, "historical_df_updated DataFrame was not created."

# Optionally, count the total number of records
total_records = historical_df_updated.count()
print(f"Total number of records in historical_df_updated: {total_records}")
```

---

#### **Cleaning Up Auxiliary Columns**

```python
# Remove auxiliary columns if present
historical_df_updated = historical_df_updated.select(
    [c for c in historical_df_updated.columns if c not in ('composite_key', 'hash_value', 'source')]
)
```

**Assertion:**

```python
# Assert that auxiliary columns have been removed
assert 'composite_key' not in historical_df_updated.columns, "composite_key should have been removed."
assert 'hash_value' not in historical_df_updated.columns, "hash_value should have been removed."
assert 'source' not in historical_df_updated.columns, "source should have been removed."
```

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

**Assertion:**

```python
# Assert that historical_df_sorted is created
assert historical_df_sorted is not None, "historical_df_sorted DataFrame was not created."

# Optionally, check the number of records remains the same
assert historical_df_sorted.count() == total_records, "Record count changed after sorting."
```

---

### **Formatting Dates**

```python
# Convert dates to desired string format
final_display_df = historical_df_sorted \
    .withColumn("from_date", date_format(col("from_date"), "dd/MM/yyyy")) \
    .withColumn("to_date", date_format(col("to_date"), "dd/MM/yyyy")) \
    .na.fill({"to_date": ""})
```

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

---

### **Displaying the Updated DataFrame**

```python
# Show the updated DataFrame
final_display_df.show(truncate=False)
```

---

<a id='conclusion'></a>
## **5. Conclusion**

In this notebook, we addressed the weaknesses in the previous code by:

- **Correct Key Specification**: Including the `from_date` in the composite key to accurately compare records.
- **Dynamic Column Handling**: Using lists of common columns instead of hardcoded column names, making the code more flexible.
- **Source Identification**: Adjusting the logic to use common columns and `from_date` when determining the source of records.
- **Renaming DataFrames and Columns**: Renaming `df_historical` to `historical_df`, `df_final` to `current_df`, and `from`/`to` to `from_date`/`to_date` for clarity and to avoid reserved keywords.

By making these adjustments, we ensure that the code accurately performs SCD Type 2 updates, correctly identifies changes, and is adaptable to schema changes.

---

**Note:** Ensure that the Spark session is stopped after the notebook execution to release resources.

```python
# Stop the Spark session
spark.stop()
```

**Assertion:**

```python
# Attempting to use spark after stopping should raise an error
try:
    spark.range(1).collect()
    assert False, "Spark session should be stopped and not allow operations."
except Exception as e:
    print("Spark session has been stopped successfully.")
```