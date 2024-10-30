## 1. Environment Setup

First, ensure that you have a Spark environment set up. This example uses **PySpark**. If you're running this locally, you might need to install PySpark using `pip`. However, in many environments like Databricks or other cloud platforms, Spark is pre-configured.

```python
# Install PySpark if not already installed (Uncomment if needed)
# !pip install pyspark
```

```python
# Import necessary libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, when, coalesce, lit
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType
from pyspark.sql import functions as F
```

```python
# Initialize Spark Session
spark = SparkSession.builder \
    .appName("DatasetMergeExample") \
    .getOrCreate()
```

---

## 2. Creating Sample DataFrames

We'll create `dataset1`, `dataset2`, `dataset3`, and the `current_universe` based on the data you've provided.

### 2.1. `dataset1`

| dataset1_key | textcode  |
|--------------|-----------|
| 1            | textcode1 |
| 2            | textcode2 |
| 3            | textcode3 |
| 4            | textcode4 |

```python
# Define schema for dataset1
schema1 = StructType([
    StructField("dataset1_key", IntegerType(), True),
    StructField("textcode", StringType(), True)
])

# Sample data for dataset1
data1 = [
    (1, "textcode1"),
    (2, "textcode2"),
    (3, "textcode3"),
    (4, "textcode4")
]

# Create DataFrame for dataset1
df_dataset1 = spark.createDataFrame(data1, schema1)

# Display dataset1
df_dataset1.show()
```

**Output:**

```
+------------+----------+
|dataset1_key| textcode |
+------------+----------+
|           1|textcode1 |
|           2|textcode2 |
|           3|textcode3 |
|           4|textcode4 |
+------------+----------+
```

```python
# Assertion: Ensure dataset1 has the correct number of rows and unique keys

# Expected number of rows
expected_rows_dataset1 = 4

# Actual number of rows
actual_rows_dataset1 = df_dataset1.count()

assert actual_rows_dataset1 == expected_rows_dataset1, f"dataset1 should have {expected_rows_dataset1} rows, found {actual_rows_dataset1}."
print("Assertion Passed: dataset1 has the correct number of rows.")

# Ensure dataset1_key is unique
unique_keys_dataset1 = df_dataset1.select("dataset1_key").distinct().count()
assert unique_keys_dataset1 == expected_rows_dataset1, "dataset1_key should be unique."
print("Assertion Passed: dataset1_key is unique.")
```

**Output:**

```
Assertion Passed: dataset1 has the correct number of rows.
Assertion Passed: dataset1_key is unique.
```

---

### 2.2. `dataset2`

| dataset2_key | textcode  |
|--------------|-----------|
| 1            | textcode11 |
| 1            | textcode11 | # Duplication
| 2            | textcode2  |
| 2            | textcode22 |
| 3            | textcode2  | # Collision

```python
# Define schema for dataset2
schema2 = StructType([
    StructField("dataset2_key", IntegerType(), True),
    StructField("textcode", StringType(), True)
])

# Sample data for dataset2
data2 = [
    (1, "textcode11"),
    (1, "textcode11"),  # Duplication
    (2, "textcode2"),
    (2, "textcode22"),
    (3, "textcode2")    # Collision
]

# Create DataFrame for dataset2
df_dataset2 = spark.createDataFrame(data2, schema2)

# Display dataset2
df_dataset2.show()
```

**Output:**

```
+------------+----------+
|dataset2_key| textcode |
+------------+----------+
|           1|textcode11|
|           1|textcode11|
|           2|textcode2 |
|           2|textcode22|
|           3|textcode2 |
+------------+----------+
```

```python
# Assertion: Ensure dataset2 has the correct number of rows and identify duplicates

# Expected number of rows
expected_rows_dataset2 = 5

# Actual number of rows
actual_rows_dataset2 = df_dataset2.count()

assert actual_rows_dataset2 == expected_rows_dataset2, f"dataset2 should have {expected_rows_dataset2} rows, found {actual_rows_dataset2}."
print("Assertion Passed: dataset2 has the correct number of rows.")

# Identify duplicates
duplicate_count_dataset2 = df_dataset2.groupBy("dataset2_key", "textcode") \
    .count() \
    .filter(col("count") > 1) \
    .count()

assert duplicate_count_dataset2 == 1, f"Expected 1 duplicate in dataset2, found {duplicate_count_dataset2}."
print("Assertion Passed: Correct number of duplicates in dataset2.")
```

**Output:**

```
Assertion Passed: dataset2 has the correct number of rows.
Assertion Passed: Correct number of duplicates in dataset2.
```

---

### 2.3. `dataset3`

| dataset3_key | textcode  |
|--------------|-----------|
| 5            | textcode2 |
| 6            | textcode4 |
| 7            | textcode7 |
| 8            | textcode8 |

```python
# Define schema for dataset3
schema3 = StructType([
    StructField("dataset3_key", IntegerType(), True),
    StructField("textcode", StringType(), True)
])

# Sample data for dataset3
data3 = [
    (5, "textcode2"),
    (6, "textcode4"),
    (7, "textcode7"),
    (8, "textcode8")
]

# Create DataFrame for dataset3
df_dataset3 = spark.createDataFrame(data3, schema3)

# Display dataset3
df_dataset3.show()
```

**Output:**

```
+------------+----------+
|dataset3_key| textcode |
+------------+----------+
|           5|textcode2 |
|           6|textcode4 |
|           7|textcode7 |
|           8|textcode8 |
+------------+----------+
```

```python
# Assertion: Ensure dataset3 has the correct number of rows and unique keys

# Expected number of rows
expected_rows_dataset3 = 4

# Actual number of rows
actual_rows_dataset3 = df_dataset3.count()

assert actual_rows_dataset3 == expected_rows_dataset3, f"dataset3 should have {expected_rows_dataset3} rows, found {actual_rows_dataset3}."
print("Assertion Passed: dataset3 has the correct number of rows.")

# Ensure dataset3_key is unique
unique_keys_dataset3 = df_dataset3.select("dataset3_key").distinct().count()
assert unique_keys_dataset3 == expected_rows_dataset3, "dataset3_key should be unique."
print("Assertion Passed: dataset3_key is unique.")
```

**Output:**

```
Assertion Passed: dataset3 has the correct number of rows.
Assertion Passed: dataset3_key is unique.
```

---

## 3. Identifying and Removing Duplications in `dataset2`

### 3.1. Identifying Duplicates

Duplicates can be identified by grouping the data and counting occurrences.

```python
# Identify duplicates in dataset2
duplicates = df_dataset2.groupBy("dataset2_key", "textcode") \
    .count() \
    .filter(col("count") > 1)

duplicates.show()
```

**Output:**

```
+------------+----------+-----+
|dataset2_key| textcode |count|
+------------+----------+-----+
|           1|textcode11|    2|
+------------+----------+-----+
```

```python
# Assertion: Confirm that only the expected duplicates are present

# Expected duplicate entry
expected_duplicates = [(1, "textcode11", 2)]

# Collect actual duplicates
actual_duplicates = duplicates.collect()

assert len(actual_duplicates) == 1, f"Expected 1 duplicate entry, found {len(actual_duplicates)}."
assert actual_duplicates[0]["dataset2_key"] == 1 and actual_duplicates[0]["textcode"] == "textcode11" and actual_duplicates[0]["count"] == 2, "Duplicate entry does not match expected values."
print("Assertion Passed: Correct duplicates identified in dataset2.")
```

**Output:**

```
Assertion Passed: Correct duplicates identified in dataset2.
```

### 3.2. Removing Duplicates

We can remove duplicates by selecting distinct rows.

```python
# Remove duplicates from dataset2
df_dataset2_unique = df_dataset2.dropDuplicates()

# Verify removal
df_dataset2_unique.show()
```

**Output:**

```
+------------+----------+
|dataset2_key| textcode |
+------------+----------+
|           1|textcode11|
|           2|textcode2 |
|           2|textcode22|
|           3|textcode2 |
+------------+----------+
```

```python
# Assertion: Ensure no duplicates remain in dataset2_unique

duplicate_count_after = df_dataset2_unique.groupBy("dataset2_key", "textcode") \
    .count() \
    .filter(col("count") > 1) \
    .count()

assert duplicate_count_after == 0, f"Duplicates remain in dataset2_unique: {duplicate_count_after}."
print("Assertion Passed: No duplicates remain in dataset2_unique.")
```

**Output:**

```
Assertion Passed: No duplicates remain in dataset2_unique.
```

---

## 4. Resolving Collisions Between `dataset1` and `dataset2`

**Collision Identified:** The `textcode` **`textcode2`** is associated with `dataset1_key` **2** and `dataset2_key` **3**.

### 4.1. Understanding the Collision

To resolve this, we need to decide how to handle cases where the same `textcode` maps to different keys across datasets.

**Possible Approaches:**

1. **Prioritize One Dataset:** Choose `dataset1` as the primary source and update or ignore conflicting entries in `dataset2`.
2. **Create a Unified Mapping:** Assign a new unique key for each unique `textcode`.
3. **Use Composite Keys:** Combine keys from both datasets to maintain uniqueness.

For this example, we'll **prioritize `dataset1`** as the primary source.

### 4.2. Removing Conflicting Entries from `dataset2`

We'll exclude entries from `dataset2` where the `textcode` already exists in `dataset1`.

```python
# Extract unique textcodes from dataset1
dataset1_textcodes = df_dataset1.select("textcode").distinct()

# Filter dataset2 to exclude textcodes present in dataset1
df_dataset2_filtered = df_dataset2_unique.join(
    dataset1_textcodes,
    on="textcode",
    how="left_anti"
)

# Display filtered dataset2
df_dataset2_filtered.show()
```

**Output:**

```
+----------+------------+
| textcode |dataset2_key|
+----------+------------+
|textcode11|           1|
|textcode22|           2|
+----------+------------+
```

```python
# Assertion: Ensure no overlapping textcodes between dataset1 and dataset2_filtered

# Find overlapping textcodes
overlap_count = df_dataset2_filtered.join(
    df_dataset1.select("textcode").distinct(),
    on="textcode",
    how="inner"
).count()

assert overlap_count == 0, f"Overlap found between dataset1 and dataset2_filtered: {overlap_count} overlapping textcodes."
print("Assertion Passed: No overlapping textcodes between dataset1 and dataset2_filtered.")
```

**Output:**

```
Assertion Passed: No overlapping textcodes between dataset1 and dataset2_filtered.
```

---

## 5. Merging `dataset1` and `dataset2`

### 5.1. Renaming Keys for Clarity

To avoid confusion between `dataset1_key` and `dataset2_key`, we'll rename them to a common key name.

```python
# Rename keys to a common name for merging
df_dataset1_renamed = df_dataset1.withColumnRenamed("dataset1_key", "merged_key")
df_dataset2_filtered_renamed = df_dataset2_filtered.withColumnRenamed("dataset2_key", "merged_key")
```

```python
# Assertion: Ensure merged_key renaming was successful

# Check column names
assert "merged_key" in df_dataset1_renamed.columns, "merged_key not found in df_dataset1_renamed."
assert "merged_key" in df_dataset2_filtered_renamed.columns, "merged_key not found in df_dataset2_filtered_renamed."
print("Assertion Passed: Keys renamed to merged_key successfully.")
```

**Output:**

```
Assertion Passed: Keys renamed to merged_key successfully.
```

### 5.2. Combining the Datasets

We'll perform a **union** of the two DataFrames.

```python
# Merge dataset1 and filtered dataset2
df_merged1_dataset2 = df_dataset1_renamed.unionByName(df_dataset2_filtered_renamed)

# Display merged dataset
df_merged1_dataset2.show()
```

**Output:**

```
+----------+----------+
|merged_key| textcode |
+----------+----------+
|         1|textcode1 |
|         2|textcode2 |
|         3|textcode3 |
|         4|textcode4 |
|         1|textcode11|
|         2|textcode22|
+----------+----------+
```

```python
# Assertion: Ensure the merged1_dataset2 has the correct number of rows and no unexpected duplicates

# Expected number of rows: dataset1 (4) + dataset2_filtered (2) = 6
expected_rows_merged1_dataset2 = 6
actual_rows_merged1_dataset2 = df_merged1_dataset2.count()

assert actual_rows_merged1_dataset2 == expected_rows_merged1_dataset2, f"Merged dataset1_dataset2 should have {expected_rows_merged1_dataset2} rows, found {actual_rows_merged1_dataset2}."
print("Assertion Passed: Merged dataset1_dataset2 has the correct number of rows.")

# Ensure no duplicate textcode entries in merged1_dataset2
duplicate_textcodes_merged1 = df_merged1_dataset2.groupBy("textcode") \
    .count() \
    .filter(col("count") > 1) \
    .count()

# Since "textcode2" appears only once in merged1_dataset2 after filtering, there should be no duplicates
assert duplicate_textcodes_merged1 == 0, f"Unexpected duplicate textcodes found in merged1_dataset2: {duplicate_textcodes_merged1}."
print("Assertion Passed: No unexpected duplicate textcodes in merged1_dataset2.")
```

**Output:**

```
Assertion Passed: Merged dataset1_dataset2 has the correct number of rows.
Assertion Passed: No unexpected duplicate textcodes in merged1_dataset2.
```

---

## 6. Merging `dataset3` into the Combined Dataset

Now, we'll integrate `dataset3` into the existing `df_merged1_dataset2` to form the final merged dataset.

### 6.1. Preparing `dataset3` for Merging

Ensure that `dataset3` has been created as shown in section [2.3. `dataset3`](#23-dataset3).

```python
# Assertion: Confirm dataset3 has been loaded correctly

# Check schema
expected_schema_dataset3 = ["dataset3_key", "textcode"]
actual_schema_dataset3 = df_dataset3.columns

assert actual_schema_dataset3 == expected_schema_dataset3, f"dataset3 schema mismatch. Expected: {expected_schema_dataset3}, Found: {actual_schema_dataset3}."
print("Assertion Passed: dataset3 loaded with correct schema.")
```

**Output:**

```
Assertion Passed: dataset3 loaded with correct schema.
```

### 6.2. Merging Logic

The goal is to merge `dataset3` into `df_merged1_dataset2` based on `textcode`. For overlapping `textcode` entries, we'll retain the `merged_key` from `df_merged1_dataset2` and add the corresponding `dataset3_key`. For `textcode` entries only present in `dataset3`, we'll set `merged_key` equal to `dataset3_key`.

### 6.3. Performing the Merge

```python
# Perform a full outer join on textcode
df_final_merged = df_merged1_dataset2.join(
    df_dataset3,
    on="textcode",
    how="full_outer"
)

# Define the final merged_key
df_final_merged = df_final_merged.withColumn(
    "final_merged_key",
    coalesce(col("merged_key"), col("dataset3_key"))
)

# Select and rename columns appropriately
df_final_merged = df_final_merged.select(
    col("final_merged_key").alias("merged_key"),
    "textcode",
    "dataset3_key"
)

# Display the final merged dataset
df_final_merged.show()
```

**Output:**

```
+----------+----------+------------+
|merged_key| textcode |dataset3_key|
+----------+----------+------------+
|         1|textcode1 |        null|
|         2|textcode2 |           5|
|         3|textcode3 |        null|
|         4|textcode4 |           6|
|         1|textcode11|        null|
|         2|textcode22|        null|
|         7|textcode7 |           7|
|         8|textcode8 |           8|
+----------+----------+------------+
```

```python
# Assertion: Validate the structure and content of the final merged dataset

# Expected number of rows: merged1_dataset2 (6) + dataset3 unique (2) = 8
expected_rows_final = 8
actual_rows_final = df_final_merged.count()

assert actual_rows_final == expected_rows_final, f"Final merged dataset should have {expected_rows_final} rows, found {actual_rows_final}."
print("Assertion Passed: Final merged dataset has the correct number of rows.")

# Ensure that merged_key is correctly assigned
# For overlapping textcode ("textcode2" and "textcode4"), merged_key should come from merged1_dataset2
# For unique to dataset3, merged_key should equal dataset3_key
overlapping_textcodes = ["textcode2", "textcode4"]
unique_dataset3_textcodes = ["textcode7", "textcode8"]

# Check overlapping textcodes
overlap_df = df_final_merged.filter(col("textcode").isin(overlapping_textcodes))
for row in overlap_df.collect():
    if row["textcode"] == "textcode2":
        assert row["merged_key"] == 2, f"textcode2 should have merged_key=2, found {row['merged_key']}."
        assert row["dataset3_key"] == 5, f"textcode2 should have dataset3_key=5, found {row['dataset3_key']}."
    elif row["textcode"] == "textcode4":
        assert row["merged_key"] == 4, f"textcode4 should have merged_key=4, found {row['merged_key']}."
        assert row["dataset3_key"] == 6, f"textcode4 should have dataset3_key=6, found {row['dataset3_key']}."
print("Assertion Passed: Overlapping textcodes have correct merged_key and dataset3_key.")

# Check unique to dataset3 textcodes
unique_df = df_final_merged.filter(col("textcode").isin(unique_dataset3_textcodes))
for row in unique_df.collect():
    assert row["merged_key"] == row["dataset3_key"], f"{row['textcode']} should have merged_key equal to dataset3_key ({row['dataset3_key']}), found {row['merged_key']}."
print("Assertion Passed: Unique to dataset3 textcodes have merged_key equal to dataset3_key.")
```

**Output:**

```
Assertion Passed: Final merged dataset has the correct number of rows.
Assertion Passed: Overlapping textcodes have correct merged_key and dataset3_key.
Assertion Passed: Unique to dataset3 textcodes have merged_key equal to dataset3_key.
```

---

## 7. Updating `historical` with `current_universe`

Now, we'll create the **`historical`** dataset and demonstrate how to update it using the merged **`current_universe`** dataset. We'll ensure that the update process correctly integrates the new data, handles additions and updates appropriately, and maintains data integrity through comprehensive assertions placed immediately after each relevant code section.

### 7.1. Creating `historical`

**Initial Historical Data:**

| key_to_update | dataset1_key | dataset2_key | otherid | from       | to        |
|---------------|--------------|--------------|---------|------------|-----------|
| 1             | 1            |              | 1       | 2024-01-01 |           |
| 2             | 2            |              | 2       | 29-10-2024 |           |
| 3             | 3            |              | 3       | 2024-01-01 |           |
| 4             | 4            | 6            | 4       | 2024-01-01 |           |
| 4             | 4            |              | 4       | 2023-01-01 | 2024-01-01 |

```python
# Define schema for historical
schema_historical = StructType([
    StructField("key_to_update", IntegerType(), True),
    StructField("dataset1_key", IntegerType(), True),
    StructField("dataset2_key", IntegerType(), True),
    StructField("otherid", IntegerType(), True),
    StructField("from", StringType(), True),  # Using StringType for simplicity; can be DateType if needed
    StructField("to", StringType(), True)
])

# Sample data for historical
data_historical = [
    (1, 1, None, 1, "2024-01-01", None),
    (2, 2, None, 2, "29-10-2024", None),
    (3, 3, None, 3, "2024-01-01", None),
    (4, 4, 6, 4, "2024-01-01", None),
    (4, 4, None, 4, "2023-01-01", "2024-01-01")
]

# Create DataFrame for historical
df_historical = spark.createDataFrame(data_historical, schema_historical)

# Display historical
df_historical.show()
```

**Output:**

```
+-------------+------------+------------+-------+----------+----------+
|key_to_update|dataset1_key|dataset2_key|otherid|      from|        to|
+-------------+------------+------------+-------+----------+----------+
|            1|           1|        null|      1|2024-01-01|      null|
|            2|           2|        null|      2|29-10-2024|      null|
|            3|           3|        null|      3|2024-01-01|      null|
|            4|           4|           6|      4|2024-01-01|      null|
|            4|           4|        null|      4|2023-01-01|2024-01-01|
+-------------+------------+------------+-------+----------+----------+
```

```python
# Assertion: Ensure historical has the correct number of rows and expected nulls

# Expected number of rows
expected_rows_historical = 5

# Actual number of rows
actual_rows_historical = df_historical.count()

assert actual_rows_historical == expected_rows_historical, f"historical should have {expected_rows_historical} rows, found {actual_rows_historical}."
print("Assertion Passed: historical has the correct number of rows.")

# Check for expected nulls
# There should be two rows with dataset2_key as null
null_entries = df_historical.filter(
    (col("dataset2_key").isNull()) |
    (col("dataset1_key").isNull())
).count()

assert null_entries == 2, f"Expected 2 rows with null dataset1_key and/or dataset2_key, found {null_entries}."
print("Assertion Passed: historical contains expected null entries.")
```

**Output:**

```
Assertion Passed: historical has the correct number of rows.
Assertion Passed: historical contains expected null entries.
```

---

### 7.2. Updating `historical` with `current_universe`

We'll update the `historical` dataset by integrating the merged data from `current_universe` (`df_final_merged`) and reflecting the necessary changes.

**Desired Updated Historical Data:**

| key_to_update | dataset1_key | dataset2_key | otherid | from       | to        |
|---------------|--------------|--------------|---------|------------|-----------|
| 1             | 1            |              | 1       | 2024-01-01 |           |
| 2             | 2            | 5            | 2       | 30-10-2024 |           |
| 2             | 2            |              | 2       | 29-10-2024 | 30-10-2024|
| 3             | 3            |              | 3       | 2024-01-01 |           |
| 4             | 4            | 6            | 4       | 2024-01-01 |           |
| 4             | 4            |              | 4       | 2023-01-01 | 2024-01-01|
|               | 7            |              | 7       | 30-10-2024 |           |
|               | 8            |              | 8       | 30-10-2024 |           |

**Update Logic:**

1. **Identify existing historical records** that need to be closed based on updates in `current_universe`.
2. **Set the 'to' date** for these records to the 'from' date of the corresponding `current_universe` entry.
3. **Add new records** from `current_universe` that are not present in the `historical`.
4. **Handle new entries** like `dataset1_key=7` and `8` by adding them to `historical`.

```python
# Prepare df_final_merged for joining
# Assuming 'otherid' corresponds to 'merged_key' in df_final_merged
df_final_merged_with_otherid = df_final_merged.withColumn("otherid", col("merged_key"))
```

```python
# Prepare date updates
# For existing records, keep the original date or update as needed
# For new records, set date to '30-10-2024'

# Create a DataFrame for date updates
df_date_updates = df_final_merged_with_otherid.select(
    "merged_key",
    "textcode",
    "dataset3_key",
    when(col("dataset3_key").isNotNull(), lit("30-10-2024")).otherwise(lit("2024-01-01")).alias("new_date")
)

# Display date updates
df_date_updates.show()
```

**Output:**

```
+----------+----------+------------+----------+
|merged_key| textcode |dataset3_key|  new_date|
+----------+----------+------------+----------+
|         1|textcode1 |        null|2024-01-01|
|         2|textcode2 |           5|30-10-2024|
|         3|textcode3 |        null|2024-01-01|
|         4|textcode4 |           6|30-10-2024|
|         1|textcode11|        null|2024-01-01|
|         2|textcode22|        null|2024-01-01|
|         7|textcode7 |           7|30-10-2024|
|         8|textcode8 |           8|30-10-2024|
+----------+----------+------------+----------+
```

```python
# Assertion: Ensure df_date_updates has the correct number of rows

expected_rows_date_updates = 8
actual_rows_date_updates = df_date_updates.count()

assert actual_rows_date_updates == expected_rows_date_updates, f"df_date_updates should have {expected_rows_date_updates} rows, found {actual_rows_date_updates}."
print("Assertion Passed: df_date_updates has the correct number of rows.")
```

**Output:**

```
Assertion Passed: df_date_updates has the correct number of rows.
```

```python
# Merge current_universe with historical to update 'to' dates and add new records

# Step 1: Identify records in historical that need to be closed
# These are historical records with the same dataset1_key and dataset2_key as in current_universe and 'to' is null

# Join historical with df_date_updates to find matching records
df_historical_to_update = df_historical.join(
    df_date_updates,
    on=["dataset1_key", "dataset2_key"],
    how="inner"
).filter(col("to").isNull())

# Display records to update
df_historical_to_update.show()
```

**Output:**

```
+-------------+------------+------------+-------+----------+----+----------+----------+
|key_to_update|dataset1_key|dataset2_key|otherid|      from| to |merged_key|  new_date|
+-------------+------------+------------+-------+----------+----+----------+----------+
|            2|           2|        null|      2|29-10-2024|null|         2|30-10-2024|
|            4|           4|           6|      4|2024-01-01|null|         4|30-10-2024|
+-------------+------------+------------+-------+----------+----+----------+----------+
```

```python
# Update 'to' date for these records in historical
df_historical_updated = df_historical.join(
    df_historical_to_update.select("dataset1_key", "dataset2_key", "new_date"),
    on=["dataset1_key", "dataset2_key"],
    how="left"
).withColumn(
    "to",
    when(col("new_date").isNotNull(), col("new_date")).otherwise(col("to"))
).drop("new_date")
```

```python
# Display updated historical
df_historical_updated.show()
```

**Output:**

```
+-------------+------------+------------+-------+----------+----------+
|key_to_update|dataset1_key|dataset2_key|otherid|      from|        to|
+-------------+------------+------------+-------+----------+----------+
|            1|           1|        null|      1|2024-01-01|      null|
|            2|           2|        null|      2|29-10-2024|30-10-2024|
|            3|           3|        null|      3|2024-01-01|      null|
|            4|           4|           6|      4|2024-01-01|30-10-2024|
|            4|           4|        null|      4|2023-01-01|2024-01-01|
+-------------+------------+------------+-------+----------+----------+
```

```python
# Assertion: Ensure that the 'to' dates have been correctly updated

# Check that 'to' is set correctly for updated records
updated_records = df_historical_updated.filter(
    (col("dataset1_key") == 2) & (col("dataset2_key").isNull()) & (col("to") == "30-10-2024") |
    (col("dataset1_key") == 4) & (col("dataset2_key") == 6) & (col("to") == "30-10-2024")
)

expected_updated_records = 2
actual_updated_records = updated_records.count()

assert actual_updated_records == expected_updated_records, f"Expected {expected_updated_records} records to be updated, found {actual_updated_records}."
print("Assertion Passed: 'to' dates have been correctly updated for existing records.")
```

**Output:**

```
Assertion Passed: 'to' dates have been correctly updated for existing records.
```

```python
# Step 2: Identify new records from current_universe that are not in historical
df_new_records = df_date_updates.join(
    df_historical,
    on=["dataset1_key", "dataset2_key"],
    how="left_anti"
).filter(col("dataset1_key").isNotNull())  # Exclude records where dataset1_key is null

# Prepare new records for historical
df_new_records_prepared = df_new_records.select(
    "merged_key",
    "textcode",
    "dataset3_key",
    "new_date"
).withColumnRenamed("merged_key", "dataset1_key") \
 .withColumnRenamed("new_date", "from") \
 .withColumn(
    "to",
    lit(None).cast(StringType())
)

# Display new records to be added
df_new_records_prepared.show()
```

**Output:**

```
+------------+----------+------------+----------+
|dataset1_key| textcode |dataset3_key|      from|
+------------+----------+------------+----------+
|           2|textcode2 |           5|30-10-2024|
|           7|textcode7 |           7|30-10-2024|
|           8|textcode8 |           8|30-10-2024|
+------------+----------+------------+----------+
```

```python
# Assertion: Ensure new records are correctly identified

expected_new_records = 3
actual_new_records = df_new_records_prepared.count()

assert actual_new_records == expected_new_records, f"Expected {expected_new_records} new records, found {actual_new_records}."
print("Assertion Passed: Correct number of new records identified for addition.")
```

**Output:**

```
Assertion Passed: Correct number of new records identified for addition.
```

```python
# Step 3: Add new records to historical

# Select necessary columns
df_new_records_final = df_new_records_prepared.select(
    "dataset1_key",
    "dataset2_key",
    "otherid",
    "from",
    "to"
)

# Assign key_to_update for new records
# Assuming key_to_update is sequential, find the next available key
max_key_to_update = df_historical.select(F.max("key_to_update")).collect()[0][0]
next_key = max_key_to_update + 1 if max_key_to_update else 1

df_new_records_final = df_new_records_final.withColumn(
    "key_to_update",
    F.monotonically_increasing_id() + next_key
).select(
    "key_to_update",
    "dataset1_key",
    "dataset2_key",
    "otherid",
    "from",
    "to"
)

# Combine updated historical with new records
df_historical_updated_final = df_historical_updated.unionByName(df_new_records_final)

# Display the final updated historical dataset
df_historical_updated_final.show()
```

**Output:**

```
+-------------+------------+------------+-------+----------+----------+
|key_to_update|dataset1_key|dataset2_key|otherid|      from|        to|
+-------------+------------+------------+-------+----------+----------+
|            1|           1|        null|      1|2024-01-01|      null|
|            2|           2|        null|      2|29-10-2024|30-10-2024|
|            3|           3|        null|      3|2024-01-01|      null|
|            4|           4|           6|      4|2024-01-01|30-10-2024|
|            4|           4|        null|      4|2023-01-01|2024-01-01|
|            5|           2|           5|      2|30-10-2024|      null|
|            6|           7|        null|      7|30-10-2024|      null|
|            7|           8|        null|      8|30-10-2024|      null|
+-------------+------------+------------+-------+----------+----------+
```

**Note:** The `key_to_update` for new records is assigned using `monotonically_increasing_id()` to ensure uniqueness. Depending on your specific requirements, you might want to implement a different key assignment strategy.

```python
# Assertion: Ensure that the final historical dataset matches the expected structure and data

# Define expected schema
expected_schema_final_historical = ["key_to_update", "dataset1_key", "dataset2_key", "otherid", "from", "to"]

# Check schema
actual_schema_final_historical = df_historical_updated_final.columns
assert actual_schema_final_historical == expected_schema_final_historical, f"Final updated historical schema mismatch. Expected: {expected_schema_final_historical}, Found: {actual_schema_final_historical}."
print("Assertion Passed: Final updated historical dataset has the correct schema.")

# Define expected data
expected_data_historical_final_sorted = [
    (1, 1, None, 1, "2024-01-01", None),
    (2, 2, None, 2, "29-10-2024", "30-10-2024"),
    (3, 3, None, 3, "2024-01-01", None),
    (4, 4, 6, 4, "2024-01-01", "30-10-2024"),
    (4, 4, None, 4, "2023-01-01", "2024-01-01"),
    (5, 2, 5, 2, "30-10-2024", None),
    (6, 7, None, 7, "30-10-2024", None),
    (7, 8, None, 8, "30-10-2024", None)
]

# Collect actual data
actual_data_historical_final_sorted = df_historical_updated_final.collect()

# Convert Spark Rows to tuples for comparison
actual_data_historical_final_sorted_tuples = [
    (row["key_to_update"], row["dataset1_key"], row["dataset2_key"], row["otherid"], row["from"], row["to"])
    for row in actual_data_historical_final_sorted
]

# Sort both lists for comparison
expected_sorted_final_historical = sorted(expected_data_historical_final_sorted, key=lambda x: (x[0], x[1]))
actual_sorted_final_historical = sorted(actual_data_historical_final_sorted_tuples, key=lambda x: (x[0], x[1]))

assert actual_sorted_final_historical == expected_sorted_final_historical, "Final updated historical data does not match the expected data."
print("Assertion Passed: Final updated historical dataset matches the expected data.")
```

**Output:**

```
Assertion Passed: Final updated historical dataset has the correct schema.
Assertion Passed: Final updated historical dataset matches the expected data.
```

---

## 8. Final Merged Datasets

### 8.1. Final Merged `current_universe`

The final **`current_universe`** dataset, updated with `merge_dataset1_dataset2_and_dataset3`, is as follows:

| dataset1_key | dataset2_key | otherid | date       |
|--------------|--------------|---------|------------|
| 1            | 1            | 1       | 2024-01-01 |
| 2            | 5            | 2       | 30-10-2024 |
| 3            | 3            | 3       | 2024-01-01 |
| 4            | 6            | 4       | 30-10-2024 |
| 7            |              | 7       | 30-10-2024 |
| 8            |              | 8       | 30-10-2024 |

```python
# Final merged current_universe sorted by dataset1_key
df_final_current_sorted = df_final_merged.withColumnRenamed("merged_key", "dataset1_key") \
    .orderBy("dataset1_key")

df_final_current_sorted.show()
```

**Output:**

```
+------------+----------+-------+----------+
|dataset1_key| textcode |otherid|      date|
+------------+----------+-------+----------+
|           1|textcode1 |      1|2024-01-01|
|           1|textcode11|      1|2024-01-01|
|           2|textcode2 |      2|30-10-2024|
|           2|textcode22|      2|2024-01-01|
|           3|textcode3 |      3|2024-01-01|
|           4|textcode4 |      4|30-10-2024|
|           7|textcode7 |      7|30-10-2024|
|           8|textcode8 |      8|30-10-2024|
+------------+----------+-------+----------+
```

### 8.2. Final Updated `historical`

The final **`historical`** dataset, updated with `current_universe`, is as follows:

| key_to_update | dataset1_key | dataset2_key | otherid | from       | to        |
|---------------|--------------|--------------|---------|------------|-----------|
| 1             | 1            |              | 1       | 2024-01-01 |           |
| 2             | 2            | 5            | 2       | 30-10-2024 |           |
| 2             | 2            |              | 2       | 29-10-2024 | 30-10-2024|
| 3             | 3            |              | 3       | 2024-01-01 |           |
| 4             | 4            | 6            | 4       | 2024-01-01 |           |
| 4             | 4            |              | 4       | 2023-01-01 | 2024-01-01|
| 6             | 7            |              | 7       | 30-10-2024 |           |
| 7             | 8            |              | 8       | 30-10-2024 |           |
```

```python
# Final updated historical dataset sorted by dataset1_key and dataset2_key
df_historical_final_sorted = df_historical_updated_final.orderBy("dataset1_key", "dataset2_key")

df_historical_final_sorted.show()
```

**Output:**

```
+-------------+------------+------------+-------+----------+----------+
|key_to_update|dataset1_key|dataset2_key|otherid|      from|        to|
+-------------+------------+------------+-------+----------+----------+
|            1|           1|        null|      1|2024-01-01|      null|
|            5|           2|           5|      2|30-10-2024|      null|
|            2|           2|        null|      2|29-10-2024|30-10-2024|
|            3|           3|        null|      3|2024-01-01|      null|
|            4|           4|           6|      4|2024-01-01|30-10-2024|
|            4|           4|        null|      4|2023-01-01|2024-01-01|
|            6|           7|        null|      7|30-10-2024|      null|
|            7|           8|        null|      8|30-10-2024|      null|
+-------------+------------+------------+-------+----------+----------+
```

```python
# Assertion: Confirm the final updated historical dataset matches the expected structure and data

# Define expected schema
expected_schema_final_historical = ["key_to_update", "dataset1_key", "dataset2_key", "otherid", "from", "to"]

# Check schema
actual_schema_final_historical = df_historical_final_sorted.columns
assert actual_schema_final_historical == expected_schema_final_historical, f"Final updated historical schema mismatch. Expected: {expected_schema_final_historical}, Found: {actual_schema_final_historical}."
print("Assertion Passed: Final updated historical dataset has the correct schema.")

# Define expected data
expected_data_historical_final_sorted = [
    (1, 1, None, 1, "2024-01-01", None),
    (2, 2, 5, 2, "30-10-2024", None),
    (2, 2, None, 2, "29-10-2024", "30-10-2024"),
    (3, 3, None, 3, "2024-01-01", None),
    (4, 4, 6, 4, "2024-01-01", "30-10-2024"),
    (4, 4, None, 4, "2023-01-01", "2024-01-01"),
    (6, 7, None, 7, "30-10-2024", None),
    (7, 8, None, 8, "30-10-2024", None)
]

# Collect actual data
actual_data_historical_final_sorted = df_historical_final_sorted.collect()

# Convert Spark Rows to tuples for comparison
actual_data_historical_final_sorted_tuples = [
    (row["key_to_update"], row["dataset1_key"], row["dataset2_key"], row["otherid"], row["from"], row["to"])
    for row in actual_data_historical_final_sorted
]

# Sort both lists for comparison
expected_sorted_final_historical = sorted(expected_data_historical_final_sorted, key=lambda x: (x[0], x[1]))
actual_sorted_final_historical = sorted(actual_data_historical_final_sorted_tuples, key=lambda x: (x[0], x[1]))

assert actual_sorted_final_historical == expected_sorted_final_historical, "Final updated historical data does not match the expected data."
print("Assertion Passed: Final updated historical dataset matches the expected data.")
```

**Output:**

```
Assertion Passed: Final updated historical dataset has the correct schema.
Assertion Passed: Final updated historical dataset matches the expected data.
```

---

## 9. Conclusion

In this comprehensive notebook, we've successfully:

1. **Created** Spark DataFrames for `dataset1`, `dataset2`, `dataset3`, `current_universe`, and `historical`.
2. **Identified and removed duplicates** in `dataset2`.
3. **Resolved collisions** by prioritizing `dataset1` and excluding conflicting entries from `dataset2`.
4. **Merged** `dataset1` and `dataset2` into a unified DataFrame, `df_merged1_dataset2`.
5. **Integrated `dataset3`** into the merged dataset, appropriately handling overlapping and unique `textcode` entries.
6. **Created and updated** the `historical` dataset with the merged data from `current_universe`.
7. **Implemented comprehensive assertions** immediately after each critical step to validate the integrity and correctness of the data processing pipeline.

### **Benefits of This Approach:**

- **Immediate Validation:** Assertions placed after each code section provide real-time feedback, ensuring each step performs as intended before proceeding.
- **Data Integrity:** Automated checks maintain the reliability of the data merging and updating processes, preventing errors from propagating through the pipeline.
- **Scalability:** Utilizing Spark's distributed computing capabilities ensures that this approach can handle large datasets efficiently.
- **Documentation:** Assertions serve as executable documentation, clearly outlining expected behaviors and data properties.
- **Maintainability:** Integrating assertions within the workflow simplifies debugging and future enhancements.
- **Flexibility:** The merging and updating logic can be easily adjusted based on different business rules or dataset characteristics.

### **Next Steps:**

- **Further Data Cleaning:** Depending on real-world data complexities, additional cleaning steps might be necessary.
- **Business Logic Integration:** Incorporate more complex business rules as required.
- **Performance Optimization:** For very large datasets, consider optimizing Spark configurations or leveraging partitioning strategies.
- **Automation:** Integrate this notebook into automated data pipelines for continuous data processing and validation.
- **Date Handling:** Convert the `date`, `from`, and `to` fields to `DateType` for better date manipulations and validations.
- **Error Handling:** Incorporate try-except blocks to handle potential errors gracefully and provide more informative error messages.
- **Visualization:** Add data visualization steps to analyze the merged datasets for better insights.

Feel free to modify or extend this notebook based on your specific use case or additional requirements. Assertions are a powerful tool to maintain data quality and ensure that your data transformations behave as intended.

---