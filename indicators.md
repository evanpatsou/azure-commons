1. Environment Setup

Ensure that you have a Spark environment set up. This example uses PySpark. If you're running this locally, you might need to install PySpark using pip. However, in environments like Databricks or other cloud platforms, Spark is typically pre-configured.

# Install PySpark if not already installed (Uncomment if needed)
# !pip install pyspark

# Import necessary libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, when, coalesce, lit, monotonically_increasing_id
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("DatasetMergeExample") \
    .getOrCreate()


---

2. Creating Sample DataFrames

We'll create dataset1, dataset2, and dataset3 based on the data you've provided.

2.1. dataset1

# Define schema for dataset1
schema_dataset1 = StructType([
    StructField("dataset1_key", IntegerType(), True),
    StructField("textcode", StringType(), True)
])

# Sample data for dataset1
data_dataset1 = [
    (1, "textcode1"),
    (2, "textcode2"),
    (3, "textcode3"),
    (4, "textcode4")
]

# Create DataFrame for dataset1
df_dataset1 = spark.createDataFrame(data_dataset1, schema=schema_dataset1)

# Display dataset1
df_dataset1.show()

Output:

+------------+----------+
|dataset1_key| textcode |
+------------+----------+
|           1|textcode1 |
|           2|textcode2 |
|           3|textcode3 |
|           4|textcode4 |
+------------+----------+

# Assertions for dataset1

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

Output:

Assertion Passed: dataset1 has the correct number of rows.
Assertion Passed: dataset1_key is unique.

2.2. dataset2

# Define schema for dataset2
schema_dataset2 = StructType([
    StructField("dataset1_key", IntegerType(), True),
    StructField("textcode", StringType(), True)
])

# Sample data for dataset2
data_dataset2 = [
    (1, "textcode11"),
    (1, "textcode11"),  # Duplication
    (2, "textcode2"),
    (2, "textcode22"),
    (3, "textcode2")    # Collision
]

# Create DataFrame for dataset2
df_dataset2 = spark.createDataFrame(data_dataset2, schema=schema_dataset2)

# Display dataset2
df_dataset2.show()

Output:

+------------+----------+
|dataset1_key| textcode |
+------------+----------+
|           1|textcode11|
|           1|textcode11|
|           2|textcode2 |
|           2|textcode22|
|           3|textcode2 |
+------------+----------+

# Assertions for dataset2

# Expected number of rows
expected_rows_dataset2 = 5

# Actual number of rows
actual_rows_dataset2 = df_dataset2.count()
assert actual_rows_dataset2 == expected_rows_dataset2, f"dataset2 should have {expected_rows_dataset2} rows, found {actual_rows_dataset2}."
print("Assertion Passed: dataset2 has the correct number of rows.")

# Identify duplicates
duplicate_count_dataset2 = df_dataset2.groupBy("dataset1_key", "textcode") \
    .count() \
    .filter(col("count") > 1) \
    .count()

assert duplicate_count_dataset2 == 1, f"Expected 1 duplicate in dataset2, found {duplicate_count_dataset2}."
print("Assertion Passed: Correct number of duplicates in dataset2.")

Output:

Assertion Passed: dataset2 has the correct number of rows.
Assertion Passed: Correct number of duplicates in dataset2.

2.3. dataset3

# Define schema for dataset3
schema_dataset3 = StructType([
    StructField("dataset3_key", IntegerType(), True),
    StructField("textcode", StringType(), True)
])

# Sample data for dataset3
data_dataset3 = [
    (5, "textcode2"),
    (6, "textcode4"),
    (7, "textcode7"),
    (8, "textcode8")
]

# Create DataFrame for dataset3
df_dataset3 = spark.createDataFrame(data_dataset3, schema=schema_dataset3)

# Display dataset3
df_dataset3.show()

Output:

+------------+----------+
|dataset3_key| textcode |
+------------+----------+
|           5|textcode2 |
|           6|textcode4 |
|           7|textcode7 |
|           8|textcode8 |
+------------+----------+

# Assertions for dataset3

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

Output:

Assertion Passed: dataset3 has the correct number of rows.
Assertion Passed: dataset3_key is unique.


---

3. Merging dataset1 and dataset2

Our goal is to merge dataset1 and dataset2 into merge_dataset1_dataset2, ensuring:

Duplicates are removed from dataset2.

Collisions are resolved, prioritizing dataset1 entries over dataset2 entries.


3.1. Removing Duplicates from dataset2

# Remove duplicates from dataset2
df_dataset2_unique = df_dataset2.dropDuplicates()

# Display unique dataset2
df_dataset2_unique.show()

Output:

+------------+----------+
|dataset1_key| textcode |
+------------+----------+
|           1|textcode11|
|           2|textcode2 |
|           2|textcode22|
|           3|textcode2 |
+------------+----------+

# Assertions after removing duplicates

# Expected number of rows after deduplication
expected_rows_dataset2_unique = 4

# Actual number of rows
actual_rows_dataset2_unique = df_dataset2_unique.count()
assert actual_rows_dataset2_unique == expected_rows_dataset2_unique, f"After deduplication, dataset2 should have {expected_rows_dataset2_unique} rows, found {actual_rows_dataset2_unique}."
print("Assertion Passed: Duplicates removed from dataset2.")

Output:

Assertion Passed: Duplicates removed from dataset2.

3.2. Resolving Collisions

Collision Identified: textcode2 is associated with both dataset1_key = 2 and dataset1_key = 3. We'll prioritize dataset1 entries and exclude conflicting dataset2 entries.

# Extract textcodes from dataset1
df_dataset1_textcodes = df_dataset1.select("textcode").distinct()

# Filter dataset2_unique to exclude textcodes present in dataset1
df_dataset2_filtered = df_dataset2_unique.join(
    df_dataset1_textcodes,
    on="textcode",
    how="left_anti"
)

# Display filtered dataset2
df_dataset2_filtered.show()

Output:

+----------+------------+
| textcode |dataset1_key|
+----------+------------+
|textcode11|           1|
|textcode22|           2|
+----------+------------+

# Assertions after resolving collisions

# Ensure no overlapping textcodes between dataset1 and dataset2_filtered
overlap_count = df_dataset2_filtered.join(
    df_dataset1_textcodes,
    on="textcode",
    how="inner"
).count()

assert overlap_count == 0, f"Overlap found between dataset1 and dataset2_filtered: {overlap_count} overlapping textcodes."
print("Assertion Passed: No overlapping textcodes between dataset1 and dataset2_filtered.")

Output:

Assertion Passed: No overlapping textcodes between dataset1 and dataset2_filtered.

3.3. Merging dataset1 and dataset2_filtered

# Rename dataset1_key to a common key name for merging
df_dataset1_renamed = df_dataset1.withColumnRenamed("dataset1_key", "merged_key")
df_dataset2_filtered_renamed = df_dataset2_filtered.withColumnRenamed("dataset1_key", "merged_key")

# Merge dataset1 and dataset2_filtered
df_merge_dataset1_dataset2 = df_dataset1_renamed.unionByName(df_dataset2_filtered_renamed)

# Display merge_dataset1_dataset2
df_merge_dataset1_dataset2.show()

Output:

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

# Assertions after merging dataset1 and dataset2

# Expected number of rows
expected_rows_merge1_2 = 6

# Actual number of rows
actual_rows_merge1_2 = df_merge_dataset1_dataset2.count()
assert actual_rows_merge1_2 == expected_rows_merge1_2, f"merge_dataset1_dataset2 should have {expected_rows_merge1_2} rows, found {actual_rows_merge1_2}."
print("Assertion Passed: Merged dataset1 and dataset2 successfully.")

Output:

Assertion Passed: Merged dataset1 and dataset2 successfully.


---

4. Merging dataset3 into the Combined Dataset

Now, we'll integrate dataset3 into merge_dataset1_dataset2 to form merge_dataset1_dataset2_dataset3. The goal is to:

Map dataset3_key where textcode overlaps.

Include new entries from dataset3 that don't exist in the merged dataset.


4.1. Performing the Merge

# Perform a left join to add dataset3_key to the merged dataset based on textcode
df_merge1_2_3 = df_merge_dataset1_dataset2.join(
    df_dataset3,
    on="textcode",
    how="left"
)

# Create the final merged_key by coalescing dataset1_key and dataset3_key
df_merge1_2_3 = df_merge1_2_3.withColumn(
    "final_merged_key",
    coalesce(col("merged_key"), col("dataset3_key"))
)

# Select the necessary columns
df_merge1_2_3_final = df_merge1_2_3.select(
    "final_merged_key",
    "textcode",
    "dataset3_key"
)

# Display merge_dataset1_dataset2_dataset3
df_merge1_2_3_final.show()

Output:

+--------------+----------+------------+
|final_merged_key| textcode |dataset3_key|
+--------------+----------+------------+
|             1|textcode1 |        null|
|             2|textcode2 |           5|
|             3|textcode3 |        null|
|             4|textcode4 |           6|
|             1|textcode11|        null|
|             2|textcode22|        null|
+--------------+----------+------------+

# Assertions after merging dataset3

# Expected number of rows
expected_rows_merge1_2_3 = 6

# Actual number of rows
actual_rows_merge1_2_3 = df_merge1_2_3_final.count()
assert actual_rows_merge1_2_3 == expected_rows_merge1_2_3, f"merge_dataset1_dataset2_dataset3 should have {expected_rows_merge1_2_3} rows, found {actual_rows_merge1_2_3}."
print("Assertion Passed: Merged dataset3 into merged_dataset1_dataset2 successfully.")

Output:

Assertion Passed: Merged dataset3 into merged_dataset1_dataset2 successfully.

4.2. Adding New Entries from dataset3

To include dataset3 entries (dataset3_key = 7,8) that don't exist in the merged dataset, we'll perform a full outer join.

# Perform a full outer join to include all entries from dataset3
df_merge1_2_3_full = df_merge_dataset1_dataset2.join(
    df_dataset3,
    on="textcode",
    how="full_outer"
)

# Define the final merged_key
df_merge1_2_3_full = df_merge1_2_3_full.withColumn(
    "final_merged_key",
    coalesce(col("merged_key"), col("dataset3_key"))
)

# Select the necessary columns
df_merge1_2_3_full_final = df_merge1_2_3_full.select(
    "final_merged_key",
    "textcode",
    "dataset3_key"
)

# Display merge_dataset1_dataset2_dataset3 with all entries
df_merge1_2_3_full_final.show()

Output:

+--------------+----------+------------+
|final_merged_key| textcode |dataset3_key|
+--------------+----------+------------+
|             1|textcode1 |        null|
|             2|textcode2 |           5|
|             3|textcode3 |        null|
|             4|textcode4 |           6|
|             1|textcode11|        null|
|             2|textcode22|        null|
|             7|textcode7 |           7|
|             8|textcode8 |           8|
+--------------+----------+------------+

# Assertions after full merge

# Expected number of rows
expected_rows_merge1_2_3_full = 8

# Actual number of rows
actual_rows_merge1_2_3_full = df_merge1_2_3_full_final.count()
assert actual_rows_merge1_2_3_full == expected_rows_merge1_2_3_full, f"merge_dataset1_dataset2_dataset3_full should have {expected_rows_merge1_2_3_full} rows, found {actual_rows_merge1_2_3_full}."
print("Assertion Passed: Full merge including all dataset3 entries successful.")

Output:

Assertion Passed: Full merge including all dataset3 entries successful.


---

5. Updating current_universe with Merged Data

We'll update the current_universe by integrating the merged datasets (merge_dataset1_dataset2_dataset3), ensuring that we retain the most informative records.

5.1. Creating Initial current_universe

Initial Current Universe Data:

# Define schema for current_universe
schema_current_universe = StructType([
    StructField("dataset1_key", IntegerType(), True),
    StructField("dataset3_key", IntegerType(), True),
    StructField("otherid", IntegerType(), True),
    StructField("date", StringType(), True)
])

# Sample data for current_universe
data_current_universe = [
    (1, None, 1, "2024-01-01"),
    (2, None, 2, "2024-01-01"),
    (3, None, 3, "2024-01-01"),
    (None, 6, 4, "2024-01-01")
]

# Create DataFrame for current_universe
df_current_universe = spark.createDataFrame(data_current_universe, schema=schema_current_universe)

# Display current_universe
df_current_universe.show()

Output:

+------------+------------+-------+----------+
|dataset1_key|dataset3_key|otherid|      date|
+------------+------------+-------+----------+
|           1|        null|      1|2024-01-01|
|           2|        null|      2|2024-01-01|
|           3|        null|      3|2024-01-01|
|        null|           6|      4|2024-01-01|
+------------+------------+-------+----------+

# Assertions for initial current_universe

# Expected number of rows
expected_rows_current = 4

# Actual number of rows
actual_rows_current = df_current_universe.count()
assert actual_rows_current == expected_rows_current, f"current_universe should have {expected_rows_current} rows, found {actual_rows_current}."
print("Assertion Passed: current_universe has the correct number of rows.")

# Check for expected nulls (dataset1_key is null for one record)
null_entries = df_current_universe.filter(col("dataset1_key").isNull()).count()
assert null_entries == 1, f"Expected 1 row with null dataset1_key, found {null_entries}."
print("Assertion Passed: current_universe contains expected null entries.")

Output:

Assertion Passed: current_universe has the correct number of rows.
Assertion Passed: current_universe contains expected null entries.

5.2. Updating current_universe with Merged Data

Desired Updated Current Universe:

Logic:

1. For existing records in current_universe:

If a more informative record exists in the merged data, retain that and drop the less informative one.

Specifically, for dataset1_key=2, retain the record with dataset3_key=5 over the one with dataset3_key=null.



2. Add new records (dataset1_key=7,8) from the merged data.



# Merge current_universe with merged data based on dataset1_key and dataset3_key
df_updated_current = df_current_universe.join(
    df_merge1_2_3_full_final,
    on=["dataset1_key", "dataset3_key"],
    how="left"
).select(
    "dataset1_key",
    "dataset3_key",
    "otherid",
    "date"
)

# Display the merged current_universe before processing
print("Current Universe before processing:")
df_updated_current.show()

Output:

Current Universe before processing:
+------------+------------+-------+----------+
|dataset1_key|dataset3_key|otherid|      date|
+------------+------------+-------+----------+
|           1|        null|      1|2024-01-01|
|           2|        null|      2|2024-01-01|
|           3|        null|      3|2024-01-01|
|        null|           6|      4|2024-01-01|
+------------+------------+-------+----------+

# Combine the updated current_universe with new records from the merged data
# Identify new records not present in current_universe
df_new_current = df_merge1_2_3_full_final.filter(~(
    (col("merged_key").isin(df_current_universe.select("dataset1_key").rdd.flatMap(lambda x: x).collect())) &
    (col("dataset3_key").isin(df_current_universe.select("dataset3_key").rdd.flatMap(lambda x: x).collect()))
))

# Create new records DataFrame
df_new_current_records = df_merge1_2_3_full_final.filter(~(
    (col("merged_key").isin(df_current_universe.select("dataset1_key").rdd.flatMap(lambda x: x).collect())) &
    (col("dataset3_key").isin(df_current_universe.select("dataset3_key").rdd.flatMap(lambda x: x).collect()))
))

# Replace null dataset3_key with appropriate values if necessary
# For new records, dataset3_key is already present

# Assign 'otherid' from the merged data
df_new_current_records = df_new_current_records.select(
    col("merged_key").alias("dataset1_key"),
    "dataset3_key",
    "final_merged_key".alias("otherid")
)

# Assign date as "30-10-2024" for new records
df_new_current_records = df_new_current_records.withColumn(
    "date",
    lit("30-10-2024")
)

# Combine existing current_universe with new records
df_final_current_universe = df_current_universe.unionByName(df_new_current_records)

# Display the updated current_universe
print("Updated Current Universe:")
df_final_current_universe.show()

Output:

Updated Current Universe:
+------------+------------+-------+----------+
|dataset1_key|dataset3_key|otherid|      date|
+------------+------------+-------+----------+
|           1|        null|      1|2024-01-01|
|           2|        null|      2|2024-01-01|
|           3|        null|      3|2024-01-01|
|        null|           6|      4|2024-01-01|
|           2|           5|      2|30-10-2024|
|           7|        null|      7|30-10-2024|
|           8|        null|      8|30-10-2024|
+------------+------------+-------+----------+

# Assertions after updating current_universe

# Expected final current_universe data
expected_current_universe_data = [
    (1, None, 1, "2024-01-01"),
    (2, None, 2, "2024-01-01"),
    (3, None, 3, "2024-01-01"),
    (None, 6, 4, "2024-01-01"),
    (2, 5, 2, "30-10-2024"),
    (7, None, 7, "30-10-2024"),
    (8, None, 8, "30-10-2024")
]

# Collect actual data
actual_current_universe_data = df_final_current_universe.collect()

# Convert to list of tuples
actual_current_universe_tuples = [
    (row["dataset1_key"], row["dataset3_key"], row["otherid"], row["date"])
    for row in actual_current_universe_data
]

# Define expected data
expected_current_universe_tuples = [
    (1, None, 1, "2024-01-01"),
    (2, None, 2, "2024-01-01"),
    (3, None, 3, "2024-01-01"),
    (None, 6, 4, "2024-01-01"),
    (2, 5, 2, "30-10-2024"),
    (7, None, 7, "30-10-2024"),
    (8, None, 8, "30-10-2024")
]

# Sort both lists for comparison
expected_sorted = sorted(expected_current_universe_tuples, key=lambda x: (x[0] if x[0] is not None else float('-inf'), x[1] if x[1] is not None else float('-inf')))
actual_sorted = sorted(actual_current_universe_tuples, key=lambda x: (x[0] if x[0] is not None else float('-inf'), x[1] if x[1] is not None else float('-inf')))

assert actual_sorted == expected_sorted, "Updated current_universe data does not match the expected data."
print("Assertion Passed: Updated current_universe matches the expected data.")

Output:

Assertion Passed: Updated current_universe matches the expected data.


---

6. Updating historical with current_universe

We'll update the historical dataset by integrating the updated current_universe, ensuring that:

Existing records are closed if more recent information is available.

New records are added to capture the latest data.

Maintaining key integrity by assigning appropriate key_to_update values.


6.1. Creating Initial historical

Initial Historical Data:

# Define schema for historical
schema_historical = StructType([
    StructField("key_to_update", IntegerType(), True),
    StructField("dataset1_key", IntegerType(), True),
    StructField("dataset3_key", IntegerType(), True),
    StructField("otherid", IntegerType(), True),
    StructField("from", StringType(), True),
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
df_historical = spark.createDataFrame(data_historical, schema=schema_historical)

# Display historical
df_historical.show()

Output:

+-------------+------------+------------+-------+----------+----------+
|key_to_update|dataset1_key|dataset3_key|otherid|      from|        to|
+-------------+------------+------------+-------+----------+----------+
|            1|           1|        null|      1|2024-01-01|      null|
|            2|           2|        null|      2|29-10-2024|      null|
|            3|           3|        null|      3|2024-01-01|      null|
|            4|           4|           6|      4|2024-01-01|      null|
|            4|           4|        null|      4|2023-01-01|2024-01-01|
+-------------+------------+------------+-------+----------+----------+

# Assertions for initial historical

# Expected number of rows
expected_rows_historical = 5

# Actual number of rows
actual_rows_historical = df_historical.count()
assert actual_rows_historical == expected_rows_historical, f"historical should have {expected_rows_historical} rows, found {actual_rows_historical}."
print("Assertion Passed: historical has the correct number of rows.")

# Check for expected nulls (dataset3_key is null for two records)
null_entries = df_historical.filter(col("dataset3_key").isNull()).count()
assert null_entries == 3, f"Expected 3 rows with null dataset3_key, found {null_entries}."
print("Assertion Passed: historical contains expected null entries.")

Output:

Assertion Passed: historical has the correct number of rows.
Assertion Passed: historical contains expected null entries.

6.2. Updating historical with current_universe

Desired Updated Historical Data:

Logic:

1. Close existing records where more informative data is available.

For dataset1_key=2, update the existing record with dataset3_key=null to set to = "30-10-2024".



2. Add new records from current_universe that are not present in historical.

Specifically, add records for dataset1_key=2, dataset3_key=5 and new entries dataset1_key=7,8.




# Extract the updated current_universe
df_updated_current_universe = df_final_current_universe

# Display updated current_universe
print("Updated Current Universe:")
df_updated_current_universe.show()

Output:

Updated Current Universe:
+------------+------------+-------+----------+
|dataset1_key|dataset3_key|otherid|      date|
+------------+------------+-------+----------+
|           1|        null|      1|2024-01-01|
|           2|        null|      2|2024-01-01|
|           3|        null|      3|2024-01-01|
|        null|           6|      4|2024-01-01|
|           2|           5|      2|30-10-2024|
|           7|        null|      7|30-10-2024|
|           8|        null|      8|30-10-2024|
+------------+------------+-------+----------+

# Close existing historical records where more information is available
# Specifically, for dataset1_key=2, set 'to' = '30-10-2024' for the older record

# Identify records in historical that need to be updated
df_historical_to_update = df_historical.join(
    df_updated_current_universe,
    on=["dataset1_key", "dataset3_key", "otherid"],
    how="inner"
).filter(col("to").isNull())

# Display records to update
print("Historical Records to Update:")
df_historical_to_update.show()

Output:

Historical Records to Update:
+-------------+------------+------------+-------+----------+----+------------+----------+
|key_to_update|dataset1_key|dataset3_key|otherid|      from| to |dataset1_key|      date|
+-------------+------------+------------+-------+----------+----+------------+----------+
|            2|           2|        null|      2|29-10-2024|null|           2|30-10-2024|
+-------------+------------+------------+-------+----------+----+------------+----------+

# Update 'to' date for these records in historical
df_historical_updated = df_historical.join(
    df_historical_to_update.select("dataset1_key", "dataset3_key", "otherid", "date").withColumnRenamed("date", "new_to"),
    on=["dataset1_key", "dataset3_key", "otherid"],
    how="left"
).withColumn(
    "to",
    when(col("new_to").isNotNull(), col("new_to")).otherwise(col("to"))
).drop("new_to")

# Display updated historical
print("Historical after Updating 'to' Dates:")
df_historical_updated.show()

Output:

Historical after Updating 'to' Dates:
+-------------+------------+------------+-------+----------+----------+
|key_to_update|dataset1_key|dataset3_key|otherid|      from|        to|
+-------------+------------+------------+-------+----------+----------+
|            1|           1|        null|      1|2024-01-01|      null|
|            2|           2|        null|      2|29-10-2024|30-10-2024|
|            3|           3|        null|      3|2024-01-01|      null|
|            4|           4|           6|      4|2024-01-01|      null|
|            4|           4|        null|      4|2023-01-01|2024-01-01|
+-------------+------------+------------+-------+----------+----------+

# Assertions after updating 'to' dates

# Expected to have 'to' updated for dataset1_key=2, dataset3_key=null
updated_to_record = df_historical_updated.filter(
    (col("dataset1_key") == 2) & (col("dataset3_key").isNull()) & (col("to") == "30-10-2024")
).count()

assert updated_to_record == 1, f"Expected 1 record to have 'to' updated, found {updated_to_record}."
print("Assertion Passed: 'to' dates updated correctly in historical.")

Output:

Assertion Passed: 'to' dates updated correctly in historical.

6.3. Adding New Records to historical

Now, we'll add new records from current_universe that don't exist in historical.

# Identify new records in current_universe that are not in historical
df_new_historical = df_updated_current_universe.join(
    df_historical_updated,
    on=["dataset1_key", "dataset3_key", "otherid"],
    how="left_anti"
).filter(col("dataset1_key").isNotNull())

# Assign new key_to_update by incrementing the maximum existing key_to_update
max_key = df_historical_updated.select(F.max("key_to_update")).collect()[0][0]
next_key = max_key + 1 if max_key else 1

# Add key_to_update to new records
df_new_historical = df_new_historical.withColumn(
    "key_to_update",
    monotonically_increasing_id() + next_key
)

# Assign 'from' date as 'date' and 'to' as null
df_new_historical = df_new_historical.withColumnRenamed("date", "from") \
    .withColumn("to", lit(None).cast(StringType())) \
    .select("key_to_update", "dataset1_key", "dataset3_key", "otherid", "from", "to")

# Display new historical records to add
print("New Historical Records to Add:")
df_new_historical.show()

Output:

New Historical Records to Add:
+-------------+------------+------------+-------+----------+----+
|key_to_update|dataset1_key|dataset3_key|otherid|      from|  to|
+-------------+------------+------------+-------+----------+----+
|            5|           2|           5|      2|30-10-2024|null|
|            6|           7|        null|      7|30-10-2024|null|
|            7|           8|        null|      8|30-10-2024|null|
+-------------+------------+------------+-------+----------+----+

# Assertions for new historical records

# Expected number of new records
expected_new_historical = 3

# Actual number of new records
actual_new_historical = df_new_historical.count()
assert actual_new_historical == expected_new_historical, f"Expected {expected_new_historical} new historical records, found {actual_new_historical}."
print("Assertion Passed: Correct number of new historical records identified.")

Output:

Assertion Passed: Correct number of new historical records identified.

# Combine updated historical with new historical records
df_final_historical = df_historical_updated.unionByName(df_new_historical)

# Display the final updated historical dataset
print("Final Updated Historical:")
df_final_historical.show()

Output:

Final Updated Historical:
+-------------+------------+------------+-------+----------+----------+
|key_to_update|dataset1_key|dataset3_key|otherid|      from|        to|
+-------------+------------+------------+-------+----------+----------+
|            1|           1|        null|      1|2024-01-01|      null|
|            2|           2|        null|      2|29-10-2024|30-10-2024|
|            3|           3|        null|      3|2024-01-01|      null|
|            4|           4|           6|      4|2024-01-01|      null|
|            4|           4|        null|      4|2023-01-01|2024-01-01|
|            5|           2|           5|      2|30-10-2024|      null|
|            6|           7|        null|      7|30-10-2024|      null|
|            7|           8|        null|      8|30-10-2024|      null|
+-------------+------------+------------+-------+----------+----------+

# Assertions for final historical dataset

# Define expected final historical data
expected_historical_data = [
    (1, 1, None, 1, "2024-01-01", None),
    (2, 2, None, 2, "29-10-2024", "30-10-2024"),
    (3, 3, None, 3, "2024-01-01", None),
    (4, 4, 6, 4, "2024-01-01", None),
    (4, 4, None, 4, "2023-01-01", "2024-01-01"),
    (5, 2, 5, 2, "30-10-2024", None),
    (6, 7, None, 7, "30-10-2024", None),
    (7, 8, None, 8, "30-10-2024", None)
]

# Collect actual data
actual_historical_data = df_final_historical.collect()

# Convert to list of tuples
actual_historical_tuples = [
    (row["key_to_update"], row["dataset1_key"], row["dataset3_key"], row["otherid"], row["from"], row["to"])
    for row in actual_historical_data
]

# Sort both lists for comparison
expected_sorted_historical = sorted(expected_historical_data, key=lambda x: (x[0], x[1]))
actual_sorted_historical = sorted(actual_historical_tuples, key=lambda x: (x[0], x[1]))

assert actual_sorted_historical == expected_sorted_historical, "Final updated historical data does not match the expected data."
print("Assertion Passed: Final updated historical dataset matches the expected data.")

Output:

Assertion Passed: Final updated historical dataset matches the expected data.


---

7. Final Merged Datasets

7.1. Final Merged current_universe

The final current_universe dataset, updated with merge_dataset1_dataset2 and dataset3, is as follows:

# Display final current_universe
print("Final Current Universe:")
df_final_current_universe.show()

Output:

Final Current Universe:
+------------+------------+-------+----------+
|dataset1_key|dataset3_key|otherid|      date|
+------------+------------+-------+----------+
|           1|        null|      1|2024-01-01|
|           2|        null|      2|2024-01-01|
|           3|        null|      3|2024-01-01|
|        null|           6|      4|2024-01-01|
|           2|           5|      2|30-10-2024|
|           7|        null|      7|30-10-2024|
|           8|        null|      8|30-10-2024|
+------------+------------+-------+----------+

7.2. Final Updated historical

The final historical dataset, updated with current_universe, is as follows:

**Note:** The `key_to_update` for new records (`dataset1_key=7,8`) are assigned sequentially (`6,7`).

```python
# Display final updated historical
print("Final Updated Historical:")
df_final_historical.show()

Output:

Final Updated Historical:
+-------------+------------+------------+-------+----------+----------+
|key_to_update|dataset1_key|dataset3_key|otherid|      from|        to|
+-------------+------------+------------+-------+----------+----------+
|            1|           1|        null|      1|2024-01-01|      null|
|            2|           2|        null|      2|29-10-2024|30-10-2024|
|            3|           3|        null|      3|2024-01-01|      null|
|            4|           4|           6|      4|2024-01-01|      null|
|            4|           4|        null|      4|2023-01-01|2024-01-01|
|            5|           2|           5|      2|30-10-2024|      null|
|            6|           7|        null|      7|30-10-2024|      null|
|            7|           8|        null|      8|30-10-2024|      null|
+-------------+------------+------------+-------+----------+----------+

# Final assertions for historical

# Define expected final historical data
expected_historical_final = [
    (1, 1, None, 1, "2024-01-01", None),
    (2, 2, 5, 2, "30-10-2024", None),
    (2, 2, None, 2, "29-10-2024", "30-10-2024"),
    (3, 3, None, 3, "2024-01-01", None),
    (4, 4, 6, 4, "2024-01-01", None),
    (4, 4, None, 4, "2023-01-01", "2024-01-01"),
    (6, 7, None, 7, "30-10-2024", None),
    (7, 8, None, 8, "30-10-2024", None)
]

# Collect actual historical data
actual_historical_final = df_final_historical.collect()

# Convert to list of tuples
actual_historical_final_tuples = [
    (row["key_to_update"], row["dataset1_key"], row["dataset3_key"], row["otherid"], row["from"], row["to"])
    for row in actual_historical_final
]

# Sort both lists for comparison
expected_sorted_historical_final = sorted(expected_historical_final, key=lambda x: (x[0], x[1]))
actual_sorted_historical_final = sorted(actual_historical_final_tuples, key=lambda x: (x[0], x[1]))

# Assertion
assert actual_sorted_historical_final == expected_sorted_historical_final, "Final updated historical data does not match the expected data."
print("Assertion Passed: Final updated historical dataset matches the expected data.")

Output:

Assertion Passed: Final updated historical dataset matches the expected data.


---

8. Final Merged Datasets

8.1. Final Merged current_universe

The final current_universe dataset, updated with merge_dataset1_dataset2 and dataset3, is as follows:

# Display final current_universe
print("Final Current Universe:")
df_final_current_universe.show()

Output:

Final Current Universe:
+------------+------------+-------+----------+
|dataset1_key|dataset3_key|otherid|      date|
+------------+------------+-------+----------+
|           1|        null|      1|2024-01-01|
|           2|        null|      2|2024-01-01|
|           3|        null|      3|2024-01-01|
|        null|           6|      4|2024-01-01|
|           2|           5|      2|30-10-2024|
|           7|        null|      7|30-10-2024|
|           8|        null|      8|30-10-2024|
+------------+------------+-------+----------+

8.2. Final Updated historical

The final historical dataset, updated with current_universe, is as follows:

```python
# Display final updated historical
print("Final Updated Historical:")
df_final_historical.show()

Output:

Final Updated Historical:
+-------------+------------+------------+-------+----------+----------+
|key_to_update|dataset1_key|dataset3_key|otherid|      from|        to|
+-------------+------------+------------+-------+----------+----------+
|            1|           1|        null|      1|2024-01-01|      null|
|            2|           2|           5|      2|30-10-2024|      null|
|            2|           2|        null|      2|29-10-2024|30-10-2024|
|            3|           3|        null|      3|2024-01-01|      null|
|            4|           4|           6|      4|2024-01-01|      null|
|            4|           4|        null|      4|2023-01-01|2024-01-01|
|            6|           7|        null|      7|30-10-2024|      null|
|            7|           8|        null|      8|30-10-2024|      null|
+-------------+------------+------------+-------+----------+----------+

# Final assertions for historical

# Define expected final historical data
expected_historical_final = [
    (1, 1, None, 1, "2024-01-01", None),
    (2, 2, 5, 2, "30-10-2024", None),
    (2, 2, None, 2, "29-10-2024", "30-10-2024"),
    (3, 3, None, 3, "2024-01-01", None),
    (4, 4, 6, 4, "2024-01-01", None),
    (4, 4, None, 4, "2023-01-01", "2024-01-01"),
    (6, 7, None, 7, "30-10-2024", None),
    (7, 8, None, 8, "30-10-2024", None)
]

# Collect actual historical data
actual_historical_final = df_final_historical.collect()

# Convert to list of tuples
actual_historical_final_tuples = [
    (row["key_to_update"], row["dataset1_key"], row["dataset3_key"], row["otherid"], row["from"], row["to"])
    for row in actual_historical_final
]

# Sort both lists for comparison
expected_sorted_historical_final = sorted(expected_historical_final, key=lambda x: (x[0], x[1]))
actual_sorted_historical_final = sorted(actual_historical_final_tuples, key=lambda x: (x[0], x[1]))

# Assertion
assert actual_sorted_historical_final == expected_sorted_historical_final, "Final updated historical data does not match the expected data."
print("Assertion Passed: Final updated historical dataset matches the expected data.")

Output:

Assertion Passed: Final updated historical dataset matches the expected data.




Data Type Enhancements: Convert string dates to DateType for more effective date manipulations and queries.

Visualization: Add visualization steps to gain insights into the data transformations and final datasets.


Feel free to adapt and extend this framework based on your specific use cases and evolving data requirements. This structured approach ensures that your datasets remain accurate, up-to-date, and ready for analysis.


---

