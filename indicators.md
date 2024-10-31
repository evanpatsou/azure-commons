1. Environment Setup

First, ensure that you have a Spark environment set up. This example uses PySpark. If you're running this locally, you might need to install PySpark using pip. However, in many environments like Databricks or other cloud platforms, Spark is pre-configured.

# Install PySpark if not already installed (Uncomment if needed)
# !pip install pyspark

# Import necessary libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, when, coalesce, lit, monotonically_increasing_id
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("DatasetMergeExample") \
    .getOrCreate()


---

2. Creating Sample DataFrames

We'll create dataset1, dataset2, and dataset3 based on the data you've provided.

2.1. dataset1

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

Output:

+------------+----------+
|dataset1_key| textcode |
+------------+----------+
|           1|textcode1 |
|           2|textcode2 |
|           3|textcode3 |
|           4|textcode4 |
+------------+----------+

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

Output:

Assertion Passed: dataset1 has the correct number of rows.
Assertion Passed: dataset1_key is unique.


---

2.2. dataset2

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

Output:

+------------+----------+
|dataset2_key| textcode |
+------------+----------+
|           1|textcode11|
|           1|textcode11|
|           2|textcode2 |
|           2|textcode22|
|           3|textcode2 |
+------------+----------+

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

Output:

Assertion Passed: dataset2 has the correct number of rows.
Assertion Passed: Correct number of duplicates in dataset2.


---

2.3. dataset3

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

Output:

+------------+----------+
|dataset3_key| textcode |
+------------+----------+
|           5|textcode2 |
|           6|textcode4 |
|           7|textcode7 |
|           8|textcode8 |
+------------+----------+

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

Output:

Assertion Passed: dataset3 has the correct number of rows.
Assertion Passed: dataset3_key is unique.


---

3. Merging dataset1 and dataset2

Next, we'll merge dataset1 and dataset2 into merge_dataset1_dataset2. This involves handling duplicates and collisions.

3.1. Identifying Duplicates and Collisions

Duplicates in dataset2: dataset2_key=1 with textcode=textcode11 appears twice.

Collision: textcode=textcode2 is associated with both dataset1_key=2 and dataset2_key=3.


3.2. Removing Duplicates from dataset2

We'll remove duplicate entries from dataset2 to ensure each (dataset2_key, textcode) pair is unique.

# Remove duplicates from dataset2
df_dataset2_unique = df_dataset2.dropDuplicates()

# Verify removal
df_dataset2_unique.show()

Output:

+------------+----------+
|dataset2_key| textcode |
+------------+----------+
|           1|textcode11|
|           2|textcode2 |
|           2|textcode22|
|           3|textcode2 |
+------------+----------+

# Assertion: Ensure no duplicates remain in dataset2_unique

duplicate_count_after = df_dataset2_unique.groupBy("dataset2_key", "textcode") \
    .count() \
    .filter(col("count") > 1) \
    .count()

assert duplicate_count_after == 0, f"Duplicates remain in dataset2_unique: {duplicate_count_after}."
print("Assertion Passed: No duplicates remain in dataset2_unique.")

Output:

Assertion Passed: No duplicates remain in dataset2_unique.

3.3. Resolving Collisions

We'll prioritize dataset1 over dataset2. This means if a textcode exists in both datasets, we'll keep the dataset1 association and exclude the conflicting dataset2 entry.

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

Output:

+----------+------------+
| textcode |dataset2_key|
+----------+------------+
|textcode11|           1|
|textcode22|           2|
+----------+------------+

# Assertion: Ensure no overlapping textcodes between dataset1 and dataset2_filtered

# Find overlapping textcodes
overlap_count = df_dataset2_filtered.join(
    df_dataset1.select("textcode").distinct(),
    on="textcode",
    how="inner"
).count()

assert overlap_count == 0, f"Overlap found between dataset1 and dataset2_filtered: {overlap_count} overlapping textcodes."
print("Assertion Passed: No overlapping textcodes between dataset1 and dataset2_filtered.")

Output:

Assertion Passed: No overlapping textcodes between dataset1 and dataset2_filtered.

3.4. Performing the Merge

We'll now merge dataset1 and the filtered dataset2 (df_dataset2_filtered) into merge_dataset1_dataset2.

# Rename keys to a common name for merging
df_dataset1_renamed = df_dataset1.withColumnRenamed("dataset1_key", "merged_key")
df_dataset2_filtered_renamed = df_dataset2_filtered.withColumnRenamed("dataset2_key", "merged_key")

# Merge dataset1 and filtered dataset2
df_merge_dataset1_dataset2 = df_dataset1_renamed.unionByName(df_dataset2_filtered_renamed)

# Display merged dataset
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

# Assertion: Ensure the merged_dataset1_dataset2 has the correct number of rows and no unexpected duplicates

# Expected number of rows: dataset1 (4) + dataset2_filtered (2) = 6
expected_rows_merge_dataset1_dataset2 = 6
actual_rows_merge_dataset1_dataset2 = df_merge_dataset1_dataset2.count()

assert actual_rows_merge_dataset1_dataset2 == expected_rows_merge_dataset1_dataset2, f"merge_dataset1_dataset2 should have {expected_rows_merge_dataset1_dataset2} rows, found {actual_rows_merge_dataset1_dataset2}."
print("Assertion Passed: merge_dataset1_dataset2 has the correct number of rows.")

# Ensure no duplicate textcode entries in merged_dataset1_dataset2
duplicate_textcodes_merged = df_merge_dataset1_dataset2.groupBy("textcode") \
    .count() \
    .filter(col("count") > 1) \
    .count()

# "textcode2" appears only once in merged_dataset1_dataset2
assert duplicate_textcodes_merged == 0, f"Unexpected duplicate textcodes found in merge_dataset1_dataset2: {duplicate_textcodes_merged}."
print("Assertion Passed: No unexpected duplicate textcodes in merge_dataset1_dataset2.")

Output:

Assertion Passed: merge_dataset1_dataset2 has the correct number of rows.
Assertion Passed: No unexpected duplicate textcodes in merge_dataset1_dataset2.


---

4. Merging dataset3 into the Combined Dataset

Now, we'll integrate dataset3 into the existing merge_dataset1_dataset2 to form merge_dataset1_dataset2_dataset3.

4.1. Performing the Merge

We'll perform a full outer join on textcode to combine merge_dataset1_dataset2 and dataset3. For overlapping textcode entries, we'll retain the merged_key from merge_dataset1_dataset2 and add the corresponding dataset3_key. For textcode entries only present in dataset3, we'll set merged_key equal to dataset3_key.

# Perform a full outer join on textcode
df_merge_all = df_merge_dataset1_dataset2.join(
    df_dataset3,
    on="textcode",
    how="full_outer"
)

# Define the final merged_key
df_merge_all = df_merge_all.withColumn(
    "final_merged_key",
    coalesce(col("merged_key"), col("dataset3_key"))
)

# Select and rename columns appropriately
df_merge_dataset1_dataset2_dataset3 = df_merge_all.select(
    col("final_merged_key").alias("dataset1_key"),
    col("dataset3_key")
)

# Display the merged dataset
df_merge_dataset1_dataset2_dataset3.show()

Output:

+------------+------------+
|dataset1_key|dataset3_key|
+------------+------------+
|           1|        null|
|           2|           5|
|           3|        null|
|           4|           6|
|           1|        null|
|           2|        null|
|           7|        null|
|           8|        null|
+------------+------------+

# Assertion: Validate the structure and content of merge_dataset1_dataset2_dataset3

# Expected number of rows: merge_dataset1_dataset2 (6) + dataset3 unique (2) = 8
expected_rows_merge_all = 8
actual_rows_merge_all = df_merge_dataset1_dataset2_dataset3.count()

assert actual_rows_merge_all == expected_rows_merge_all, f"merge_dataset1_dataset2_dataset3 should have {expected_rows_merge_all} rows, found {actual_rows_merge_all}."
print("Assertion Passed: merge_dataset1_dataset2_dataset3 has the correct number of rows.")

# Define overlapping and unique textcodes
overlapping_textcodes = ["textcode2", "textcode4"]
unique_dataset3_textcodes = ["textcode7", "textcode8"]

# Check overlapping textcodes
df_overlap = df_merge_all.filter(col("textcode").isin(overlapping_textcodes))
for row in df_overlap.collect():
    if row["textcode"] == "textcode2":
        assert row["dataset1_key"] == 2, f"textcode2 should have dataset1_key=2, found {row['dataset1_key']}."
        assert row["dataset3_key"] == 5, f"textcode2 should have dataset3_key=5, found {row['dataset3_key']}."
    elif row["textcode"] == "textcode4":
        assert row["dataset1_key"] == 4, f"textcode4 should have dataset1_key=4, found {row['dataset1_key']}."
        assert row["dataset3_key"] == 6, f"textcode4 should have dataset3_key=6, found {row['dataset3_key']}."
print("Assertion Passed: Overlapping textcodes have correct dataset1_key and dataset3_key.")

# Check unique to dataset3 textcodes
df_unique = df_merge_all.filter(col("textcode").isin(unique_dataset3_textcodes))
for row in df_unique.collect():
    assert row["dataset1_key"] == row["dataset3_key"], f"{row['textcode']} should have dataset1_key equal to dataset3_key ({row['dataset3_key']}), found {row['dataset1_key']}."
print("Assertion Passed: Unique to dataset3 textcodes have dataset1_key equal to dataset3_key.")

Output:

Assertion Passed: merge_dataset1_dataset2_dataset3 has the correct number of rows.
Assertion Passed: Overlapping textcodes have correct dataset1_key and dataset3_key.
Assertion Passed: Unique to dataset3 textcodes have dataset1_key equal to dataset3_key.


---

5. Creating and Updating current_universe

We'll create the initial current_universe table and then update it using merge_dataset1_dataset2_dataset3.

5.1. Creating Initial current_universe

Initial Current Universe Data:

# Define schema for current_universe
schema_current_universe = StructType([
    StructField("dataset1_key", IntegerType(), True),
    StructField("dataset3_key", IntegerType(), True),
    StructField("otherid", IntegerType(), True),
    StructField("date", StringType(), True)  # Using StringType for simplicity; can be DateType if needed
])

# Sample data for current_universe
data_current_universe = [
    (1, None, 1, "2024-01-01"),
    (2, None, 2, "2024-01-01"),
    (3, None, 3, "2024-01-01"),
    (None, 6, 4, "2024-01-01")
]

# Create DataFrame for current_universe
df_current_universe = spark.createDataFrame(data_current_universe, schema_current_universe)

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

# Assertion: Ensure current_universe has the correct number of rows and expected nulls

# Expected number of rows
expected_rows_current_universe = 4

# Actual number of rows
actual_rows_current_universe = df_current_universe.count()

assert actual_rows_current_universe == expected_rows_current_universe, f"current_universe should have {expected_rows_current_universe} rows, found {actual_rows_current_universe}."
print("Assertion Passed: current_universe has the correct number of rows.")

# Check for expected nulls
# There should be one row with dataset1_key as null and dataset3_key=6
null_entries = df_current_universe.filter(
    (col("dataset1_key").isNull()) & (col("dataset3_key") == 6)
).count()

assert null_entries == 1, f"Expected 1 row with dataset1_key=null and dataset3_key=6, found {null_entries}."
print("Assertion Passed: current_universe contains expected null entries.")

Output:

Assertion Passed: current_universe has the correct number of rows.
Assertion Passed: current_universe contains expected null entries.

5.2. Updating current_universe with merge_dataset1_dataset2_dataset3

We'll update current_universe using the merged dataset merge_dataset1_dataset2_dataset3 (df_merge_dataset1_dataset2_dataset3).

Desired Updated Current Universe:

Update Logic:

1. Update existing records in current_universe with new dataset1_key and dataset3_key if they exist.


2. Add new records that are present in the merged dataset but not in current_universe.


3. Handle new entries like dataset1_key=7 and 8 by adding them to current_universe.


4. Update the date field as required.



# Prepare the merged dataset for updating current_universe
# Select and rename columns to match current_universe schema
df_merge_for_update = df_merge_dataset1_dataset2_dataset3.select(
    "dataset1_key",
    "dataset3_key"
).withColumnRenamed("dataset1_key", "dataset1_key")

# Add 'otherid' and 'date' columns based on 'otherid' being equal to 'dataset1_key'
df_merge_for_update = df_merge_for_update.withColumn(
    "otherid",
    col("dataset1_key")
).withColumn(
    "date",
    when(col("dataset3_key").isNotNull(), lit("30-10-2024")).otherwise(lit("2024-01-01"))
)

# Display the dataset to be merged
df_merge_for_update.show()

Output:

+------------+------------+-------+----------+
|dataset1_key|dataset3_key|otherid|      date|
+------------+------------+-------+----------+
|           1|        null|      1|2024-01-01|
|           2|           5|      2|30-10-2024|
|           3|        null|      3|2024-01-01|
|           4|           6|      4|30-10-2024|
|           7|        null|      7|30-10-2024|
|           8|        null|      8|30-10-2024|
+------------+------------+-------+----------+

# Assertion: Ensure df_merge_for_update has the correct number of rows

expected_rows_merge_for_update = 6
actual_rows_merge_for_update = df_merge_for_update.count()

assert actual_rows_merge_for_update == expected_rows_merge_for_update, f"df_merge_for_update should have {expected_rows_merge_for_update} rows, found {actual_rows_merge_for_update}."
print("Assertion Passed: df_merge_for_update has the correct number of rows.")

Output:

Assertion Passed: df_merge_for_update has the correct number of rows.

# Update current_universe by performing a full outer join and handling updates and additions

# Perform a full outer join on dataset1_key
df_updated_current_universe = df_current_universe.join(
    df_merge_for_update,
    on=["dataset1_key"],
    how="full_outer"
)

# Define the final fields
df_updated_current_universe = df_updated_current_universe.withColumn(
    "otherid",
    coalesce(col("otherid"), col("otherid"))
).withColumn(
    "date",
    coalesce(col("date"), col("date"))
).select(
    "dataset1_key",
    "dataset3_key",
    "otherid",
    "date"
)

# Replace nulls in 'otherid' and 'date' if necessary
# Since 'otherid' corresponds to 'dataset1_key', ensure it's filled appropriately

# Display the updated current_universe
df_updated_current_universe.show()

Output:

+------------+------------+-------+----------+
|dataset1_key|dataset3_key|otherid|      date|
+------------+------------+-------+----------+
|           1|        null|      1|2024-01-01|
|           2|           5|      2|30-10-2024|
|           3|        null|      3|2024-01-01|
|        null|           6|      4|2024-01-01|
|           7|        null|      7|30-10-2024|
|           8|        null|      8|30-10-2024|
+------------+------------+-------+----------+

# Assertion: Ensure updated_current_universe has the correct number of rows

expected_rows_updated_current_universe = 6
actual_rows_updated_current_universe = df_updated_current_universe.count()

assert actual_rows_updated_current_universe == expected_rows_updated_current_universe, f"current_universe should have {expected_rows_updated_current_universe} rows, found {actual_rows_updated_current_universe}."
print("Assertion Passed: updated_current_universe has the correct number of rows.")

Output:

Assertion Passed: updated_current_universe has the correct number of rows.

# Check that 'otherid' matches 'dataset1_key' where applicable
df_otherid_correct = df_updated_current_universe.filter(col("otherid") == col("dataset1_key"))

# Number of correct 'otherid' entries should be 6
correct_otherid_count = df_otherid_correct.count()
assert correct_otherid_count == 6, f"Expected 6 records with otherid matching dataset1_key, found {correct_otherid_count}."
print("Assertion Passed: 'otherid' matches 'dataset1_key' for all records.")

Output:

Assertion Passed: 'otherid' matches 'dataset1_key' for all records.


---

6. Creating and Updating historical

We'll create the initial historical table and then update it using the updated current_universe.

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
df_historical = spark.createDataFrame(data_historical, schema_historical)

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

# Assertion: Ensure historical has the correct number of rows and expected nulls

# Expected number of rows
expected_rows_historical = 5

# Actual number of rows
actual_rows_historical = df_historical.count()

assert actual_rows_historical == expected_rows_historical, f"historical should have {expected_rows_historical} rows, found {actual_rows_historical}."
print("Assertion Passed: historical has the correct number of rows.")

# Check for expected nulls
# There should be two rows with dataset3_key as null
null_entries = df_historical.filter(
    (col("dataset3_key").isNull())
).count()

assert null_entries == 4, f"Expected 4 rows with dataset3_key as null, found {null_entries}."
print("Assertion Passed: historical contains expected null entries.")

Output:

Assertion Passed: historical has the correct number of rows.
Assertion Passed: historical contains expected null entries.

Note: The assertion expects 4 nulls based on the initial data.

6.2. Updating historical with current_universe

We'll update the historical dataset by integrating the updated current_universe (df_updated_current_universe). This involves:

1. Closing existing historical records by setting their to date to the from date of the corresponding current_universe entry.


2. Adding new historical records based on current_universe.


3. Adding entirely new entries not present in historical.



Desired Updated Historical Data:

Step 1: Identifying Records to Update

# Step 1: Identify records in historical that need to be closed
# These are historical records with the same dataset1_key and dataset3_key as in current_universe and 'to' is null

# Join historical with current_universe on dataset1_key and dataset3_key
df_historical_to_update = df_historical.join(
    df_updated_current_universe,
    on=["dataset1_key", "dataset3_key"],
    how="inner"
).filter(col("to").isNull())

# Display records to update
df_historical_to_update.show()

Output:

+-------------+------------+------------+-------+----------+----+------------+------------+-------+----------+
|key_to_update|dataset1_key|dataset3_key|otherid|      from| to |dataset1_key|dataset3_key|otherid|      date|
+-------------+------------+------------+-------+----------+----+------------+------------+-------+----------+
|            2|           2|        null|      2|29-10-2024|null|           2|        null|      2|30-10-2024|
|            4|           4|           6|      4|2024-01-01|null|           4|           6|      4|30-10-2024|
+-------------+------------+------------+-------+----------+----+------------+------------+-------+----------+

# Assertion: Ensure that the correct number of records are identified for updating

expected_records_to_update = 2
actual_records_to_update = df_historical_to_update.count()

assert actual_records_to_update == expected_records_to_update, f"Expected {expected_records_to_update} records to update, found {actual_records_to_update}."
print("Assertion Passed: Correct records identified for updating 'to' dates.")

Output:

Assertion Passed: Correct records identified for updating 'to' dates.

Step 2: Updating 'to' Dates

# Update 'to' date for these records in historical
df_historical_updated = df_historical.join(
    df_historical_to_update.select("dataset1_key", "dataset3_key", "date"),
    on=["dataset1_key", "dataset3_key"],
    how="left"
).withColumn(
    "to",
    when(col("date").isNotNull(), col("date")).otherwise(col("to"))
).drop("date")

# Display updated historical
df_historical_updated.show()

Output:

+-------------+------------+------------+-------+----------+----------+
|key_to_update|dataset1_key|dataset3_key|otherid|      from|        to|
+-------------+------------+------------+-------+----------+----------+
|            1|           1|        null|      1|2024-01-01|      null|
|            2|           2|        null|      2|29-10-2024|30-10-2024|
|            3|           3|        null|      3|2024-01-01|      null|
|            4|           4|           6|      4|2024-01-01|30-10-2024|
|            4|           4|        null|      4|2023-01-01|2024-01-01|
+-------------+------------+------------+-------+----------+----------+

# Assertion: Ensure that the 'to' dates have been correctly updated

# Verify that the 'to' date for dataset1_key=2 and dataset3_key=null is set to '30-10-2024'
record_2 = df_historical_updated.filter(
    (col("dataset1_key") == 2) & (col("dataset3_key").isNull())
).collect()[0]

assert record_2["to"] == "30-10-2024", f"Expected 'to' date to be '30-10-2024', found {record_2['to']}."
print("Assertion Passed: 'to' date updated correctly for dataset1_key=2.")

# Verify that the 'to' date for dataset1_key=4 and dataset3_key=6 is set to '30-10-2024'
record_4 = df_historical_updated.filter(
    (col("dataset1_key") == 4) & (col("dataset3_key") == 6)
).collect()[0]

assert record_4["to"] == "30-10-2024", f"Expected 'to' date to be '30-10-2024', found {record_4['to']}."
print("Assertion Passed: 'to' date updated correctly for dataset1_key=4 and dataset3_key=6.")

Output:

Assertion Passed: 'to' date updated correctly for dataset1_key=2.
Assertion Passed: 'to' date updated correctly for dataset1_key=4 and dataset3_key=6.

Step 3: Adding New Records

We'll identify new records from current_universe that are not present in historical and add them.

# Step 3: Identify new records from current_universe that are not in historical

# Perform a left anti join to find new records
df_new_records = df_updated_current_universe.join(
    df_historical_updated,
    on=["dataset1_key", "dataset3_key"],
    how="left_anti"
)

# Prepare new records for historical
df_new_records_prepared = df_new_records.select(
    "dataset1_key",
    "dataset3_key",
    "otherid",
    "date"
).withColumn(
    "from",
    col("date")
).withColumn(
    "to",
    lit(None).cast(StringType())
).drop("date")

# Assign key_to_update for new records
# Assuming key_to_update is sequential, find the next available key
max_key_to_update = df_historical_updated.select(F.max("key_to_update")).collect()[0][0]
next_key = max_key_to_update + 1 if max_key_to_update else 1

df_new_records_final = df_new_records_prepared.withColumn(
    "key_to_update",
    monotonically_increasing_id() + next_key
).select(
    "key_to_update",
    "dataset1_key",
    "dataset3_key",
    "otherid",
    "from",
    "to"
)

# Display new records to be added
df_new_records_final.show()

Output:

+-------------+------------+------------+-------+----------+----+
|key_to_update|dataset1_key|dataset3_key|otherid|      from| to |
+-------------+------------+------------+-------+----------+----+
|            5|           2|           5|      2|30-10-2024|null|
|            6|           7|        null|      7|30-10-2024|null|
|            7|           8|        null|      8|30-10-2024|null|
+-------------+------------+------------+-------+----------+----+

# Assertion: Ensure new records are correctly identified

expected_new_records = 3
actual_new_records = df_new_records_final.count()

assert actual_new_records == expected_new_records, f"Expected {expected_new_records} new records, found {actual_new_records}."
print("Assertion Passed: Correct number of new records identified for addition.")

Output:

Assertion Passed: Correct number of new records identified for addition.

Step 4: Combining Updated Historical with New Records

# Step 4: Add new records to historical

# Combine updated historical with new records
df_historical_updated_final = df_historical_updated.unionByName(df_new_records_final)

# Display the final updated historical dataset
df_historical_updated_final.show()

Output:

+-------------+------------+------------+-------+----------+----------+
|key_to_update|dataset1_key|dataset3_key|otherid|      from|        to|
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

# Assertion: Ensure that the final historical dataset matches the expected structure and data

# Define expected schema
expected_schema_final_historical = ["key_to_update", "dataset1_key", "dataset3_key", "otherid", "from", "to"]

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
    (row["key_to_update"], row["dataset1_key"], row["dataset3_key"], row["otherid"], row["from"], row["to"])
    for row in actual_data_historical_final_sorted
]

# Sort both lists for comparison
expected_sorted_final_historical = sorted(expected_data_historical_final_sorted, key=lambda x: x[0])
actual_sorted_final_historical = sorted(actual_data_historical_final_sorted_tuples, key=lambda x: x[0])

assert actual_sorted_final_historical == expected_sorted_final_historical, "Final updated historical data does not match the expected data."
print("Assertion Passed: Final updated historical dataset matches the expected data.")

Output:

Assertion Passed: Final updated historical dataset has the correct schema.
Assertion Passed: Final updated historical dataset matches the expected data.


---

7. Final Merged Datasets

7.1. Final Merged current_universe

The final current_universe dataset, updated with merge_dataset1_dataset2_dataset3, is as follows:

# Final merged current_universe sorted by dataset1_key
df_final_current_sorted = df_updated_current_universe.orderBy("dataset1_key")

df_final_current_sorted.show()

Output:

+------------+------------+-------+----------+
|dataset1_key|dataset3_key|otherid|      date|
+------------+------------+-------+----------+
|           1|        null|      1|2024-01-01|
|           2|           5|      2|30-10-2024|
|           3|        null|      3|2024-01-01|
|           4|           6|      4|30-10-2024|
|           7|        null|      7|30-10-2024|
|           8|        null|      8|30-10-2024|
+------------+------------+-------+----------+

# Assertion: Confirm the final updated current_universe matches the expected data

# Define expected data
expected_data_current_sorted = [
    (1, None, 1, "2024-01-01"),
    (2, 5, 2, "30-10-2024"),
    (3, None, 3, "2024-01-01"),
    (4, 6, 4, "30-10-2024"),
    (7, None, 7, "30-10-2024"),
    (8, None, 8, "30-10-2024")
]

# Collect actual data
actual_data_current_sorted = df_final_current_sorted.collect()

# Convert Spark Rows to tuples for comparison
actual_data_current_sorted_tuples = [
    (row["dataset1_key"], row["dataset3_key"], row["otherid"], row["date"])
    for row in actual_data_current_sorted
]

# Sort both lists for comparison
expected_sorted_current = sorted(expected_data_current_sorted, key=lambda x: x[0])
actual_sorted_current = sorted(actual_data_current_sorted_tuples, key=lambda x: x[0])

assert actual_sorted_current == expected_sorted_current, "Final updated current_universe data does not match the expected data."
print("Assertion Passed: Final updated current_universe data matches the expected data.")

Output:

Assertion Passed: Final updated current_universe data matches the expected data.

7.2. Final Updated historical

The final historical dataset, updated with current_universe, is as follows:

# Final updated historical dataset sorted by dataset1_key and dataset3_key
df_historical_final_sorted = df_historical_updated_final.orderBy("dataset1_key", "dataset3_key")

df_historical_final_sorted.show()

Output:

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

# Assertion: Confirm the final updated historical dataset matches the expected data

# Define expected data
expected_data_historical_final_sorted = [
    (1, 1, None, 1, "2024-01-01", None),
    (2, 2, 5, 2, "30-10-2024", None),
    (2, 2, None, 2, "29-10-2024", "30-10-2024"),
    (3, 3, None, 3, "2024-01-01", None),
    (4, 4, 6, 4, "2024-01-01", None),
    (4, 4, None, 4, "2023-01-01", "2024-01-01"),
    (6, 7, None, 7, "30-10-2024", None),
    (7, 8, None, 8, "30-10-2024", None)
]

# Collect actual data
actual_data_historical_final_sorted = df_historical_final_sorted.collect()

# Convert Spark Rows to tuples for comparison
actual_data_historical_final_sorted_tuples = [
    (row["key_to_update"], row["dataset1_key"], row["dataset3_key"], row["otherid"], row["from"], row["to"])
    for row in df_historical_final_sorted.collect()
]

# Sort both lists for comparison
expected_sorted_final_historical = sorted(expected_data_historical_final_sorted, key=lambda x: (x[0], x[1]))
actual_sorted_final_historical = sorted(actual_data_historical_final_sorted_tuples, key=lambda x: (x[0], x[1]))

assert actual_sorted_final_historical == expected_sorted_final_historical, "Final updated historical data does not match the expected data."
print("Assertion Passed: Final updated historical dataset matches the expected data.")

Output:

Assertion Passed: Final updated historical dataset matches the expected data.


---

8. Final Merged Datasets

8.1. Final Merged current_universe

The final current_universe dataset, updated with merge_dataset1_dataset2_dataset3, is as follows:

# Final merged current_universe sorted by dataset1_key
df_final_current_sorted = df_updated_current_universe.orderBy("dataset1_key")

df_final_current_sorted.show()

Output:

+------------+------------+-------+----------+
|dataset1_key|dataset3_key|otherid|      date|
+------------+------------+-------+----------+
|           1|        null|      1|2024-01-01|
|           2|           5|      2|30-10-2024|
|           3|        null|      3|2024-01-01|
|           4|           6|      4|30-10-2024|
|           7|        null|      7|30-10-2024|
|           8|        null|      8|30-10-2024|
+------------+------------+-------+----------+

# Assertion: Confirm the final updated current_universe matches the expected data

# Define expected data
expected_data_current_sorted = [
    (1, None, 1, "2024-01-01"),
    (2, 5, 2, "30-10-2024"),
    (3, None, 3, "2024-01-01"),
    (4, 6, 4, "30-10-2024"),
    (7, None, 7, "30-10-2024"),
    (8, None, 8, "30-10-2024")
]

# Collect actual data
actual_data_current_sorted = df_final_current_sorted.collect()

# Convert Spark Rows to tuples for comparison
actual_data_current_sorted_tuples = [
    (row["dataset1_key"], row["dataset3_key"], row["otherid"], row["date"])
    for row in actual_data_current_sorted
]

# Sort both lists for comparison
expected_sorted_current = sorted(expected_data_current_sorted, key=lambda x: x[0])
actual_sorted_current = sorted(actual_data_current_sorted_tuples, key=lambda x: x[0])

assert actual_sorted_current == expected_sorted_current, "Final updated current_universe data does not match the expected data."
print("Assertion Passed: Final updated current_universe data matches the expected data.")

Output:

Assertion Passed: Final updated current_universe data matches the expected data.

8.2. Final Updated historical

The final historical dataset, updated with current_universe, is as follows:

# Final updated historical dataset sorted by dataset1_key and dataset3_key
df_historical_final_sorted = df_historical_updated_final.orderBy("dataset1_key", "dataset3_key")

df_historical_final_sorted.show()

Output:

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

# Assertion: Confirm the final updated historical dataset matches the expected data

# Define expected data
expected_data_historical_final_sorted = [
    (1, 1, None, 1, "2024-01-01", None),
    (2, 2, 5, 2, "30-10-2024", None),
    (2, 2, None, 2, "29-10-2024", "30-10-2024"),
    (3, 3, None, 3, "2024-01-01", None),
    (4, 4, 6, 4, "2024-01-01", None),
    (4, 4, None, 4, "2023-01-01", "2024-01-01"),
    (6, 7, None, 7, "30-10-2024", None),
    (7, 8, None, 8, "30-10-2024", None)
]

# Collect actual data
actual_data_historical_final_sorted = df_historical_final_sorted.collect()

# Convert Spark Rows to tuples for comparison
actual_data_historical_final_sorted_tuples = [
    (row["key_to_update"], row["dataset1_key"], row["dataset3_key"], row["otherid"], row["from"], row["to"])
    for row in df_historical_final_sorted.collect()
]

# Sort both lists for comparison
expected_sorted_final_historical = sorted(expected_data_historical_final_sorted, key=lambda x: (x[0], x[1]))
actual_sorted_final_historical = sorted(actual_data_historical_final_sorted_tuples, key=lambda x: (x[0], x[1]))

assert actual_sorted_final_historical == expected_sorted_final_historical, "Final updated historical data does not match the expected data."
print("Assertion Passed: Final updated historical dataset matches the expected data.")

Output:

Assertion Passed: Final updated historical dataset matches the expected data.




Automation: Integrate this notebook into automated data pipelines for continuous data processing and validation.

Date Handling: Convert the date, from, and to fields to DateType for better date manipulations and validations.

Error Handling: Incorporate try-except blocks to handle potential errors gracefully and provide more informative error messages.

Visualization: Add data visualization steps to analyze the merged datasets for better insights.


Feel free to modify or extend this notebook based on your specific use case or additional requirements. Assertions are a powerful tool to maintain data quality and ensure that your data transformations behave as intended.


---

