```python
from pyspark.sql.types import StructType, StructField, StringType, DateType

# Define schema for dataset1
dataset1_schema = StructType([
    StructField("dataset1_key", StringType(), True),
    StructField("textcode", StringType(), True),
    StructField("names", StringType(), True)
])

# Define schema for dataset2
dataset2_schema = StructType([
    StructField("dataset1_key", StringType(), True),
    StructField("textcode", StringType(), True)
])

# Define schema for dataset3
dataset3_schema = StructType([
    StructField("dataset3key", StringType(), True),
    StructField("name", StringType(), True),
    StructField("textcode", StringType(), True)
])

# Define schema for current_df
current_df_schema = StructType([
    StructField("dataset1_key", StringType(), True),
    StructField("dataset3key", StringType(), True),
    StructField("name", StringType(), True),
    StructField("someotherkey", StringType(), True),
    StructField("as_of_date", DateType(), True)
])
```

**Assertion**:

```python
# No assertion needed for schema definitions.
```

---

## **3. Create Datasets from Arrays**

```python
# Sample data for dataset1 (you can replace this with your actual data)
data1 = [
    # ... (Add your data here)
]

# Sample data for dataset2 (you can replace this with your actual data)
data2 = [
    # ... (Add your data here)
]

# Sample data for dataset3 (you can replace this with your actual data)
data3 = [
    # ... (Add your data here)
]

# Sample data for current_df (you can replace this with your actual data)
from datetime import date, timedelta

data_current = [
    # ... (Add your data here)
]

# Create DataFrames
dataset1 = spark.createDataFrame(data1, schema=dataset1_schema)
dataset2 = spark.createDataFrame(data2, schema=dataset2_schema)
dataset3 = spark.createDataFrame(data3, schema=dataset3_schema)
current_df = spark.createDataFrame(data_current, schema=current_df_schema)
```

**Assertion**:

```python
# Assert that DataFrames have been created and have the expected columns
assert set(dataset1_schema.fieldNames()).issubset(dataset1.columns), "dataset1 schema mismatch."
assert set(dataset2_schema.fieldNames()).issubset(dataset2.columns), "dataset2 schema mismatch."
assert set(dataset3_schema.fieldNames()).issubset(dataset3.columns), "dataset3 schema mismatch."
assert set(current_df_schema.fieldNames()).issubset(current_df.columns), "current_df schema mismatch."
```

---

## **4. Data Cleaning Functions**

```python
from pyspark.sql.functions import col, lower, trim, regexp_replace

# Data Cleaning Function for dataset1
def clean_dataset1(df):
    df_clean = df.dropna(subset=["dataset1_key", "textcode", "names"]) \
        .filter((col("dataset1_key") != "") & (col("textcode") != "") & (col("names") != "")) \
        .withColumn("dataset1_key", trim(col("dataset1_key"))) \
        .withColumn("textcode", regexp_replace(lower(trim(col("textcode"))), r"\s+", "")) \
        .withColumn("names", trim(col("names")))
    return df_clean

# Data Cleaning Function for dataset2
def clean_dataset2(df):
    df_clean = df.dropna(subset=["dataset1_key", "textcode"]) \
        .filter((col("dataset1_key") != "") & (col("textcode") != "")) \
        .withColumn("dataset1_key", trim(col("dataset1_key"))) \
        .withColumn("textcode", regexp_replace(lower(trim(col("textcode"))), r"\s+", ""))
    return df_clean

# Data Cleaning Function for dataset3
def clean_dataset3(df):
    df_clean = df.dropna(subset=["dataset3key", "name", "textcode"]) \
        .filter((col("dataset3key") != "") & (col("name") != "") & (col("textcode") != "")) \
        .withColumn("dataset3key", trim(col("dataset3key"))) \
        .withColumn("name", trim(col("name"))) \
        .withColumn("textcode", regexp_replace(lower(trim(col("textcode"))), r"\s+", ""))
    return df_clean
```

**Assertion**:

```python
# No assertion needed for function definitions.
```

---

## **5. Clean the Datasets**

```python
# Clean datasets
dataset1_clean = clean_dataset1(dataset1)
dataset2_clean = clean_dataset2(dataset2)
dataset3_clean = clean_dataset3(dataset3)
```

**Assertion**:

```python
# Assert that no nulls exist in required columns after cleaning
assert dataset1_clean.filter(
    col("dataset1_key").isNull() | col("textcode").isNull() | col("names").isNull()
).count() == 0, "Null values found in dataset1_clean."

assert dataset2_clean.filter(
    col("dataset1_key").isNull() | col("textcode").isNull()
).count() == 0, "Null values found in dataset2_clean."

assert dataset3_clean.filter(
    col("dataset3key").isNull() | col("name").isNull() | col("textcode").isNull()
).count() == 0, "Null values found in dataset3_clean."
```

---

## **6. Process Dataset2 and Merge with Dataset1**

```python
# Handle new 'dataset1_key' values in dataset2
existing_keys = dataset1_clean.select("dataset1_key").distinct()
dataset2_clean = dataset2_clean.join(existing_keys, on="dataset1_key", how="inner")

# Identify collisions in dataset2
from pyspark.sql.functions import countDistinct

collision_df = dataset2_clean.groupBy("textcode") \
    .agg(countDistinct("dataset1_key").alias("key_count")) \
    .filter(col("key_count") > 1) \
    .select("textcode")

# Remove collisions and duplicates from dataset2
dataset2_no_collisions = dataset2_clean.join(collision_df, on="textcode", how="left_anti") \
    .dropDuplicates(["dataset1_key", "textcode"])
```

**Assertion**:

```python
# Assert that collisions have been removed
assert dataset2_no_collisions.groupBy("textcode").agg(countDistinct("dataset1_key").alias("key_count")) \
    .filter(col("key_count") > 1).count() == 0, "Collisions still present in dataset2_no_collisions."

# Assert that no duplicates exist
assert dataset2_no_collisions.groupBy("dataset1_key", "textcode").count().filter(col("count") > 1).count() == 0, "Duplicates found in dataset2_no_collisions."
```

---

## **7. Create Enhanced Dataset1**

```python
# Enrich dataset2 with names from dataset1
key_name_mapping = dataset1_clean.select("dataset1_key", "names").distinct()
dataset2_with_names = dataset2_no_collisions.join(key_name_mapping, on="dataset1_key", how="left")

# Combine datasets
enhanced_dataset1 = dataset1_clean.unionByName(dataset2_with_names, allowMissingColumns=True) \
    .dropDuplicates(["dataset1_key", "textcode", "names"])
```

**Assertion**:

```python
# Assert that enhanced_dataset1 has the expected columns
expected_columns = ["dataset1_key", "textcode", "names"]
assert all(col in enhanced_dataset1.columns for col in expected_columns), "enhanced_dataset1 is missing expected columns."

# Assert that no duplicates exist in enhanced_dataset1
assert enhanced_dataset1.groupBy("dataset1_key", "textcode", "names").count().filter(col("count") > 1).count() == 0, "Duplicates found in enhanced_dataset1."
```

---

## **8. Merge Enhanced Dataset1 with Dataset3**

```python
from pyspark.sql.functions import broadcast

# Perform full outer join on 'textcode' using broadcast
left_df = enhanced_dataset1.select("dataset1_key", "textcode")
right_df = dataset3_clean.select("dataset3key", "name", "textcode")

# Decide which DataFrame to broadcast based on size
if left_df.count() <= right_df.count():
    left_df = broadcast(left_df)
else:
    right_df = broadcast(right_df)

unified_dataset = left_df.join(
    right_df,
    on="textcode",
    how="full_outer"
)
```

**Assertion**:

```python
# Assert that unified_dataset has the expected columns
expected_columns = ["dataset1_key", "dataset3key", "name", "textcode"]
assert all(col in unified_dataset.columns for col in expected_columns), "unified_dataset is missing expected columns."
```

---

## **9. Ensure All Keys are Present and Handle Nulls**

```python
# No placeholders needed; keep nulls
# Verify all 'dataset1_key's from enhanced_dataset1 are included
enhanced_keys_count = enhanced_dataset1.select("dataset1_key").distinct().count()
unified_dataset1_keys_count = unified_dataset.filter(col("dataset1_key").isNotNull()).select("dataset1_key").distinct().count()
assert enhanced_keys_count == unified_dataset1_keys_count, "Not all 'dataset1_key's are included in unified_dataset."

# Verify all 'dataset3key's from dataset3_clean are included
dataset3_keys_count = dataset3_clean.select("dataset3key").distinct().count()
unified_dataset3_keys_count = unified_dataset.filter(col("dataset3key").isNotNull()).select("dataset3key").distinct().count()
assert dataset3_keys_count == unified_dataset3_keys_count, "Not all 'dataset3key's are included in unified_dataset."
```

---

## **10. Update Current DataFrame (`current_df`) with Unified Dataset**

```python
from pyspark.sql.functions import when, current_date

# Perform full outer join on 'dataset1_key' and 'dataset3key'
merged_df = current_df.alias("cur").join(
    unified_dataset.alias("uni"),
    on=["dataset1_key", "dataset3key"],
    how="full_outer"
)

# Update or add columns
merged_df = merged_df.withColumn(
    "updated_name",
    when(merged_df.uni.name.isNotNull(), merged_df.uni.name)
    .otherwise(merged_df.cur.name)
)

merged_df = merged_df.withColumn(
    "updated_as_of_date",
    when(
        (merged_df.cur.name != merged_df.updated_name) |
        merged_df.cur.name.isNull() |
        merged_df.cur.as_of_date.isNull(),
        current_date()
    ).otherwise(merged_df.cur.as_of_date)
)

merged_df = merged_df.withColumn(
    "updated_someotherkey",
    when(merged_df.cur.someotherkey.isNotNull(), merged_df.cur.someotherkey)
    .otherwise(None)
)

# Create the updated current_df
updated_current_df = merged_df.select(
    "dataset1_key",
    "dataset3key",
    col("updated_name").alias("name"),
    col("updated_someotherkey").alias("someotherkey"),
    col("updated_as_of_date").alias("as_of_date")
)
```

**Assertion**:

```python
# Assert that updated_current_df has the expected columns
expected_columns = ["dataset1_key", "dataset3key", "name", "someotherkey", "as_of_date"]
assert all(col in updated_current_df.columns for col in expected_columns), "updated_current_df is missing expected columns."

# Assert that all keys from unified_dataset are present
unified_keys = unified_dataset.select("dataset1_key", "dataset3key").distinct()
updated_current_keys = updated_current_df.select("dataset1_key", "dataset3key").distinct()
missing_keys = unified_keys.exceptAll(updated_current_keys).count()
assert missing_keys == 0, "Not all keys from unified_dataset are present in updated_current_df."
```

---

## **11. Optimize for Small Dataset**

```python
# Since the dataset is small, avoid unnecessary repartitioning and caching
# Coalesce to a single partition if appropriate
updated_current_df = updated_current_df.coalesce(1)
```

**Assertion**:

```python
# Assert that the DataFrame has one partition
assert updated_current_df.rdd.getNumPartitions() == 1, "DataFrame should have one partition for small datasets."
```
