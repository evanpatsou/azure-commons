### 1. Environment Setup

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("DatasetEnhancementExample") \
    .getOrCreate()
```

---

### 2. Creating Sample DataFrames

#### 2.1. `dataset1`

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

#### 2.2. `dataset2`

```python
# Define schema for dataset2
schema2 = StructType([
    StructField("dataset1_key", IntegerType(), True),
    StructField("textcode", StringType(), True)
])

# Sample data for dataset2 with duplication and collision
data2 = [
    (1, "textcode11"),
    (1, "textcode11"),  # Duplication
    (2, "textcode2"),
    (2, "textcode22"),
    (3, "textcode2")    # Collision with dataset1_key 2 and textcode2
]

# Create DataFrame for dataset2
df_dataset2 = spark.createDataFrame(data2, schema2)

# Display dataset2
df_dataset2.show()
```

---

### 3. Identify and Remove Duplications in `dataset2`

```python
# Remove duplicate rows from dataset2
df_dataset2_no_duplicates = df_dataset2.dropDuplicates()

# Assertion to ensure no duplicates
duplicate_count = df_dataset2_no_duplicates.groupBy("dataset1_key", "textcode").count() \
    .filter(col("count") > 1).count()
assert duplicate_count == 0, "Duplicates still exist in dataset2_no_duplicates"
print("Assertion Passed: No duplicates in dataset2_no_duplicates.")
```

---

### 4. Identify and Remove Collisions

- A collision exists when multiple `textcode` values map to the same `dataset1_key`. We will identify such keys in `dataset2` and remove these colliding rows.

```python
# Identify keys with collisions
colliding_keys = df_dataset2_no_duplicates.groupBy("dataset1_key") \
    .agg(count("textcode").alias("textcode_count")) \
    .filter(col("textcode_count") > 1) \
    .select("dataset1_key")

# Exclude colliding keys from dataset2
df_dataset2_no_collisions = df_dataset2_no_duplicates.join(
    colliding_keys,
    on="dataset1_key",
    how="left_anti"
)

# Assertion to ensure no collisions: each key should now have exactly one `textcode`
collision_count = df_dataset2_no_collisions.groupBy("dataset1_key").agg(count("textcode").alias("textcode_count")) \
    .filter(col("textcode_count") > 1).count()
assert collision_count == 0, "Collisions still exist in dataset2_no_collisions"
print("Assertion Passed: No collisions in dataset2_no_collisions.")
```

---

### 5. Merge `dataset1` with the Cleaned `dataset2` to Create `enchanced_dataset1`

After cleaning duplications and collisions, we can merge `dataset1` with `dataset2_no_collisions`.

```python
# Perform union to merge dataset1 and cleaned dataset2
df_enhanced_dataset1 = df_dataset1.unionByName(df_dataset2_no_collisions)

# Display the enhanced dataset
df_enhanced_dataset1.show()
```

---

### 6. Assertions to Verify the Final Dataset (`enchanced_dataset1`)

#### 6.1. Ensure All Original Rows in `dataset1` are Present in `enchanced_dataset1`

```python
# Check that all rows from dataset1 are in enhanced_dataset1
dataset1_count = df_dataset1.count()
in_enhanced_count = df_enhanced_dataset1.join(df_dataset1, on=["dataset1_key", "textcode"], how="inner").count()
assert dataset1_count == in_enhanced_count, "Not all dataset1 rows are present in enhanced_dataset1"
print("Assertion Passed: All original rows from dataset1 are present in enhanced_dataset1.")
```

#### 6.2. Ensure No Duplicates in `enchanced_dataset1`

```python
# Check for duplicates in enhanced_dataset1
enhanced_duplicates_count = df_enhanced_dataset1.groupBy("dataset1_key", "textcode").count() \
    .filter(col("count") > 1).count()
assert enhanced_duplicates_count == 0, "Duplicates found in enhanced_dataset1"
print("Assertion Passed: No duplicates in enhanced_dataset1.")
```

#### 6.3. Ensure No Collisions in `enchanced_dataset1`

Each `dataset1_key` should map to only one `textcode`.

```python
# Check for collisions in enhanced_dataset1
enhanced_collision_count = df_enhanced_dataset1.groupBy("dataset1_key").agg(count("textcode").alias("textcode_count")) \
    .filter(col("textcode_count") > 1).count()
assert enhanced_collision_count == 0, "Collisions found in enhanced_dataset1"
print("Assertion Passed: No collisions in enhanced_dataset1.")
```

---

### Final Output of `enchanced_dataset1`

```python
# Show final enhanced dataset1
df_enhanced_dataset1.show()
```

### Expected Result for `enchanced_dataset1`

| dataset1_key | textcode  |
|--------------|-----------|
| 1            | textcode1 |
| 2            | textcode2 |
| 3            | textcode3 |
| 4            | textcode4 |
| 1            | textcode11|
| 2            | textcode22|