## **1. Setup and Initial DataFrames**

### Action: Import Libraries and Initialize Spark Session

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("DatasetMergeExample") \
    .getOrCreate()
```

### Action: Define Schemas and Create Sample DataFrames

#### `dataset1`

| dataset1_key | textcode  |
|--------------|-----------|
| 1            | textcode1 |
| 2            | textcode2 |
| 3            | textcode3 |
| 4            | textcode4 |
| 1            | textcode11 |
| 2            | textcode22 |

#### `dataset3`

| dataset3_key | textcode  |
|--------------|-----------|
| 5            | textcode2 |
| 6            | textcode4 |
| 7            | textcode7 |
| 8            | textcode8 |

```python
# Define schemas
schema1 = StructType([
    StructField("dataset1_key", IntegerType(), True),
    StructField("textcode", StringType(), True)
])

schema3 = StructType([
    StructField("dataset3_key", IntegerType(), True),
    StructField("textcode", StringType(), True)
])

# Sample data for dataset1 and dataset3
data1 = [
    (1, "textcode1"),
    (2, "textcode2"),
    (3, "textcode3"),
    (4, "textcode4"),
    (1, "textcode11"),
    (2, "textcode22")
]
data3 = [
    (5, "textcode2"),
    (6, "textcode4"),
    (7, "textcode7"),
    (8, "textcode8")
]

# Create DataFrames
df_dataset1 = spark.createDataFrame(data1, schema1)
df_dataset3 = spark.createDataFrame(data3, schema3)

# Show initial DataFrames
print("Initial dataset1:")
df_dataset1.show()
print("Initial dataset3:")
df_dataset3.show()
```

### Expected Output:
The tables displayed should match `dataset1` and `dataset3` as defined above.

---

## **2. Combine `dataset1` and `dataset3` by `textcode`**

### Action: Perform Outer Join on `textcode`

```python
# Outer join on textcode to combine dataset1 and dataset3
df_combined = df_dataset1.join(df_dataset3, on="textcode", how="outer")
print("Combined dataset (after outer join on textcode):")
df_combined.show()
```

### Expected Output:
The combined dataset should include all `textcode` values, along with `dataset1_key` and `dataset3_key` where available:

| textcode  | dataset1_key | dataset3_key |
|-----------|--------------|--------------|
| textcode1 | 1            | null         |
| textcode2 | 2            | 5            |
| textcode3 | 3            | null         |
| textcode4 | 4            | 6            |
| textcode11| 1            | null         |
| textcode22| 2            | null         |
| textcode7 | null         | 7            |
| textcode8 | null         | 8            |

---

## **3. Filter to Keep the Most Complete Rows**

### Action: Assign Row Priority and Retain Only the Most Complete Rows for Each `textcode`

- Rows with both `dataset1_key` and `dataset3_key` should have higher priority.
- Less complete rows will be filtered out automatically by prioritizing rows with both keys present.

```python
# Step 1: Assign priority based on the completeness of each row
df_prioritized = df_combined.withColumn(
    "row_priority", 
    when(col("dataset1_key").isNotNull() & col("dataset3_key").isNotNull(), 1)  # Both keys are present
    .otherwise(2)  # Only one key is present
)

# Step 2: Use a window function to retain only the most complete (highest priority) row for each textcode
window_spec = Window.partitionBy("textcode").orderBy("row_priority")
df_most_complete = df_prioritized.withColumn("row_num", F.row_number().over(window_spec)) \
    .filter(col("row_num") == 1) \
    .drop("row_priority", "row_num")

print("Filtered dataset (keeping only the most complete rows):")
df_most_complete.show()
```

### Expected Output:
This step should filter out redundant rows, leaving only one row per `textcode`, with priority given to rows containing both `dataset1_key` and `dataset3_key`.

| textcode  | dataset1_key | dataset3_key |
|-----------|--------------|--------------|
| textcode1 | 1            | null         |
| textcode2 | 2            | 5            |
| textcode3 | 3            | null         |
| textcode4 | 4            | 6            |
| textcode11| 1            | null         |
| textcode22| 2            | null         |
| textcode7 | null         | 7            |
| textcode8 | null         | 8            |

---

## **4. Create Final `dataset1_dataset3` by Selecting Only the Keys**

### Action: Select Only `dataset1_key` and `dataset3_key` Columns

After filtering, select only the key columns to form the final output.

```python
# Select only dataset1_key and dataset3_key columns to form the final dataset
df_dataset1_dataset3 = df_most_complete.select("dataset1_key", "dataset3_key").distinct()

print("Final dataset1_dataset3 (dynamically filtered):")
df_dataset1_dataset3.show()
```

### Expected Output:

The final `dataset1_dataset3` should retain only the unique and most complete rows, showing each unique `dataset1_key` or `dataset3_key` without duplicates.

| dataset1_key | dataset3_key |
|--------------|--------------|
| 1            | null         |
| 2            | 5            |
| 3            | null         |
| 4            | 6            |
| null         | 7            |
| null         | 8            |
