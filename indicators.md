```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit, current_date, coalesce, concat_ws, explode, array
from pyspark.sql.window import Window
from functools import reduce

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("CurrentAndHistoricalDataManagement") \
    .config("spark.sql.shuffle.partitions", "200") \
    .getOrCreate()

# Load current_df and another_table
# Replace with actual data loading logic

# For demonstration, we'll create sample DataFrames
current_df = spark.createDataFrame([
    ("A", "X1", "Value1"),
    ("B", "X2", "Value2"),
    ("C", "X3", "Value3")
], ["column1", "column2", "column3"])

another_table = spark.createDataFrame([
    ("K1", "A", "X1", "OldValue1", "2020-01-01", None),
    ("K2", "B", "X2", "Value2", "2020-01-01", None),
    ("K3", "D", "X4", "Value4", "2020-01-01", None)
], ["another_unique_key", "column1", "column2", "column3", "from_date", "to_date"])

# Step 2: Retrieve Active Entries
active_entries = another_table.filter(col("to_date").isNull())

# Step 3: Identify Common Columns and Create Composite Keys
common_columns = [col_name for col_name in current_df.columns if col_name in another_table.columns]

def create_partial_keys(df, columns):
    key_arrays = [concat_ws("_", col(c)).alias(f"key_{c}") for c in columns]
    return df.select("*", array(*key_arrays).alias("partial_keys"))

current_df = create_partial_keys(current_df, common_columns)
active_entries = create_partial_keys(active_entries, common_columns)

# Step 4: Explode Partial Keys and Join
current_df = current_df.withColumn("partial_key", explode(col("partial_keys")))
active_entries = active_entries.withColumn("partial_key", explode(col("partial_keys")))

# Repartition for performance
num_partitions = 200
current_df = current_df.repartition(num_partitions, "partial_key")
active_entries = active_entries.repartition(num_partitions, "partial_key")

# Join on partial_key
joined_df = current_df.alias("current").join(
    active_entries.alias("another"),
    on="partial_key",
    how="full_outer"
)

# Step 5: Detect Changes
compare_columns = [c for c in current_df.columns if c not in common_columns + ["partial_keys", "partial_key"]]
change_conditions = [
    (col(f"current.{col_name}") != col(f"another.{col_name}")) & col(f"current.{col_name}").isNotNull()
    for col_name in compare_columns
]
change_condition = reduce(lambda x, y: x | y, change_conditions)
records_to_update = joined_df.filter(change_condition)

# Step 6: Update another_table
# Close old records
updated_active_entries = active_entries.alias("a").join(
    records_to_update.select("another.another_unique_key"),
    on=col("a.another_unique_key") == col("another.another_unique_key"),
    how="left"
).withColumn(
    "to_date",
    when(col("another.another_unique_key").isNotNull(), current_date()).otherwise(col("a.to_date"))
).select("a.*")

# Insert new records
new_records = records_to_update.select(
    col("another.another_unique_key"),
    *[col(f"current.{c}") for c in current_df.columns if c != "partial_keys"],
    lit(current_date()).alias("from_date"),
    lit(None).cast("date").alias("to_date")
)

# Combine updated records
another_table_updated = updated_active_entries.unionByName(new_records)

# Step 7: Reattach Inactive Records
inactive_entries = another_table.filter(col("to_date").isNotNull())
final_another_table = another_table_updated.unionByName(inactive_entries)

# Step 8: Optimize for Large Datasets (already applied)

# Stop Spark Session
spark.stop()
```
---

## **1. Initialize Spark Session**

```python
from pyspark.sql import SparkSession

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("CurrentAndHistoricalDataManagement") \
    .config("spark.sql.shuffle.partitions", "200") \
    .getOrCreate()
```

**Assertion**:

```python
# Assert that Spark Session is active
assert spark.sparkContext is not None, "Spark Session was not created successfully."
```

---

## **2. Load DataFrames**

*Assuming that `current_df` and `another_table` are loaded as Spark DataFrames with the required columns.*

```python
# Load current_df and another_table
# Replace with actual data loading logic
# For this example, we assume they are already loaded
```

**Assertion**:

```python
# Assert that DataFrames have been loaded and have required columns
required_columns_current = set(["column1", "column2", "column3"])
required_columns_another = required_columns_current.union({"another_unique_key", "from_date", "to_date"})

assert all(col in current_df.columns for col in required_columns_current), "current_df is missing required columns."
assert all(col in another_table.columns for col in required_columns_another), "another_table is missing required columns."
```

---

## **3. Identify Common Columns**

```python
# Identify common columns between current_df and another_table
common_columns = [col_name for col_name in current_df.columns if col_name in another_table.columns]
```

**Assertion**:

```python
# Assert that there is at least one common column
assert len(common_columns) > 0, "No common columns found between current_df and another_table."
```

---

## **4. Retrieve Active Entries from `another_table`**

```python
from pyspark.sql.functions import col

# Retrieve only active entries (to_date IS NULL)
active_entries = another_table.filter(col("to_date").isNull())
```

**Assertion**:

```python
# Assert that all entries in active_entries have to_date as NULL
assert active_entries.filter(col("to_date").isNotNull()).count() == 0, "Active entries should have to_date as NULL."
```

---

## **5. Perform Left Join with OR Condition on Common Columns**

```python
from functools import reduce
from pyspark.sql.functions import col

# Build join condition: at least one common column matches
join_conditions = [current_df[c] == active_entries[c] for c in common_columns]
join_condition = reduce(lambda x, y: x | y, join_conditions)

# Perform left join using custom join condition
joined_df = current_df.alias("current").join(
    active_entries.alias("active"),
    on=join_condition,
    how="left"
)
```

**Assertion**:

```python
# Assert that the join has been performed and joined_df has expected columns
assert set(current_df.columns).issubset(joined_df.columns), "Joined DataFrame is missing columns from current_df."
assert 'another_unique_key' in joined_df.columns, "Joined DataFrame is missing 'another_unique_key' from active_entries."
```

---

## **6. Detect Changes**

```python
from pyspark.sql.functions import col, lit

# Columns to compare (excluding common columns)
compare_columns = [c for c in current_df.columns if c not in common_columns]

# Detect changes where values differ and current_df's value is not NULL
change_conditions = [
    (col(f"current.{col_name}") != col(f"active.{col_name}")) & col(f"current.{col_name}").isNotNull()
    for col_name in compare_columns
]

if change_conditions:
    change_condition = reduce(lambda x, y: x | y, change_conditions)
else:
    # If no columns to compare, set change_condition to False
    change_condition = lit(False)

# Records that need to be updated
records_to_update = joined_df.filter(change_condition)
```

**Assertion**:

```python
# Assert that records_to_update is a DataFrame
assert records_to_update is not None, "records_to_update DataFrame was not created."
# Assert that records_to_update contains only records where changes are detected
# Since we cannot check data, we ensure that the DataFrame exists and has the expected schema
```

---

## **7. Close Old Records in `another_table`**

```python
from pyspark.sql.functions import when, current_date

# Update to_date for old records
updated_active_entries = active_entries.alias("a").join(
    records_to_update.select(col("active.another_unique_key")).distinct(),
    on="another_unique_key",
    how="left"
).withColumn(
    "to_date",
    when(col("another_unique_key").isNotNull(), current_date()).otherwise(col("a.to_date"))
)
```

**Assertion**:

```python
# Assert that updated_active_entries is a DataFrame
assert updated_active_entries is not None, "updated_active_entries DataFrame was not created."
# Since we cannot check data, we ensure that the DataFrame has the 'to_date' column
assert 'to_date' in updated_active_entries.columns, "'to_date' column is missing in updated_active_entries."
```

---

## **8. Insert New Records into `another_table`**

```python
from pyspark.sql.functions import lit

# Prepare new records with updated values
new_records = records_to_update.select(
    col("active.another_unique_key"),
    *[col(f"current.{c}") for c in current_df.columns],
    lit(current_date()).alias("from_date"),
    lit(None).cast("date").alias("to_date")
)
```

**Assertion**:

```python
# Assert that new_records have 'from_date' and 'to_date' columns
assert 'from_date' in new_records.columns, "'from_date' column is missing in new_records."
assert 'to_date' in new_records.columns, "'to_date' column is missing in new_records."
```

---

## **9. Reattach Inactive Entries**

```python
# Get inactive entries from another_table
inactive_entries = another_table.filter(col("to_date").isNotNull())

# Combine updated active entries, new records, and inactive entries
final_another_table = updated_active_entries.unionByName(new_records, allowMissingColumns=True).unionByName(inactive_entries, allowMissingColumns=True)
```

**Assertion**:

```python
# Assert that final_another_table is a DataFrame
assert final_another_table is not None, "final_another_table DataFrame was not created."
# Assert that final_another_table contains all expected columns
expected_columns = set(another_table.columns)
assert expected_columns.issubset(final_another_table.columns), "final_another_table is missing expected columns."
```

---

## **10. Optimize for Large Datasets**

```python
# Optimization Steps:

# 1. Repartition DataFrames based on join keys (common columns)
num_partitions = 200  # Adjust based on cluster resources
current_df = current_df.repartition(num_partitions, *common_columns)
active_entries = active_entries.repartition(num_partitions, *common_columns)

# 2. Use broadcast join if one DataFrame is significantly smaller
from pyspark.sql.functions import broadcast

# Example: If current_df is small, broadcast it
if current_df.count() < 10000:  # Threshold can be adjusted
    joined_df = current_df.alias("current").join(
        broadcast(active_entries.alias("active")),
        on=join_condition,
        how="left"
    )

# 3. Cache intermediate DataFrames if reused
current_df.cache()
active_entries.cache()
```

**Assertion**:

```python
# Assert that DataFrames have been repartitioned
assert current_df.rdd.getNumPartitions() == num_partitions, f"current_df should have {num_partitions} partitions."
assert active_entries.rdd.getNumPartitions() == num_partitions, f"active_entries should have {num_partitions} partitions."
# Since we cannot verify caching or broadcasting in code without data, we ensure that the code for caching is present
```

---

## **11. Stop Spark Session**

```python
# Stop Spark Session
spark.stop()
```

**Assertion**:

```python
# Assert that the Spark session is stopped
assert spark._jsparkSession is None, "Spark session was not stopped properly."
```
