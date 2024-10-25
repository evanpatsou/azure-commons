## Import Libraries

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, when, lit, to_date, current_date, countDistinct, count, coalesce, lower
)
from pyspark.sql.window import Window
from functools import reduce

# For assertions
import sys
```

---

## Initialize Spark Session

```python
# Initialize Spark session
spark = SparkSession.builder.appName("HistoricalDataUpdate").getOrCreate()
```

---

## Step 1: Read and Prepare Datasets

### Dataset 1 (`dataset_1`)

```python
# Sample data for dataset_1
data1 = [
    (1, 'tc1', 'Name1'),
    (2, 'tc2', 'Name2'),
    (3, 'tc3', 'Name3'),
    (4, 'tc4', 'Name4'),
    (5, 'tc5', 'Name5')
]

columns = ['dataset1_id', 'textcode', 'name']
dataset_1 = spark.createDataFrame(data1, columns)
```

**Assertion:**

```python
# Assert that dataset_1 has non-null dataset1_id and textcode
assert dataset_1.filter(col('dataset1_id').isNull() | col('textcode').isNull()).count() == 0, \
    "dataset_1 should not have null values in 'dataset1_id' or 'textcode'"
```

---

### Dataset 2 (`dataset_2`)

```python
# Sample data for dataset_2 (additional textcodes for dataset_1)
data2 = [
    (1, 'tc1a', 'Name1a'),
    (2, 'tc2', 'Name2a'),         # Potential collision
    (3, 'tc3b', 'Name3b'),
    (5, 'tc5', 'Name5a'),         # Duplicate textcode with dataset_1
    (5, 'tc5b', 'Name5b'),
    (6, 'tc2', 'Name2b')          # Collision: tc2 associated with different dataset1_id
]

columns = ['dataset1_id', 'textcode', 'name']
dataset_2 = spark.createDataFrame(data2, columns)
```

**Assertion:**

```python
# Assert that dataset_2 has non-null dataset1_id and textcode
assert dataset_2.filter(col('dataset1_id').isNull() | col('textcode').isNull()).count() == 0, \
    "dataset_2 should not have null values in 'dataset1_id' or 'textcode'"
```

---

## Step 2: Correctly Handle Collisions Between `dataset_1` and `dataset_2`

### Identify Collisions Between `dataset_1` and `dataset_2`

```python
from pyspark.sql.functions import countDistinct

# Combine dataset_1 and dataset_2 to identify collisions across both datasets
combined_datasets = dataset_1.select('dataset1_id', 'textcode').unionByName(
    dataset_2.select('dataset1_id', 'textcode')
)

# Find textcodes associated with multiple dataset1_id across both datasets
textcode_counts = combined_datasets.groupBy('textcode').agg(countDistinct('dataset1_id').alias('id_count'))

# Identify colliding textcodes (textcodes associated with multiple dataset1_id)
colliding_textcodes = textcode_counts.filter(col('id_count') > 1).select('textcode')
```

**Explanation:**

- We combine `dataset_1` and `dataset_2` to consider all associations of `textcode` to `dataset1_id`.
- We group by `textcode` and count the distinct `dataset1_id`s.
- If a `textcode` is associated with more than one `dataset1_id`, it is considered a collision.

**Assertion:**

```python
# Assert that colliding_textcodes has expected columns
assert 'textcode' in colliding_textcodes.columns, "colliding_textcodes should have 'textcode' column"
```

### Exclude Colliding Textcodes from `dataset_2`

```python
# Exclude colliding textcodes from dataset_2
dataset_2_filtered = dataset_2.join(colliding_textcodes, on='textcode', how='left_anti')
```

**Assertion:**

```python
# Assert that dataset_2_filtered does not contain any colliding textcodes
colliding_textcodes_list = [row.textcode for row in colliding_textcodes.collect()]
assert dataset_2_filtered.filter(col('textcode').isin(colliding_textcodes_list)).count() == 0, \
    "dataset_2_filtered should not contain colliding textcodes"
```

**Explanation:**

- The assertion now passes because we correctly identified colliding textcodes across both `dataset_1` and `dataset_2`, and excluded them from `dataset_2_filtered`.
- In our sample data, `tc2` is associated with `dataset1_id` 2 in `dataset_1` and `dataset1_id` 6 in `dataset_2`, causing a collision.

### Enhance `dataset_1` with `dataset_2_filtered`

```python
# Combine dataset_1 and dataset_2_filtered
enhanced_dataset1 = dataset_1.unionByName(dataset_2_filtered.select('dataset1_id', 'textcode', 'name'))
```

**Assertion:**

```python
# Assert that enhanced_dataset1 has unique combinations of dataset1_id and textcode
enhanced_count = enhanced_dataset1.count()
enhanced_distinct_count = enhanced_dataset1.dropDuplicates(['dataset1_id', 'textcode']).count()
assert enhanced_count == enhanced_distinct_count, \
    "enhanced_dataset1 should not have duplicate combinations of 'dataset1_id' and 'textcode'"
```

---

## Step 3: Find Common `textcode`s Between Enhanced `dataset_1` and `dataset_3`

### Identify Common `textcode`s

```python
# Find common textcodes between enhanced_dataset1 and dataset_3
common_textcodes = enhanced_dataset1.select('textcode').intersect(dataset_3.select('textcode'))
```

**Assertion:**

```python
# Assert that common_textcodes are present in both datasets
assert common_textcodes.count() > 0, "There should be common textcodes between enhanced_dataset1 and dataset_3"
```

### Determine One-to-One Mappings

```python
# Count occurrences in enhanced_dataset1
enhanced_dataset1_counts = enhanced_dataset1.groupBy('textcode').agg(count('dataset1_id').alias('dataset1_count'))

# Count occurrences in dataset_3
dataset3_counts = dataset_3.groupBy('textcode').agg(count('dataset3_id').alias('dataset3_count'))

# Join counts with common_textcodes
textcode_counts = common_textcodes.join(enhanced_dataset1_counts, on='textcode', how='inner') \
                                  .join(dataset3_counts, on='textcode', how='inner')

# Filter for textcodes where counts are both 1 (one-to-one mapping)
valid_textcodes = textcode_counts.filter(
    (col('dataset1_count') == 1) & (col('dataset3_count') == 1)
).select('textcode')
```

**Assertion:**

```python
# Assert that valid_textcodes are one-to-one mappings
assert valid_textcodes.count() > 0, "There should be valid one-to-one textcode mappings"
```

### Exclude Colliding Textcodes

```python
# Colliding textcodes are those not in valid_textcodes
colliding_textcodes = common_textcodes.join(valid_textcodes, on='textcode', how='left_anti')

# Exclude colliding textcodes from datasets
enhanced_dataset1_filtered = enhanced_dataset1.join(colliding_textcodes, on='textcode', how='left_anti')
dataset_3_filtered = dataset_3.join(colliding_textcodes, on='textcode', how='left_anti')
```

**Assertion:**

```python
# Assert that colliding textcodes are excluded from the datasets
colliding_textcodes_list = [row.textcode for row in colliding_textcodes.collect()]
assert enhanced_dataset1_filtered.filter(col('textcode').isin(colliding_textcodes_list)).count() == 0, \
    "enhanced_dataset1_filtered should not contain colliding textcodes"
assert dataset_3_filtered.filter(col('textcode').isin(colliding_textcodes_list)).count() == 0, \
    "dataset_3_filtered should not contain colliding textcodes"
```

---

## Step 4: Create `join_df` with All Identifiers from `dataset_1` and `dataset_3`

```python
# Full outer join on textcode to include all identifiers
join_df = enhanced_dataset1_filtered.alias('d1').join(
    dataset_3_filtered.alias('d3'),
    on='textcode',
    how='full_outer'
).select(
    col('d1.dataset1_id'),
    col('d3.dataset3_id'),
    col('d1.textcode'),
    col('d1.name').alias('name_dataset1'),
    col('d3.name').alias('name_dataset3')
)
```

**Assertion:**

```python
# Assert that join_df contains all unique dataset1_id and dataset3_id
unique_ids = join_df.select('dataset1_id', 'dataset3_id').distinct().count()
assert unique_ids > 0, "join_df should contain unique identifiers from both datasets"
```

