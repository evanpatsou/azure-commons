```python
# Cell 1: Initialize Spark Session and Define Schemas
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit, to_date, coalesce
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType

spark = SparkSession.builder \
    .appName("UpdateCurrentDFExample") \
    .getOrCreate()

schema_universe = StructType([
    StructField("dataset1_key", IntegerType(), True),
    StructField("textcode", StringType(), True),
    StructField("dataset3_key", IntegerType(), True)
])

schema_current = StructType([
    StructField("dataset1_key", IntegerType(), True),
    StructField("dataset3_key", IntegerType(), True),
    StructField("otherid", StringType(), True),
    StructField("date", DateType(), True)
])
```

```python
# Cell 2: Create DataFrames
data_universe = [
    (1, "textcode1", None),
    (2, "textcode2", 5),
    (3, "textcode3", None),
    (4, "textcode4", 6),
    (None, "textcode7", 7),
    (None, "textcode8", 8)
]

df_universe = spark.createDataFrame(data_universe, schema_universe)

data_current = [
    (1, None, "otherid1", to_date(lit("2024-01-01"), "yyyy-MM-dd")),
    (2, None, "otherid2", to_date(lit("2024-01-01"), "yyyy-MM-dd")),
    (3, None, "otherid3", to_date(lit("2024-01-01"), "yyyy-MM-dd")),
    (None, 6, "otherid4", to_date(lit("2024-01-01"), "yyyy-MM-dd"))
]

df_current = spark.createDataFrame(data_current, schema_current)
```

```python
# Cell 3: Assert Initial DataFrames
# Assert df_universe
expected_universe = spark.createDataFrame([
    (1, "textcode1", None),
    (2, "textcode2", 5),
    (3, "textcode3", None),
    (4, "textcode4", 6),
    (None, "textcode7", 7),
    (None, "textcode8", 8)
], schema_universe)

assert df_universe.subtract(expected_universe).count() == 0 and expected_universe.subtract(df_universe).count() == 0, "df_universe does not match expected data."

# Assert df_current
expected_current = spark.createDataFrame([
    (1, None, "otherid1", to_date(lit("2024-01-01"), "yyyy-MM-dd")),
    (2, None, "otherid2", to_date(lit("2024-01-01"), "yyyy-MM-dd")),
    (3, None, "otherid3", to_date(lit("2024-01-01"), "yyyy-MM-dd")),
    (None, 6, "otherid4", to_date(lit("2024-01-01"), "yyyy-MM-dd"))
], schema_current)

assert df_current.subtract(expected_current).count() == 0 and expected_current.subtract(df_current).count() == 0, "df_current does not match expected data."
```

```python
# Cell 4: Merge Universe Data on textcode
df_universe_combined = df_universe.select("dataset1_key", "dataset3_key", "textcode")
```

```python
# Cell 5: Assert Merge Universe Data on textcode
expected_universe_combined = spark.createDataFrame([
    (1, None, "textcode1"),
    (2, 5, "textcode2"),
    (3, None, "textcode3"),
    (4, 6, "textcode4"),
    (None, 7, "textcode7"),
    (None, 8, "textcode8")
], schema_universe)

assert df_universe_combined.subtract(expected_universe_combined).count() == 0 and expected_universe_combined.subtract(df_universe_combined).count() == 0, "df_universe_combined does not match expected data."
```

```python
# Cell 6: Calculate Non-Null Keys
df_universe_combined = df_universe_combined.withColumn(
    "non_null_keys",
    (col("dataset1_key").isNotNull().cast("integer") + col("dataset3_key").isNotNull().cast("integer"))
)
```

```python
# Cell 7: Assert Non-Null Keys Calculation
from pyspark.sql.functions import expr

df_universe_combined_calculated = df_universe_combined.withColumn("expected_non_null_keys", 
    when(col("dataset1_key").isNotNull(), 1).otherwise(0) + 
    when(col("dataset3_key").isNotNull(), 1).otherwise(0)
)

assert df_universe_combined.select("non_null_keys").subtract(
    df_universe_combined_calculated.select("expected_non_null_keys")
).count() == 0, "Non-null keys calculation is incorrect."
```

```python
# Cell 8: Define Window Specification
from pyspark.sql.window import Window
import pyspark.sql.functions as F

window_spec = Window.partitionBy("textcode").orderBy(
    col("non_null_keys").desc(),
    col("dataset1_key").asc_nulls_last(),
    col("dataset3_key").asc_nulls_last()
)
```

```python
# Cell 9: Select the Most Complete Row per textcode
df_universe_most_complete = df_universe_combined.withColumn(
    "row_num", F.row_number().over(window_spec)
).filter(col("row_num") == 1).drop("non_null_keys", "row_num")
```

```python
# Cell 10: Assert Most Complete Row Selection
expected_universe_most_complete = spark.createDataFrame([
    (1, None, "textcode1"),
    (2, 5, "textcode2"),
    (3, None, "textcode3"),
    (4, 6, "textcode4"),
    (None, 7, "textcode7"),
    (None, 8, "textcode8")
], schema_universe)

assert df_universe_most_complete.subtract(expected_universe_most_complete).count() == 0 and expected_universe_most_complete.subtract(df_universe_most_complete).count() == 0, "Most complete rows selection is incorrect."
```

```python
# Cell 11: Prepare Universe Data
df_universe_prepared = df_universe_most_complete.withColumn(
    "dataset1_key",
    when(col("dataset1_key").isNull(), col("dataset3_key")).otherwise(col("dataset1_key"))
).withColumn(
    "dataset3_key",
    when(col("dataset1_key") == col("dataset3_key"), None).otherwise(col("dataset3_key"))
).drop("textcode")
```

```python
# Cell 12: Assert Universe Data Preparation
expected_universe_prepared = spark.createDataFrame([
    (1, None),
    (2, 5),
    (3, None),
    (4, 6),
    (7, None),
    (8, None)
], StructType([
    StructField("dataset1_key", IntegerType(), True),
    StructField("dataset3_key", IntegerType(), True)
]))

assert df_universe_prepared.subtract(expected_universe_prepared).count() == 0 and expected_universe_prepared.subtract(df_universe_prepared).count() == 0, "Universe data preparation is incorrect."
```

```python
# Cell 13: Full Outer Join on dataset1_key
df_merged = df_current.alias("current").join(
    df_universe_prepared.alias("universe"),
    on="dataset1_key",
    how="full_outer"
)
```

```python
# Cell 14: Assert Full Outer Join
from pyspark.sql.functions import coalesce

expected_merged = df_current.alias("current").join(
    df_universe_prepared.alias("universe"),
    on="dataset1_key",
    how="full_outer"
)

# Since schema may have overlapping columns, ensure columns are correctly referenced
assert df_merged.schema == expected_merged.schema, "Full outer join schema mismatch."
assert df_merged.subtract(expected_merged).count() == 0 and expected_merged.subtract(df_merged).count() == 0, "Full outer join content mismatch."
```

```python
# Cell 15: Define Update Date
update_date = to_date(lit("2024-10-30"), "yyyy-MM-dd")
```

```python
# Cell 16: Assert Update Date
expected_update_date = to_date(lit("2024-10-30"), "yyyy-MM-dd")
assert update_date == expected_update_date, "Update date is incorrect."
```

```python
# Cell 17: Determine Action
df_merged = df_merged.withColumn(
    "action",
    when(
        col("current.otherid").isNull(), "added"
    ).when(
        (col("current.dataset3_key").isNull()) & (col("universe.dataset3_key").isNotNull()), "updated"
    ).otherwise("unchanged")
)
```

```python
# Cell 18: Assert Action Column
# Expected actions based on merged data
expected_actions = {
    1: "unchanged",
    2: "updated",
    3: "unchanged",
    4: "unchanged",
    7: "added",
    8: "added"
}

df_actions = df_merged.select("dataset1_key", "action").collect()
for row in df_actions:
    key = row["dataset1_key"]
    action = row["action"]
    assert action == expected_actions.get(key, "unchanged"), f"Action for dataset1_key {key} is incorrect."
```

```python
# Cell 19: Update Rows Based on Action
df_updated = df_merged.withColumn(
    "dataset3_key",
    coalesce(col("universe.dataset3_key"), col("current.dataset3_key"))
).withColumn(
    "otherid",
    when(col("action") == "added", None).otherwise(col("current.otherid"))
).withColumn(
    "date",
    when(
        col("action").isin("updated", "added"), update_date
    ).otherwise(col("current.date"))
).select(
    "dataset1_key", "dataset3_key", "otherid", "date", "action"
)
```

```python
# Cell 20: Assert Updated Rows
from pyspark.sql.functions import to_date

expected_updated_data = [
    (1, None, "otherid1", to_date(lit("2024-01-01"), "yyyy-MM-dd"), "unchanged"),
    (2, 5, "otherid2", to_date(lit("2024-10-30"), "yyyy-MM-dd"), "updated"),
    (3, None, "otherid3", to_date(lit("2024-01-01"), "yyyy-MM-dd"), "unchanged"),
    (4, 6, "otherid4", to_date(lit("2024-01-01"), "yyyy-MM-dd"), "unchanged"),
    (7, None, None, to_date(lit("2024-10-30"), "yyyy-MM-dd"), "added"),
    (8, None, None, to_date(lit("2024-10-30"), "yyyy-MM-dd"), "added")
]

expected_updated = spark.createDataFrame(expected_updated_data, 
    StructType([
        StructField("dataset1_key", IntegerType(), True),
        StructField("dataset3_key", IntegerType(), True),
        StructField("otherid", StringType(), True),
        StructField("date", DateType(), True),
        StructField("action", StringType(), True)
    ])
)

assert df_updated.subtract(expected_updated).count() == 0 and expected_updated.subtract(df_updated).count() == 0, "Updated DataFrame does not match expected data."
```

```python
# Cell 21: Select Necessary Columns and Remove Duplicates
df_final = df_updated.select("dataset1_key", "dataset3_key", "otherid", "date").dropDuplicates(["dataset1_key", "dataset3_key"])
```

```python
# Cell 22: Assert Final DataFrame
expected_final_data = [
    (1, None, "otherid1", to_date(lit("2024-01-01"), "yyyy-MM-dd")),
    (2, 5, "otherid2", to_date(lit("2024-10-30"), "yyyy-MM-dd")),
    (3, None, "otherid3", to_date(lit("2024-01-01"), "yyyy-MM-dd")),
    (4, 6, "otherid4", to_date(lit("2024-01-01"), "yyyy-MM-dd")),
    (7, None, None, to_date(lit("2024-10-30"), "yyyy-MM-dd")),
    (8, None, None, to_date(lit("2024-10-30"), "yyyy-MM-dd"))
]

expected_final = spark.createDataFrame(expected_final_data, 
    StructType([
        StructField("dataset1_key", IntegerType(), True),
        StructField("dataset3_key", IntegerType(), True),
        StructField("otherid", StringType(), True),
        StructField("date", DateType(), True)
    ])
)

assert df_final.subtract(expected_final).count() == 0 and expected_final.subtract(df_final).count() == 0, "Final DataFrame does not match expected data."
```

```python
# Cell 23: Display Final Updated DataFrame
df_final.orderBy("dataset1_key").show()
```

```python
# Cell 24: Create Historical DataFrame
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType

schema_historical = StructType([
    StructField("otherkey", IntegerType(), True),
    StructField("dataset1_key", IntegerType(), True),
    StructField("dataset3_key", IntegerType(), True),
    StructField("otherid", StringType(), True),
    StructField("from", StringType(), True),
    StructField("to", StringType(), True)
])

data_historical = [
    (1, 1, None, "otherid1", "01/01/2024", None),
    (2, 2, None, "otherid2", "29/10/2024", None),
    (3, 3, None, "otherid3", "01/01/2024", None),
    (4, 4, 6, "otherid4", "01/01/2024", None),
    (4, 4, None, "otherid4", "01/01/2023", "2024-01-01"),
    (10, None, None, None, "01/01/2023", None)
]

df_historical = spark.createDataFrame(data_historical, schema_historical)
```

```python
# Cell 25: Convert Date Columns to DateType
from pyspark.sql.functions import to_date

df_historical = df_historical.withColumn("from", to_date(col("from"), "dd/MM/yyyy")) \
                             .withColumn("to", to_date(col("to"), "yyyy-MM-dd"))
```

```python
# Cell 26: Assert Historical DataFrame
expected_historical_data = [
    (1, 1, None, "otherid1", "2024-01-01", None),
    (2, 2, None, "otherid2", "2024-10-29", None),
    (3, 3, None, "otherid3", "2024-01-01", None),
    (4, 4, 6, "otherid4", "2024-01-01", None),
    (4, 4, None, "otherid4", "2023-01-01", "2024-01-01"),
    (10, None, None, None, "2023-01-01", None)
]

expected_historical = spark.createDataFrame(expected_historical_data, schema_historical) \
    .withColumn("from", to_date(col("from"), "yyyy-MM-dd")) \
    .withColumn("to", to_date(col("to"), "yyyy-MM-dd"))

assert df_historical.subtract(expected_historical).count() == 0 and expected_historical.subtract(df_historical).count() == 0, "Historical DataFrame does not match expected data."
```

```python
# Cell 27: Define Update Function
from pyspark.sql.functions import when, max as spark_max

def update_historical(df_historical, df_final, current_date_str='2024-10-30'):
    current_date = to_date(lit(current_date_str), "yyyy-MM-dd")
    
    # Iterate over each row in df_final
    for row in df_final.collect():
        dataset1_key = row['dataset1_key']
        dataset3_key = row['dataset3_key']
        otherid = row['otherid']
        date = row['date']
        
        # Find matching historical records
        matches = df_historical.filter(
            (col("dataset1_key") == dataset1_key) |
            (col("dataset3_key") == dataset3_key) |
            (col("otherid") == otherid)
        ).filter(col("to").isNull())
        
        if matches.count() > 0:
            for match in matches.collect():
                # Update 'to' date of the existing historical row
                df_historical = df_historical.withColumn(
                    "to",
                    when((col("otherkey") == match["otherkey"]), current_date).otherwise(col("to"))
                )
                # Add new historical record
                new_record = spark.createDataFrame([(
                    match["otherkey"],
                    dataset1_key,
                    dataset3_key,
                    otherid,
                    current_date_str,
                    None
                )], schema_historical)
                df_historical = df_historical.union(new_record)
        else:
            # Assign new otherkey
            max_otherkey = df_historical.agg(spark_max("otherkey")).collect()[0][0]
            new_otherkey = max_otherkey + 1 if max_otherkey is not None else 1
            # Add new historical record with new otherkey
            new_record = spark.createDataFrame([(
                new_otherkey,
                dataset1_key,
                dataset3_key,
                otherid,
                current_date_str,
                None
            )], schema_historical)
            df_historical = df_historical.union(new_record)
    
    return df_historical
```

```python
# Cell 28: Apply Update Function
df_historical_updated = update_historical(df_historical, df_final)
```

```python
# Cell 29: Assert Updated Historical DataFrame
expected_historical_updated_data = [
    (1, 1, None, "otherid1", "2024-01-01", None),
    (2, 2, None, "otherid2", "2024-10-29", "2024-10-30"),
    (3, 3, None, "otherid3", "2024-01-01", None),
    (4, 4, 6, "otherid4", "2024-01-01", None),
    (4, 4, None, "otherid4", "2023-01-01", "2024-01-01"),
    (10, None, None, None, "2023-01-01", None),
    (1, 1, None, "otherid1", "2024-10-30", None),
    (2, 2, None, "otherid2", "2024-10-30", None),
    (3, 3, None, "otherid3", "2024-10-30", None),
    (4, 4, 6, "otherid4", "2024-10-30", None),
    (7, None, None, None, "2024-10-30", None),
    (8, None, None, None, "2024-10-30", None)
]

expected_historical_updated = spark.createDataFrame(expected_historical_updated_data, schema_historical) \
    .withColumn("from", to_date(col("from"), "yyyy-MM-dd")) \
    .withColumn("to", to_date(col("to"), "yyyy-MM-dd"))

assert df_historical_updated.subtract(expected_historical_updated).count() == 0 and expected_historical_updated.subtract(df_historical_updated).count() == 0, "Updated Historical DataFrame does not match expected data."
```

```python
# Cell 30: Display Updated Historical DataFrame
df_historical_updated.orderBy("otherkey").show()
```

```python
# Cell 31: Create Entities DataFrame
schema_entities = StructType([
    StructField("otherkey", IntegerType(), True),
    StructField("countrycode", StringType(), True),
    StructField("industry", StringType(), True)
])

data_entities = [
    (1, "usa", "finance"),
    (2, "gre", "oil"),
    (3, "usa", "finance"),
    (4, "usa", "automobile"),
    (10, "usa", "automobile")
]

df_entities = spark.createDataFrame(data_entities, schema_entities)
```

```python
# Cell 32: Create Dataset1 and Dataset3 DataFrames
schema_dataset1 = StructType([
    StructField("dataset1_key", IntegerType(), True),
    StructField("textcode", StringType(), True),
    StructField("countrycode", StringType(), True)
])

data_dataset1 = [
    (1, "textcode1", "country1"),
    (2, "textcode2", "country2"),
    (3, "textcode3", "country1"),
    (4, "textcode4", "country1")
]

df_dataset1 = spark.createDataFrame(data_dataset1, schema_dataset1)

schema_dataset3 = StructType([
    StructField("dataset3_key", IntegerType(), True),
    StructField("textcode", StringType(), True),
    StructField("countrycode", StringType(), True)
])

data_dataset3 = [
    (5, "textcode2", "country2"),
    (6, "textcode4", "country2"),
    (7, "textcode7", "country2"),
    (8, "textcode8", "country2")
]

df_dataset3 = spark.createDataFrame(data_dataset3, schema_dataset3)
```

```python
# Cell 33: Update Entities Based on Historical Overlaps
# Join historical with dataset3 to get countrycode
df_overlaps = df_historical_updated.join(df_dataset3, on="dataset3_key", how="left") \
    .filter(df_historical_updated.dataset1_key.isNotNull())

df_entities_updated = df_entities.alias("e").join(
    df_overlaps.select("otherkey", "countrycode"),
    on="otherkey",
    how="left"
).withColumn(
    "countrycode",
    when(col("countrycode").isNotNull(), col("countrycode")).otherwise(col("e.countrycode"))
).select("e.otherkey", "countrycode", "industry")
```

```python
# Cell 34: Assert Entities Update
expected_entities_updated_data = [
    (1, "usa", "finance"),
    (2, "country2", "oil"),
    (3, "usa", "finance"),
    (4, "usa", "automobile"),
    (10, "usa", "automobile")
]

expected_entities_updated = spark.createDataFrame(expected_entities_updated_data, schema_entities)

assert df_entities_updated.subtract(expected_entities_updated).count() == 0 and expected_entities_updated.subtract(df_entities_updated).count() == 0, "Entities DataFrame update is incorrect."
```

```python
# Cell 35: Handle Non-Overlapping Dataset3 Entries
# Identify new keys to add
current_keys = df_current.select("dataset1_key").na.drop().rdd.flatMap(lambda x: x).collect() + \
               df_current.select("dataset3_key").na.drop().rdd.flatMap(lambda x: x).collect()

universe_keys = df_universe_prepared.select("dataset1_key").na.drop().rdd.flatMap(lambda x: x).collect() + \
                df_universe_prepared.select("dataset3_key").na.drop().rdd.flatMap(lambda x: x).collect()

new_keys = list(set(universe_keys) - set(current_keys))

# Get new rows from universe
df_new_rows = df_universe_prepared.filter(
    (col("dataset1_key").isin(new_keys)) | (col("dataset3_key").isin(new_keys))
).select("dataset1_key", "dataset3_key").distinct()

# Assign new otherkeys
max_otherkey = df_entities.select(spark_max("otherkey")).collect()[0][0]
new_otherkeys = list(range(max_otherkey + 1, max_otherkey + 1 + df_new_rows.count()))

# Create new rows with new_otherkeys
new_rows_rdd = df_new_rows.rdd.zipWithIndex().map(lambda row: (
    new_otherkeys[int(row[1])],
    row[0][0],
    row[0][1],
    None,
    to_date(lit("2024-10-30"), "yyyy-MM-dd"),
    None
))

df_new_rows_final = spark.createDataFrame(new_rows_rdd.collect(), schema_historical)

# Append new rows to historical
df_historical_updated = df_historical_updated.union(df_new_rows_final)
```

```python
# Cell 36: Assert Non-Overlapping Entries Handling
# Define expected new rows
expected_new_rows = [
    (11, 7, None, None, "2024-10-30", None),
    (12, 8, None, None, "2024-10-30", None)
]

expected_new_rows_df = spark.createDataFrame(expected_new_rows, schema_historical) \
    .withColumn("from", to_date(col("from"), "yyyy-MM-dd")) \
    .withColumn("to", to_date(col("to"), "yyyy-MM-dd"))

actual_new_rows = df_historical_updated.filter(col("otherkey").isin(11, 12))

assert actual_new_rows.subtract(expected_new_rows_df).count() == 0 and expected_new_rows_df.subtract(actual_new_rows).count() == 0, "Non-overlapping entries handling is incorrect."
```

```python
# Cell 37: Final Entities Update with New Rows
df_new_entities = df_new_rows_final.select("otherkey", "dataset3_key").join(df_dataset3, on="dataset3_key", how="left") \
    .select("otherkey", "countrycode").withColumn("industry", lit("n/a"))

df_entities_final = df_entities_updated.union(df_new_entities).dropDuplicates(["otherkey"]).fillna({"industry": "n/a"})
```

```python
# Cell 38: Assert Final Entities DataFrame
expected_entities_final_data = [
    (1, "usa", "finance"),
    (2, "country2", "oil"),
    (3, "usa", "finance"),
    (4, "usa", "automobile"),
    (10, "usa", "automobile"),
    (11, "country2", "n/a"),
    (12, "country2", "n/a")
]

expected_entities_final = spark.createDataFrame(expected_entities_final_data, schema_entities)

assert df_entities_final.subtract(expected_entities_final).count() == 0 and expected_entities_final.subtract(df_entities_final).count() == 0, "Final Entities DataFrame does not match expected data."
```

```python
# Cell 39: Display Final Entities DataFrame
df_entities_final.orderBy("otherkey").show()
```