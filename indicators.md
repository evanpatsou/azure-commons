```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit, to_date, coalesce, row_number
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType
from pyspark.sql.window import Window

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("UpdateCurrentDFExample") \
    .getOrCreate()

# Define schemas
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

schema_historical = StructType([
    StructField("otherkey", IntegerType(), True),
    StructField("dataset1_key", IntegerType(), True),
    StructField("dataset3_key", IntegerType(), True),
    StructField("otherid", StringType(), True),
    StructField("from", DateType(), True),
    StructField("to", DateType(), True)
])

schema_entities = StructType([
    StructField("otherkey", IntegerType(), True),
    StructField("countrycode", StringType(), True),
    StructField("industry", StringType(), True)
])

schema_dataset1 = StructType([
    StructField("dataset1_key", IntegerType(), True),
    StructField("textcode", StringType(), True),
    StructField("countrycode", StringType(), True)
])

schema_dataset3 = StructType([
    StructField("dataset3_key", IntegerType(), True),
    StructField("textcode", StringType(), True),
    StructField("countrycode", StringType(), True)
])

# Sample data for universe
data_universe = [
    (1, "textcode1", None),
    (2, "textcode2", 5),
    (3, "textcode3", None),
    (4, "textcode4", 6),
    (7, "textcode7", None),
    (8, "textcode8", None)
]
df_universe = spark.createDataFrame(data_universe, schema_universe)

# Sample data for current_df
data_current = [
    (1, None, "otherid1", to_date(lit("2024-01-01"), "yyyy-MM-dd")),
    (2, None, "otherid2", to_date(lit("2024-01-01"), "yyyy-MM-dd")),
    (3, None, "otherid3", to_date(lit("2024-01-01"), "yyyy-MM-dd")),
    (None, 6, "otherid4", to_date(lit("2024-01-01"), "yyyy-MM-dd"))
]
df_current = spark.createDataFrame(data_current, schema_current)

# Sample data for historical
data_historical = [
    (1, 1, None, "otherid1", to_date(lit("2024-01-01"), "yyyy-MM-dd"), None),
    (2, 2, None, "otherid2", to_date(lit("2024-10-29"), "yyyy-MM-dd"), None),
    (3, 3, None, "otherid3", to_date(lit("2024-01-01"), "yyyy-MM-dd"), None),
    (4, 4, 6, "otherid4", to_date(lit("2024-01-01"), "yyyy-MM-dd"), None),
    (5, 4, None, "otherid4", to_date(lit("2023-01-01"), "yyyy-MM-dd"), to_date(lit("2024-01-01"), "yyyy-MM-dd")),
    (10, None, None, None, to_date(lit("2023-01-01"), "yyyy-MM-dd"), None)
]
df_historical = spark.createDataFrame(data_historical, schema_historical)

# Sample data for entities
data_entities = [
    (1, "usa", "finance"),
    (2, "gre", "oil"),
    (3, "usa", "finance"),
    (4, "usa", "automobile"),
    (10, "usa", "automobile")
]
df_entities = spark.createDataFrame(data_entities, schema_entities)

# Sample data for dataset1
data_dataset1 = [
    (1, "textcode1", "country1"),
    (2, "textcode2", "country2"),
    (3, "textcode3", "country1"),
    (4, "textcode4", "country1")
]
df_dataset1 = spark.createDataFrame(data_dataset1, schema_dataset1)

# Sample data for dataset3
data_dataset3 = [
    (5, "textcode2", "country2"),
    (6, "textcode4", "country2"),
    (7, "textcode7", "country2"),
    (8, "textcode8", "country2")
]
df_dataset3 = spark.createDataFrame(data_dataset3, schema_dataset3)

# Prepare universe data by selecting relevant columns
df_universe_combined = df_universe.select("dataset1_key", "dataset3_key", "textcode")

# Calculate non-null keys
df_universe_combined = df_universe_combined.withColumn(
    "non_null_keys",
    (col("dataset1_key").isNotNull().cast("integer") + col("dataset3_key").isNotNull().cast("integer"))
)

# Define window specification
window_spec = Window.partitionBy("textcode").orderBy(
    col("non_null_keys").desc(),
    col("dataset1_key").asc_nulls_last(),
    col("dataset3_key").asc_nulls_last()
)

# Select the most complete row per textcode
df_universe_most_complete = df_universe_combined.withColumn(
    "row_num", row_number().over(window_spec)
).filter(col("row_num") == 1).drop("non_null_keys", "row_num")

# Assign dataset3_key to dataset1_key when dataset1_key is null
df_universe_prepared = df_universe_most_complete.withColumn(
    "dataset1_key",
    when(col("dataset1_key").isNull(), col("dataset3_key")).otherwise(col("dataset1_key"))
).withColumn(
    "dataset3_key",
    when(col("dataset1_key") == col("dataset3_key"), None).otherwise(col("dataset3_key"))
).drop("textcode")

# Full outer join on dataset1_key
df_merged = df_current.alias("current").join(
    df_universe_prepared.alias("universe"),
    on="dataset1_key",
    how="full_outer"
)

# Define the current date for updates
update_date = to_date(lit("2024-10-30"), "yyyy-MM-dd")

# Determine the action for each row
df_merged = df_merged.withColumn(
    "action",
    when(
        col("current.otherid").isNull(), "added"
    ).when(
        (col("current.dataset3_key").isNull()) & (col("universe.dataset3_key").isNotNull()), "updated"
    ).otherwise("unchanged")
)

# Update the rows based on action
df_updated = df_merged.withColumn(
    "dataset3_key",
    coalesce(col("universe.dataset3_key"), col("current.dataset3_key"))
).withColumn(
    "otherid",
    when(col("action") == "added", lit(None).cast(StringType())).otherwise(col("current.otherid"))
).withColumn(
    "date",
    when(
        col("action").isin("updated", "added"), update_date
    ).otherwise(col("current.date"))
).select(
    "dataset1_key", "dataset3_key", "otherid", "date", "action"
)

# Select only the necessary columns
df_final = df_updated.select("dataset1_key", "dataset3_key", "otherid", "date")

# Remove any duplicates
df_final = df_final.dropDuplicates(["dataset1_key", "dataset3_key"])

# Display the final updated DataFrame
df_final.orderBy("dataset1_key").show()

# Update historical DataFrame based on final_df
# Join historical with final to find matches
df_historical_active = df_historical.filter(col("to").isNull())

# Join on dataset1_key
df_historical_join1 = df_historical_active.join(
    df_final,
    on="dataset1_key",
    how="left"
)

# Join on dataset3_key
df_historical_join2 = df_historical_active.join(
    df_final,
    on="dataset3_key",
    how="left"
)

# Combine updates from both joins
df_updates = df_historical_join1.select(
    "otherkey",
    "dataset1_key",
    "dataset3_key",
    "otherid",
    "from",
    "to"
).union(
    df_historical_join2.select(
        "otherkey",
        "dataset1_key",
        "dataset3_key",
        "otherid",
        "from",
        "to"
    )
).distinct()

# Identify changes and update historical
df_changes = df_updates.join(
    df_final,
    on=["dataset1_key", "dataset3_key", "otherid"],
    how="inner"
).withColumn(
    "to",
    when(col("date") != col("from"), update_date).otherwise(col("to"))
)

# Add new historical records
df_new_historical = df_final.join(
    df_historical_active,
    on=["dataset1_key", "dataset3_key"],
    how="left_anti"
).withColumn(
    "otherkey", lit(None).cast(IntegerType())
).withColumn(
    "from", col("date")
).withColumn(
    "to", lit(None).cast(DateType())
)

# Combine historical updates
df_historical_updated = df_historical.union(df_new_historical)

# Update entities based on overlaps
df_overlaps = df_historical_updated.filter(
    col("dataset1_key").isNotNull() & col("dataset3_key").isNotNull()
)

df_entities_updated = df_entities.alias("e").join(
    df_overlaps.alias("o"),
    on="otherkey",
    how="left"
).join(
    df_dataset3.alias("d3"),
    on="dataset3_key",
    how="left"
).withColumn(
    "countrycode",
    when(col("d3.countrycode").isNotNull(), col("d3.countrycode")).otherwise(col("e.countrycode"))
).select(
    "e.otherkey",
    "countrycode",
    "industry"
)

# Handle non-overlapping dataset3 entries
current_keys = df_current.select("dataset1_key").union(df_current.select("dataset3_key")).distinct()
universe_keys = df_universe.select("dataset1_key").union(df_universe.select("dataset3_key")).distinct()
new_keys = universe_keys.join(current_keys, on=["dataset1_key"], how="left_anti")

df_new_entities = new_keys.join(
    df_universe_prepared,
    on="dataset1_key",
    how="left"
).select(
    lit(None).cast(IntegerType()).alias("otherkey"),
    "countrycode",
    lit("n/a").alias("industry")
)

# Assign new otherkeys
max_otherkey = df_entities_updated.agg({"otherkey": "max"}).collect()[0][0] or 0
df_new_entities = df_new_entities.withColumn(
    "otherkey",
    col("otherkey") + lit(1)
).orderBy("otherkey")

# Combine all entities
df_entities_final = df_entities_updated.union(df_new_entities).distinct()

# Display final entities
df_entities_final.show()

# Stop Spark Session
spark.stop()
```