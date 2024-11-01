```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit, to_date, coalesce
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType

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

# Sample data for universe
data_universe = [
    (1, "textcode1", None),
    (2, "textcode2", 5),
    (3, "textcode3", None),
    (4, "textcode4", 6),
    (None, "textcode7", 7),
    (None, "textcode8", 8)
]

df_universe = spark.createDataFrame(data_universe, schema_universe)

# Exclude textcode from universe
df_universe_prepared = df_universe.select("dataset1_key", "dataset3_key") \
    .filter(col("dataset1_key").isNotNull() | col("dataset3_key").isNotNull())

# Sample data for current_df
data_current = [
    (1, None, "otherid", to_date(lit("2024-01-01"))),
    (2, None, "otherid", to_date(lit("2024-01-01"))),
    (3, None, "otherid", to_date(lit("2024-01-01"))),
    (None, 6, "otherid", to_date(lit("2024-01-01")))
]

df_current = spark.createDataFrame(data_current, schema_current)

# Left join on dataset1_key
df_update1 = df_current.alias("current").join(
    df_universe_prepared.alias("universe"),
    on="dataset1_key",
    how="left"
).select(
    col("dataset1_key"),
    coalesce(col("current.dataset3_key"), col("universe.dataset3_key")).alias("dataset3_key"),
    col("current.otherid"),
    col("current.date"),
    when(
        col("current.dataset3_key").isNull() & col("universe.dataset3_key").isNotNull(),
        True
    ).otherwise(False).alias("updated")
)

# Left join on dataset3_key
df_update2 = df_current.alias("current").join(
    df_universe_prepared.alias("universe"),
    on="dataset3_key",
    how="left"
).select(
    coalesce(col("current.dataset1_key"), col("universe.dataset1_key")).alias("dataset1_key"),
    col("dataset3_key"),
    col("current.otherid"),
    col("current.date"),
    when(
        col("current.dataset1_key").isNull() & col("universe.dataset1_key").isNotNull(),
        True
    ).otherwise(False).alias("updated")
)

# Combine updates
df_updates_combined = df_update1.union(df_update2).distinct()

# Update dates for modified rows
update_date = to_date(lit("2024-10-30"), "yyyy-MM-dd")

df_updates = df_updates_combined.withColumn(
    "date",
    when(col("updated"), update_date).otherwise(col("date"))
).drop("updated")

# Identify new rows to add from universe
df_new_rows = df_universe_prepared.alias("universe").join(
    df_current.alias("current"),
    on=["dataset1_key", "dataset3_key"],
    how="left_anti"
).select(
    "dataset1_key",
    "dataset3_key",
    lit(None).alias("otherid"),
    update_date.alias("date")
)

# Combine updated rows and new rows
df_final = df_updates.union(df_new_rows)

# Include unchanged rows from current_df
df_unchanged = df_current.alias("current").join(
    df_updates_combined.select("dataset1_key", "dataset3_key").alias("updates"),
    on=["dataset1_key", "dataset3_key"],
    how="left_anti"
)

df_final = df_final.union(df_unchanged)

# Display the final updated DataFrame
print("Final Updated current_df:")
df_final.orderBy("dataset1_key").show()
```