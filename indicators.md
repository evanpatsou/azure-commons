```python
# Import required libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit, to_date, coalesce
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("DataFrameUpdateExample") \
    .getOrCreate()

# ------------------------------
# 2. Create Initial DataFrames
# ------------------------------

# Define schemas
schema1 = StructType([
    StructField("dataset1_key", IntegerType(), True),
    StructField("textcode", StringType(), True)
])

schema3 = StructType([
    StructField("dataset3_key", IntegerType(), True),
    StructField("textcode", StringType(), True)
])

# Sample data for dataset1
data1 = [
    (1, "textcode1"),
    (2, "textcode2"),
    (3, "textcode3"),
    (4, "textcode4"),
    (1, "textcode11"),
    (2, "textcode22")
]

# Sample data for dataset3
data3 = [
    (5, "textcode2"),
    (6, "textcode4"),
    (7, "textcode7"),
    (8, "textcode8")
]

# Create DataFrames
df_dataset1 = spark.createDataFrame(data1, schema1)
df_dataset3 = spark.createDataFrame(data3, schema3)

# Display dataset1 and dataset3
print("Dataset1:")
df_dataset1.show()

print("Dataset3:")
df_dataset3.show()

# --------------------------------------
# 2.2 Merge dataset1 and dataset3
# --------------------------------------

# Outer join on textcode to combine dataset1 and dataset3
df_combined = df_dataset1.join(df_dataset3, on="textcode", how="outer")

# Calculate the number of non-null keys in each row
df_combined = df_combined.withColumn(
    "non_null_keys",
    (col("dataset1_key").isNotNull().cast("integer") + col("dataset3_key").isNotNull().cast("integer"))
)

# Define window specification with enhanced ordering
from pyspark.sql.window import Window
import pyspark.sql.functions as F

window_spec = Window.partitionBy("textcode").orderBy(
    col("non_null_keys").desc(),
    col("dataset1_key").asc_nulls_last(),
    col("dataset3_key").asc_nulls_last()
)

# Use row_number to select the top row for each textcode
df_most_complete = df_combined.withColumn("row_num", F.row_number().over(window_spec)) \
    .filter(col("row_num") == 1) \
    .drop("non_null_keys", "row_num")

# Select the desired columns to form the universe
df_universe = df_most_complete.select("dataset1_key", "textcode", "dataset3_key").distinct()

# Display the universe DataFrame
print("Universe DataFrame:")
df_universe.show()

# ------------------------------
# 3. Create current_df
# ------------------------------

# Sample data for current_df
data_current = [
    (1, None, "2024-01-01"),
    (2, None, "2024-01-01"),
    (3, None, "2024-01-01"),
    (None, 6, "2024-01-01")
]

schema_current = StructType([
    StructField("dataset1_key", IntegerType(), True),
    StructField("dataset3_key", IntegerType(), True),
    StructField("date", StringType(), True)  # We'll parse this to DateType later
])

df_current = spark.createDataFrame(data_current, schema_current)

# Convert the 'date' column to DateType
df_current = df_current.withColumn("date", to_date(col("date"), "yyyy-MM-dd"))

# Display the current DataFrame
print("Current DataFrame:")
df_current.show()

# ------------------------------
# 4. Update current_df
# ------------------------------

# Full outer join on dataset1_key and dataset3_key
df_combined_update = df_current.alias("current").join(
    df_universe.alias("universe"),
    on=["dataset1_key", "dataset3_key"],
    how="full_outer"
)

# Define the current date for updates
update_date = to_date(lit("2024-10-30"), "yyyy-MM-dd")

# Update existing rows
df_updated_rows = df_combined_update.where(col("current.date").isNotNull()).select(
    coalesce(col("current.dataset1_key"), col("universe.dataset1_key")).alias("dataset1_key"),
    coalesce(col("current.dataset3_key"), col("universe.dataset3_key")).alias("dataset3_key"),
    when(
        (col("current.dataset1_key").isNull() & col("universe.dataset1_key").isNotNull()) |
        (col("current.dataset3_key").isNull() & col("universe.dataset3_key").isNotNull()),
        update_date
    ).otherwise(col("current.date")).alias("date")
)

# New rows to add (present in universe but not in current_df)
df_new_rows = df_combined_update.where(col("current.date").isNull()).select(
    col("universe.dataset1_key").alias("dataset1_key"),
    col("universe.dataset3_key").alias("dataset3_key"),
    update_date.alias("date")
)

# Combine updated and new rows
df_final = df_updated_rows.union(df_new_rows).distinct()

# Add additional columns from universe
df_universe_additional = df_universe.select("dataset1_key", "dataset3_key", "textcode")

# Join additional columns to df_final
df_final_with_additional = df_final.join(
    df_universe_additional,
    on=["dataset1_key", "dataset3_key"],
    how="left"
)

# Display the final DataFrame
print("Final Updated DataFrame:")
df_final_with_additional.show()
```