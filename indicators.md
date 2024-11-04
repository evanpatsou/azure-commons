```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit, to_date, coalesce, max as spark_max, array, explode
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType
import pyspark.sql.functions as F

# Initialize Spark Session
spark = SparkSession.builder.appName("PandasToSparkFlexible").getOrCreate()

# 1. Set Up Sample DataFrames

# Schema for historical
schema_historical = StructType([
    StructField("otherkey", IntegerType(), True),
    StructField("dataset1_key", IntegerType(), True),
    StructField("dataset3_key", IntegerType(), True),
    StructField("otherid", StringType(), True),
    StructField("from", DateType(), True),
    StructField("to", DateType(), True)
])

data_historical = [
    (1, 1, None, "otherid1", "2024-01-01", None),
    (2, 2, None, "otherid2", "2024-10-29", None),
    (3, 3, None, "otherid3", "2024-01-01", None),
    (4, 4, 6, "otherid4", "2024-01-01", None),
    (4, 4, None, "otherid4", "2023-01-01", "2024-01-01"),
    (10, None, None, None, "2023-01-01", None)
]

df_historical = spark.createDataFrame(data_historical, schema_historical) \
    .withColumn("from", to_date(col("from"), "yyyy-MM-dd")) \
    .withColumn("to", to_date(col("to"), "yyyy-MM-dd"))

# Schema for final_df
schema_final = StructType([
    StructField("dataset1_key", IntegerType(), True),
    StructField("dataset3_key", IntegerType(), True),
    StructField("otherid", StringType(), True),
    StructField("date", DateType(), True)
])

data_final = [
    (1, None, "otherid1", "2024-01-01"),
    (2, 5, "otherid2", "2024-10-30"),
    (3, None, "otherid3", "2024-01-01"),
    (4, 6, "otherid4", "2024-10-30"),
    (7, None, None, "2024-10-30"),
    (8, None, None, "2024-10-30")
]

df_final = spark.createDataFrame(data_final, schema_final) \
    .withColumn("date", to_date(col("date"), "yyyy-MM-dd"))

# 2. Define the Update Function

current_date_str = '2024-10-30'
current_date = F.lit(current_date_str).cast(DateType())

# Identify non-date columns
historical_fields = df_historical.schema.fields
final_fields = df_final.schema.fields

historical_non_date_cols = [f.name for f in historical_fields if not isinstance(f.dataType, DateType)]
final_non_date_cols = [f.name for f in final_fields if not isinstance(f.dataType, DateType)]

# Common columns excluding date columns
common_cols = list(set(historical_non_date_cols).intersection(set(final_non_date_cols)))

# Build join condition
join_condition = None
for col_name in common_cols:
    condition = df_final[col_name] == df_historical[col_name]
    if join_condition is None:
        join_condition = condition
    else:
        join_condition = join_condition | condition

# Join active historical records with final_df
active_historical = df_historical.filter(col("to").isNull())

joined_df = df_final.join(
    active_historical,
    on=join_condition,
    how="left"
)

# Identify changes
changes_conditions = []
for col_name in common_cols:
    condition = (
        (df_final[col_name] != df_historical[col_name]) |
        (df_final[col_name].isNull() & df_historical[col_name].isNotNull()) |
        (df_final[col_name].isNotNull() & df_historical[col_name].isNull())
    )
    changes_conditions.append(condition)

overall_change_condition = None
for condition in changes_conditions:
    if overall_change_condition is None:
        overall_change_condition = condition
    else:
        overall_change_condition = overall_change_condition | condition

# Update 'to' dates for existing records where changes are detected
updated_historical = df_historical.join(
    df_final,
    on=join_condition,
    how="left"
).withColumn(
    "to",
    when(overall_change_condition, current_date).otherwise(col("to"))
)

# Create new records for changes and new entries
new_records = joined_df.filter(overall_change_condition | col("otherkey").isNull()) \
    .select(
        coalesce(col("otherkey"), lit(None).cast(IntegerType())).alias("otherkey"),
        *[col(c) for c in common_cols],
        current_date.alias("from"),
        lit(None).cast(DateType()).alias("to")
    )

# Union updated historical with new records
df_historical_updated = updated_historical.union(new_records)

# 3. Post-Processing for Display

# Sorting
df_historical_sorted = df_historical_updated.withColumn(
    "otherkey_sort",
    when(col("otherkey").isNull(), 99999).otherwise(col("otherkey"))
).orderBy(
    "otherkey_sort",
    *common_cols,
    "from"
).drop("otherkey_sort")

# Convert dates to desired string format
df_final_display = df_historical_sorted \
    .withColumn("from", F.date_format(col("from"), "dd/MM/yyyy")) \
    .withColumn("to", F.date_format(col("to"), "dd/MM/yyyy")) \
    .na.fill({"to": ""})

# Show the updated DataFrame
df_final_display.show(truncate=False)
```