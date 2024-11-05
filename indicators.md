```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit, to_date, coalesce, date_format, monotonically_increasing_id
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType
import pyspark.sql.functions as F
from functools import reduce
import operator

# Initialize Spark Session
spark = SparkSession.builder.appName("PandasToSparkFlexibleOuterJoin").getOrCreate()

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

# Build join condition for composite key (AND condition)
join_conditions = []
for col_name in common_cols:
    join_conditions.append(df_final[col_name] == df_historical[col_name])

if join_conditions:
    join_condition = reduce(operator.and_, join_conditions)
else:
    join_condition = lit(False)

# Perform left join to update existing records
updated_historical = df_historical.alias("historical").join(
    df_final.alias("final"),
    on=join_condition,
    how="left"
).withColumn(
    "to",
    when(
        reduce(operator.or_, [
            (col("final." + c) != col("historical." + c)) |
            (col("final." + c).isNull() & col("historical." + c).isNotNull()) |
            (col("final." + c).isNotNull() & col("historical." + c).isNull())
            for c in common_cols
        ]),
        current_date
    ).otherwise(col("to"))
)

# Create new records for additions and updates
new_records = df_final.alias("final").join(
    df_historical.alias("historical"),
    on=join_condition,
    how="left_anti"
).select(
    monotonically_increasing_id().alias("otherkey"),
    *["final." + c for c in common_cols],
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
    .withColumn("from", date_format(col("from"), "dd/MM/yyyy")) \
    .withColumn("to", date_format(col("to"), "dd/MM/yyyy")) \
    .na.fill({"to": ""})

# Show the updated DataFrame
df_final_display.show(truncate=False)
```
