```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, count, lit
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("DatasetMergeExample") \
    .getOrCreate()

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

# Outer join on textcode to combine dataset1 and dataset3
df_combined = df_dataset1.join(df_dataset3, on="textcode", how="outer")

# Calculate the number of non-null keys in each row
df_combined = df_combined.withColumn(
    "non_null_keys",
    (col("dataset1_key").isNotNull().cast("integer") + col("dataset3_key").isNotNull().cast("integer"))
)

# Define window specification for dataset1_key
from pyspark.sql.window import Window
import pyspark.sql.functions as F

window_spec1 = Window.partitionBy("dataset1_key").orderBy(
    col("non_null_keys").desc(),
    col("dataset3_key").asc_nulls_last(),
    col("textcode").asc()
)

# Apply row_number over dataset1_key
df_combined = df_combined.withColumn(
    "row_num1",
    F.row_number().over(window_spec1)
)

# Define window specification for dataset3_key
window_spec2 = Window.partitionBy("dataset3_key").orderBy(
    col("non_null_keys").desc(),
    col("dataset1_key").asc_nulls_last(),
    col("textcode").asc()
)

# Apply row_number over dataset3_key
df_combined = df_combined.withColumn(
    "row_num2",
    F.row_number().over(window_spec2)
)

# Filter to keep only the most complete rows per key
df_filtered = df_combined.filter(
    ((col("dataset1_key").isNotNull()) & (col("row_num1") == 1)) |
    ((col("dataset3_key").isNotNull()) & (col("row_num2") == 1))
).drop("non_null_keys", "row_num1", "row_num2")

# Select the desired columns to form the final dataset
df_dataset1_dataset3 = df_filtered.select("dataset1_key", "dataset3_key").distinct()

# Display the final dataset1_dataset3
df_dataset1_dataset3.show()
```
