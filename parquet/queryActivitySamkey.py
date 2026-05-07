from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType
from pyspark.sql.functions import col, input_file_name, regexp_extract, lit
from glob import glob
import re

spark = SparkSession.builder \
    .appName("OuterJoin") \
    .config("spark.driver.memory", "4g") \
    .config("spark.executor.memory", "8g") \
    .config("spark.executor.memoryOverhead", "4g") \
    .getOrCreate()

parquet_lake_base_path = "/home/ionadmin/Git/mongo-catalog-tracker/data_lake/parquet"

# Now, when you read from output_path, Spark will automatically pick up the partitions
df_re_read1 = spark.read.parquet(f"{parquet_lake_base_path}/basicSamkey")
#df_re_read1 = spark.read.parquet(f"{parquet_lake_base_path}/collectionActivityLabels")

df_re_read1.createOrReplaceTempView("samkey_data")

df2 = spark.sql("""
    WITH Labeled AS (
        SELECT 
            row.mongo_hostname, row.EdgeCount,
            row.PriorAscendingLabel, row.AscendingLabel,
            row.CollectionUseStatusBefore, row.CollectionUseStatus,
            CONCAT(row.CollectionUseStatusBefore, row.PriorAscendingLabel) AS StateBefore,
            CONCAT(row.CollectionUseStatus, row.AscendingLabel) AS StateAfter
        FROM samkey_data AS row
    )
    SELECT 
        row.mongo_hostname,
        CONCAT(row.StateBefore, ',', row.StateAfter, ',', row.EdgeCount) AS samkey_row
    FROM Labeled AS row
    ORDER BY row.mongo_hostname, row.AscendingLabel ASC,
        row.EdgeCount DESC, row.PriorAscendingLabel ASC,
        row.StateBefore ASC, row.StateAfter ASC
""")
df2.show(1000, truncate=False)

spark.stop();
