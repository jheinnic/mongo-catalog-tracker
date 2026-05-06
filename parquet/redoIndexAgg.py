from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType
from pyspark.sql.functions import col, input_file_name, regexp_extract, lit
from glob import glob
import re

spark = SparkSession.builder \
        .appName("OuterJoin") \
        .config("spark.driver.memory", "8g") \
        .config("spark.driver.maxResultSize", "6g") \
        .config("spark.sql.shuffle.partitions", "500" ) \
        .config("spark.executor.memory", "32g") \
        .config("spark.executor.memoryOverhead", "6g") \
        .config("spark.memory.fraction", "0.75" ) \
        .getOrCreate()

parquet_lake_base_path = "/home/ionadmin/Git/mongo-catalog-tracker/data_lake/parquet"

# Now, when you read from output_path, Spark will automatically pick up the partitions
df1_read = spark.read.parquet(f"{parquet_lake_base_path}/opCountItem")
df1_read.createOrReplaceTempView("op_count_item")

df2_read = spark.read.parquet(f"{parquet_lake_base_path}/collectionOpsAgg")
df2_read.createOrReplaceTempView("collection_agg")

df3 = spark.sql("""
        SELECT row.mongo_hostname, row.days_post_epoch, row.seconds_of_day, row.IndexName, sum(row.UseCount) AS UseCount, sum(if(row.UseCount > 0, 1, 0)) AS ActiveInCollectionCount, sum(if(collection_agg.ActiveIndexCount > 0, 1, 0)) AS LoadedInCollectionCount, count(1) AS PresentInCollectionCount
        FROM op_count_item AS row JOIN collection_agg ON row.mongo_hostname = collection_agg.mongo_hostname AND row.days_post_epoch = collection_agg.days_post_epoch AND row.seconds_of_day = collection_agg.seconds_of_day AND row.CollectionName = collection_agg.CollectionName
        GROUP BY row.mongo_hostname, row.days_post_epoch, row.seconds_of_day, row.IndexName
        ORDER By row.mongo_hostname ASC, row.days_post_epoch ASC, row.seconds_of_day ASC, sum(row.UseCount) DESC
        """)
df3.show(210, truncate=False)

# Save as a partitioned Orc table
df3.write \
   .partitionBy("mongo_hostname", "days_post_epoch", "seconds_of_day") \
   .option("partitionOverwriteMode", "dynamic") \
   .mode("overwrite") \
   .parquet(f"{parquet_lake_base_path}/indexOpsAgg/")

spark.stop();
