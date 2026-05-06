from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType
from pyspark.sql.functions import col, input_file_name, regexp_extract, lit
from glob import glob
import re

spark = SparkSession.builder \
    .appName("OuterJoin") \
    .config("spark.driver.memory", "4g") \
    .config("spark.executor.memory", "16g") \
    .config("spark.executor.memoryOverhead", "4g") \
    .config("spark.driver.maxResultSize", "4g") \
    .getOrCreate()

orc_lake_base_path = "/home/ionadmin/Git/mongo-catalog-tracker/data_lake/orc"

# Now, when you read from output_path, Spark will automatically pick up the partitions
df_re_read = spark.read.orc(f"{orc_lake_base_path}/opCountItem")
df_re_read.createOrReplaceTempView("op_count_item")

df1 = spark.sql("""
        SELECT row.mongo_hostname, row.days_post_epoch, row.seconds_of_day, row.CollectionName, sum(row.UseCount) AS UseCount, sum(if(row.UseCount > 0, 1, 0)) AS ActiveIndexCount, count(1) AS TotalIndexCount
        FROM op_count_item AS row
        GROUP BY row.mongo_hostname, row.days_post_epoch, row.seconds_of_day, row.CollectionName
        ORDER By row.mongo_hostname ASC, row.days_post_epoch ASC, row.seconds_of_day ASC, sum(row.UseCount) DESC
        """)
df1.show(210, truncate=False)
df1.createOrReplaceTempView("collection_agg")

# Save as a partitioned Orc table
df1.write \
   .partitionBy("mongo_hostname", "days_post_epoch", "seconds_of_day") \
   .option("partitionOverwriteMode", "dynamic") \
   .mode("overwrite") \
   .orc(f"{orc_lake_base_path}/collectionOpsAgg/")

df2 = spark.sql("""
        SELECT row.mongo_hostname, row.days_post_epoch, row.seconds_of_day, row.IndexName, sum(row.UseCount) AS UseCount, sum(if(row.UseCount > 0, 1, 0)) AS ActiveInCollectionCount, sum(if(collection_agg.ActiveIndexCount > 0, 1, 0)) AS LoadedInCollectionCount, count(1) AS PresentInCollectionCount
        FROM op_count_item AS row JOIN collection_agg ON row.mongo_hostname = collection_agg.mongo_hostname AND row.days_post_epoch = collection_agg.days_post_epoch AND row.seconds_of_day = collection_agg.seconds_of_day AND row.CollectionName = collection_agg.CollectionName
        GROUP BY row.mongo_hostname, row.days_post_epoch, row.seconds_of_day, row.IndexName
        ORDER By row.mongo_hostname ASC, row.days_post_epoch ASC, row.seconds_of_day ASC, sum(row.UseCount) DESC
        """)
df2.show(210, truncate=False)

# Save as a partitioned Orc table
df2.write \
   .partitionBy("mongo_hostname", "days_post_epoch", "seconds_of_day") \
   .option("partitionOverwriteMode", "dynamic") \
   .mode("overwrite") \
   .orc(f"{orc_lake_base_path}/indexOpsAgg/")
