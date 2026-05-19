from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType
from pyspark.sql.functions import col, input_file_name, regexp_extract, lit
from glob import glob
import re

spark = SparkSession.builder.appName("OuterJoin").getOrCreate()

orc_lake_base_path = "/home/ionadmin/Git/mongo-catalog-tracker/data_lake/orc"

# Now, when you read from output_path, Spark will automatically pick up the partitions
df_re_read1 = spark.read.orc(f"{orc_lake_base_path}/partitionRanges")
df_re_read2 = spark.read.orc(f"{orc_lake_base_path}/opCountItem")

df_re_read1.createOrReplaceTempView("partition_ranges")
df_re_read2.createOrReplaceTempView("op_count_item")

df1 = spark.sql("""
    WITH SinceDate AS (
        SELECT row.mongo_hostname, p.days_post_epoch, p.seconds_of_day,
            MIN(row.Since) AS PostSince
        FROM op_count_item AS row
        JOIN partition_ranges AS p
            ON row.mongo_hostname = p.mongo_hostname
            AND row.days_post_epoch = p.days_post_epoch
            AND row.seconds_of_day = p.seconds_of_day
            AND p.is_first_partition
        WHERE row.Since IS NOT NULL
        GROUP BY row.mongo_hostname, p.days_post_epoch, p.seconds_of_day
    ) 
    SELECT DISTINCT row.mongo_hostname, row.CollectionName, MIN(row.Since), MIN(sd.PostSince)
    FROM op_count_item AS row
        JOIN SinceDate AS sd
            ON row.mongo_hostname = sd.mongo_hostname
            AND row.days_post_epoch = sd.days_post_epoch
            AND row.seconds_of_day = sd.seconds_of_day
    WHERE row.Since <= sd.PostSince
    GROUP BY row.mongo_hostname, row.CollectionName
""")
# WHERE row.UseCount > 0
df1.createOrReplaceTempView("report")
df1.show(30000, truncate=False)

spark.stop()
