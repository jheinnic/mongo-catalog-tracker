from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType
from pyspark.sql.functions import col, input_file_name, regexp_extract, lit
from glob import glob
import re

spark = SparkSession.builder.appName("OuterJoin").getOrCreate()

orc_lake_base_path = "/home/ionadmin/Git/mongo-catalog-tracker/data_lake/orc"

# Now, when you read from output_path, Spark will automatically pick up the partitions
df_re_read1 = spark.read.orc(f"{orc_lake_base_path}/indexAgg")
df_re_read2 = spark.read.orc(f"{orc_lake_base_path}/collectionOpsAgg")
df_re_read3 = spark.read.orc(f"{orc_lake_base_path}/opCountItem")

df_re_read1.createOrReplaceTempView("index_agg")
df_re_read2.createOrReplaceTempView("collection_agg")
df_re_read3.createOrReplaceTempView("op_count_item")

df1 = spark.sql("""
    SELECT mongo_hostname, days_post_epoch, seconds_of_day, CollectionName, IndexName, COUNT(*)
    FROM op_count_item
    GROUP BY mongo_hostname, days_post_epoch, seconds_of_day, CollectionName, IndexName
    HAVING COUNT(*) > 1;
""")

df1.show(20)

df2 = spark.sql("""
    SELECT mongo_hostname, days_post_epoch, seconds_of_day, CollectionName, COUNT(*)
    FROM collection_agg
    GROUP BY mongo_hostname, days_post_epoch, seconds_of_day, CollectionName
    HAVING COUNT(*) > 1;
""")

df2.show(20)

df3 = spark.sql("""
    SELECT mongo_hostname, days_post_epoch, seconds_of_day, IndexName, COUNT(*)
    FROM index_agg
    GROUP BY mongo_hostname, days_post_epoch, seconds_of_day, IndexName
    HAVING COUNT(*) > 1;
""")

df3.show(20)

df4 = spark.sql("""
    SELECT mongo_hostname, days_post_epoch, seconds_of_day, COUNT(*)
    FROM op_count_item
    GROUP BY mongo_hostname, days_post_epoch, seconds_of_day
    ORDER BY mongo_hostname ASC, days_post_epoch ASC, seconds_of_day ASC;
""")

df4.show(20)

df5 = spark.sql("""
    SELECT mongo_hostname, days_post_epoch, seconds_of_day, COUNT(*)
    FROM collection_agg
    GROUP BY mongo_hostname, days_post_epoch, seconds_of_day
    ORDER BY mongo_hostname ASC, days_post_epoch ASC, seconds_of_day ASC;
""")

df5.show(20)

df6 = spark.sql("""
    SELECT mongo_hostname, days_post_epoch, seconds_of_day, COUNT(*)
    FROM index_agg
    GROUP BY mongo_hostname, days_post_epoch, seconds_of_day
    ORDER BY mongo_hostname ASC, days_post_epoch ASC, seconds_of_day ASC;
""")

df6.show(20)
