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
    .config("spark.executor.memoryOverhead", "2g") \
    .getOrCreate()

orc_lake_base_path = "/home/ionadmin/Git/mongo-catalog-tracker/data_lake/orc"

# Now, when you read from output_path, Spark will automatically pick up the partitions
df_re_read1 = spark.read.orc(f"{orc_lake_base_path}/collectionOpsAgg")
df_re_read2 = spark.read.orc(f"{orc_lake_base_path}/collectionActivityLabels")
df_re_read2 = spark.read.orc(f"{orc_lake_base_path}/opCountItem")

df_re_read1.createOrReplaceTempView("collection_agg")
df_re_read2.createOrReplaceTempView("activity_labels")
df_re_read2.createOrReplaceTempView("op_count_items")

df1 = spark.sql("""
        SELECT row.days_post_epoch, row.seconds_of_day, COUNT(1)
        FROM op_count_items AS row
        GROUP BY row.days_post_epoch, row.seconds_of_day
        ORDER BY row.days_post_epoch ASC, row.seconds_of_day ASC
""")
df1.show(100, truncate=False)

df2 = spark.sql("""
        SELECT row.days_post_epoch, row.seconds_of_day, COUNT(1)
        FROM collection_agg AS row
        GROUP BY row.days_post_epoch, row.seconds_of_day
        ORDER BY row.days_post_epoch ASC, row.seconds_of_day ASC
""")
df2.show(100, truncate=False)
 
# df3 = spark.sql("""
#         SELECT *
#         FROM activity_labels AS row
#         WHERE row.CollectionName = 's8a80008c7fda9080017fe35c0bd92ff5_8a80808c7fdaa395017fe37baf4a2c57'
#             AND row.to_days_post = 20048
# """)
# df3.show(400000, truncate=False)

spark.stop();
