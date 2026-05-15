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
df_re_read1 = spark.read.parquet(f"{parquet_lake_base_path}/collectionActivityPolicyLabels")
df_re_read1.createOrReplaceTempView("collection_activity_policy_labels")

df1 = spark.sql(f"""
    WITH PolicyWithLag AS (
        SELECT row.*,
            LAG(row.ActivityPolicyStatus, 1) OVER (
                PARTITION BY row.mongo_hostname, row.CollectionName
                ORDER BY row.PartitionRank ASC
            ) AS PriorActivityPolicyStatus,
            LAG(row.PartitionRank, 1) OVER (
                PARTITION BY row.mongo_hostname, row.CollectionName
                ORDER BY row.PartitionRank ASC
            ) AS PriorPartitionRank
        FROM collection_activity_policy_labels AS row
    ),
    PolicyNodes AS (
        SELECT row.*,
            CONCAT(row.PriorActivityPolicyStatus, row.PriorPartitionRank) AS StateBefore,
            CONCAT(row.ActivityPolicyStatus, row.PartitionRank) AS StateAfter
        FROM PolicyWithLag AS row
        WHERE row.PriorActivityPolicyStatus IS NOT NULL
    )
        SELECT
            row.mongo_hostname,
            row.StateBefore,
            row.StateAfter,
            COUNT(1) AS EdgeCount
        FROM PolicyNodes AS row
        GROUP BY
            row.mongo_hostname,
            row.PartitionRank,
            row.StateBefore,
            row.StateAfter
        ORDER BY
            row.mongo_hostname ASC,
            row.PartitionRank ASC,
            EdgeCount DESC,
            row.StateBefore ASC,
            row.StateAfter ASC
""")

df1.createOrReplaceTempView('transition_counts')

        # row.mongo_hostname,
# df2 = spark.sql("""
#     SELECT 
#         CONCAT(row.StateBefore, ',', row.StateAfter, ',', row.EdgeCount) AS samkey_row
#     FROM transition_counts as row
# """)
# df2.show(5000, truncate=False)

df3 = spark.sql("""
    SELECT 
        CONCAT(row.StateBefore, ' [', row.EdgeCount, '] ', row.StateAfter) AS samkey_row
    FROM transition_counts as row
""")
df3.show(5000, truncate=False)

spark.stop();

          # CASE
          #     WHEN row.PriorActivityPolicyStatus IN ('NotCreated', 'NotLoaded')
          #         THEN CONCAT(row.PriorActivityPolicyStatus, row.PriorPartitionRank)
          #     ELSE CONCAT(row.PriorActivityPolicyStatus, row.PriorPartitionRank)
          # END AS StateBefore,
          # CASE
          #     WHEN row.ActivityPolicyStatus IN ('Deleted', 'Inactive')
          #         THEN row.ActivityPolicyStatus
          #     ELSE CONCAT(row.ActivityPolicyStatus, row.PartitionRank)
          # END AS StateAfter
          # AND row.PriorActivityPolicyStatus NOT IN ('Deleted', 'Inactive')
          # AND row.ActivityPolicyStatus NOT IN ('NotCreated', 'NotLoaded')
