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
    .config("spark.executor.memoryOverhead", "2g") \
    .getOrCreate()

parquet_lake_base_path = "/home/ionadmin/Git/mongo-catalog-tracker/data_lake/parquet"
n_inactivity = 14

df_re_read1 = spark.read.parquet(f"{parquet_lake_base_path}/collectionDiscreteLabels")
df_re_read1.createOrReplaceTempView("collection_discrete_labels")

# Derive policy-driven state labels from the raw ClassifyUse labels,
# then count transitions between adjacent samples to produce Sankey edges.
#
# Policy rules (applied with a lookback window of n_inactivity samples):
#   NotCreated / NotLoaded / Deleted / Removed  -> NotCreated / NotLoaded / Deleted (Removed merged into Deleted)
#   Current or Accessed                         -> Active
#   Forgotten or Ignored, with at least one
#     Current/Accessed in the prior N samples   -> Idle
#   Forgotten or Ignored, with no prior N active,
#     but with future Active ahead              -> Restorable
#   Forgotten or Ignored, with no prior N active
#     and no future Active                      -> Inactive
#
# Node label formatting:
#   NotCreated, NotLoaded, Deleted, Inactive    -> state name only (origin/terminal nodes)
#   Active, Idle, Restorable                    -> {state}{PartitionRank} (flow nodes)
df1 = spark.sql(f"""
    WITH PolicyWindow AS (
        SELECT row.*,
            SUM(CASE WHEN row.CollectionUseStatus IN ('Current', 'Accessed') THEN 1 ELSE 0 END)
                OVER (
                    PARTITION BY row.mongo_hostname, row.CollectionName
                    ORDER BY row.PartitionRank ASC
                    ROWS BETWEEN {n_inactivity} PRECEDING AND 1 PRECEDING
                ) AS ActiveCountInPriorN,
            SUM(CASE WHEN row.CollectionUseStatus IN ('Current', 'Accessed') THEN 1 ELSE 0 END)
                OVER (
                    PARTITION BY row.mongo_hostname, row.CollectionName
                    ORDER BY row.PartitionRank ASC
                    ROWS BETWEEN 1 FOLLOWING AND UNBOUNDED FOLLOWING
                ) AS FutureActiveCount
        FROM collection_discrete_labels AS row
    ),
    PolicyLabel AS (
        SELECT row.*,
            CASE
                WHEN row.CollectionUseStatus IN ('NotCreated', 'NotLoaded', 'Deleted')
                    THEN row.CollectionUseStatus
                WHEN row.CollectionUseStatus = 'Removed'
                    THEN 'Deleted'
                WHEN row.CollectionUseStatus IN ('Current', 'Accessed')
                    THEN 'Active'
                WHEN row.CollectionUseStatus IN ('Forgotten', 'Ignored')
                    AND row.ActiveCountInPriorN > 0
                    THEN 'Idle'
                WHEN row.CollectionUseStatus IN ('Forgotten', 'Ignored')
                    AND row.FutureActiveCount > 0
                    THEN 'Restorable'
                WHEN row.CollectionUseStatus IN ('Forgotten', 'Ignored')
                    THEN 'Inactive'
                ELSE 'ERROR'
            END AS ActivityPolicyStatus
        FROM PolicyWindow AS row
    )
    SELECT
        row.mongo_hostname,
        row.days_post_epoch,
        row.seconds_of_day,
        row.PartitionRank,
        row.CollectionName,
        row.UseCount,
        row.CollectionUseStatus,
        row.ActivityPolicyStatus
    FROM PolicyLabel AS row
""")

df1.createOrReplaceTempView("collection_activity_policy_labels")

df1.write \
    .partitionBy("mongo_hostname") \
    .option("partitionOverwriteMode", "dynamic") \
    .mode("overwrite") \
    .parquet(f"{parquet_lake_base_path}/collectionActivityPolicyLabels/")

# df2 = spark.sql(f"""
#     WITH PolicyWindow AS (
#     PolicyWithLag AS (
#         SELECT row.*,
#             LAG(row.ActivityPolicyStatus, 1) OVER (
#                 PARTITION BY row.mongo_hostname, row.CollectionName
#                 ORDER BY row.PartitionRank ASC
#             ) AS PriorActivityPolicyStatus,
#             LAG(row.PartitionRank, 1) OVER (
#                 PARTITION BY row.mongo_hostname, row.CollectionName
#                 ORDER BY row.PartitionRank ASC
#             ) AS PriorPartitionRank
#         FROM collection_activity_policy_labels AS row
#         WHERE row.PriorActivityPolicyStatus IS NOT NULL
#     ),
#     PolicyNodes AS (
#         SELECT row.*,
#             CASE
#                 WHEN row.PriorActivityPolicyStatus IN ('NotCreated', 'NotLoaded')
#                     THEN row.PriorActivityPolicyStatus
#                 ELSE CONCAT(row.PriorActivityPolicyStatus, row.PriorPartitionRank)
#             END AS FromNode,
#             CASE
#                 WHEN row.ActivityPolicyStatus IN ('Deleted', 'Inactive')
#                     THEN row.ActivityPolicyStatus
#                 ELSE CONCAT(row.ActivityPolicyStatus, row.PartitionRank)
#             END AS ToNode
#         FROM PolicyWithLag AS row
#         WHERE row.PriorActivityPolicyStatus NOT IN ('Deleted', 'Inactive')
#           AND row.ActivityPolicyStatus NOT IN ('NotCreated', 'NotLoaded')
#     )
#     SELECT
#         row.mongo_hostname,
#         row.PartitionRank AS CurrentPartitionRank,
#         row.FromNode,
#         row.ToNode,
#         COUNT(1) AS EdgeCount
#     FROM PolicyNodes AS row
#     GROUP BY
#         row.mongo_hostname,
#         row.PartitionRank,
#         row.FromNode,
#         row.ToNode,
#     ORDER BY
#         row.mongo_hostname ASC,
#         row.PartitionRank ASC,
#         COUNT(1) DESC,
#         row.FromNode ASC,
#         row.ToNode ASC
# """)
# 
# df2.write \
#     .partitionBy("mongo_hostname") \
#     .option("partitionOverwriteMode", "dynamic") \
#     .mode("overwrite") \
#     .parquet(f"{parquet_lake_base_path}/basicSamkey/")

spark.stop()
