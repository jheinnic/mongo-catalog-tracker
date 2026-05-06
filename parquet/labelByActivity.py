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

# Now, when you read from output_path, Spark will automatically pick up the partitions
df_re_read1 = spark.read.parquet(f"{parquet_lake_base_path}/collectionOpsAgg")
df_re_read2 = spark.read.parquet(f"{parquet_lake_base_path}/opCountItem")
df_re_read3 = spark.read.parquet(f"{parquet_lake_base_path}/partitionRanges")

df_re_read1.createOrReplaceTempView("collection_agg")
df_re_read2.createOrReplaceTempView("op_count_item")
df_re_read3.createOrReplaceTempView("partition_ranges")

df1 = spark.sql("""
    WITH AllKnownCollectionsEver AS (
        SELECT v.mongo_hostname, v.CollectionName, MIN(v.Since) AS IsKnownAsOf
        FROM op_count_item AS v
        WHERE v.CollectionName NOT LIKE 'audit%'
            AND v.CollectionName NOT LIKE '%AnnotationSource.metas%'
        GROUP BY v.mongo_hostname, v.CollectionName
        HAVING SUM(v.UseCount) > 0
    ),
    AllBeforeAndAfter AS (
        SELECT p.*,
            v.CollectionName, v.IsKnownAsOf, 
            row.ActiveIndexCount, row.TotalIndexCount, row.UseCount,
            LAG(row.UseCount, 1) OVER (
                PARTITION BY row.mongo_hostname, row.CollectionName
                ORDER BY p.partition_rank
            ) AS UseCountBefore,
            (row.UseCount IS NULL
                AND v.IsKnownAsOf < p.this_event_at) AS IsPurgedCollection,
            future.UseCount IS NULL AS IsFutureCollection
        FROM AllKnownCollectionsEver AS v
            JOIN partition_ranges AS p
                ON v.mongo_hostname = p.mongo_hostname
            LEFT OUTER JOIN collection_agg AS row
                ON p.mongo_hostname = row.mongo_hostname
                AND p.days_post_epoch = row.days_post_epoch
                AND p.seconds_of_day = row.seconds_of_day
                AND v.CollectionName = row.CollectionName
            LEFT OUTER JOIN collection_agg AS future
                ON p.mongo_hostname = future.mongo_hostname
                AND p.first_days_post = future.days_post_epoch
                AND p.first_seconds_of = future.seconds_of_day
                AND v.CollectionName = future.CollectionName
    ),
    ClassifyUse AS (
        SELECT row.*,
            CASE
                WHEN row.UseCountBefore IS NULL
                    AND row.UseCount IS NULL
                    AND row.IsFutureCollection
                    AND NOT row.IsPurgedCollection
                    THEN 'NotCreated'
                WHEN row.UseCount IS NULL
                    AND row.IsPurgedCollection
                    AND row.IsFutureCollection
                    THEN 'Deleted'
                WHEN row.UseCount IS NULL
                    AND row.IsPurgedCollection
                    AND NOT row.IsFutureCollection
                    THEN 'Removed'
                WHEN row.UseCountBefore IS NULL
                    AND row.UseCount IS NOT NULL
                    AND row.IsFutureCollection
                    AND NOT row.IsPurgedCollection
                    THEN 'Current'
                WHEN row.UseCountBefore IS NULL
                    AND row.UseCount > 0
                    AND NOT row.IsFutureCollection
                    AND NOT row.IsPurgedCollection
                    THEN 'Accessed'
                WHEN row.UseCount = 0
                    AND NOT row.IsFutureCollection
                    AND NOT row.IsPurgedCollection
                    THEN 'NotLoaded'
                WHEN row.UseCountBefore < row.UseCount
                    AND row.IsFutureCollection
                    AND NOT row.IsPurgedCollection
                    THEN 'Current'
                WHEN row.UseCountBefore < row.UseCount
                    AND NOT row.IsFutureCollection
                    AND NOT row.IsPurgedCollection
                    THEN 'Accessed'
                WHEN row.UseCountBefore > 0
                    AND row.UseCount = row.UseCountBefore
                    AND row.IsFutureCollection
                    AND NOT row.IsPurgedCollection
                    THEN 'Forgotten'
                WHEN row.UseCountBefore > 0
                    AND row.UseCount = row.UseCountBefore
                    AND NOT row.IsFutureCollection
                    AND NOT row.IsPurgedCollection
                    THEN 'Ignored'
                ELSE 'ERROR'
            END AS CollectionUseStatus
        FROM AllBeforeAndAfter AS row
    ),
    CopyPreviousStatus AS (
        SELECT row.*,
            LAG(row.CollectionUseStatus, 1) OVER (
                PARTITION BY row.mongo_hostname, row.CollectionName
                ORDER BY row.partition_rank ASC
            ) AS CollectionUseStatusBefore
        FROM ClassifyUse AS row
    ),
    MarkStatusChanged AS (
        SELECT row.*,
            CASE
                WHEN row.CollectionUseStatusBefore IS DISTINCT FROM row.CollectionUseStatus
                     THEN row.partition_rank
                ELSE NULL
            END AS ChangedAtRank,
            CASE
                WHEN row.CollectionUseStatusBefore IS DISTINCT FROM row.CollectionUseStatus
                     THEN row.CollectionUseStatus
                ELSE NULL
            END AS ChangedUseStatusTo,
            LAST_VALUE(row.partition_rank) IGNORE NULLS OVER (
                PARTITION BY row.mongo_hostname, row.CollectionName, 
                    row.CollectionUseStatusBefore IS DISTINCT FROM row.CollectionUseStatus
                ORDER BY row.partition_rank ASC
                ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
            ) AS PriorChangeRankTwo,
            LAST_VALUE(row.CollectionUseStatus) IGNORE NULLS OVER (
                PARTITION BY row.mongo_hostname, row.CollectionName,
                    row.CollectionUseStatusBefore IS DISTINCT FROM row.CollectionUseStatus
                ORDER BY row.partition_rank ASC
                ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
            ) AS PriorChangeStatusTwo
        FROM CopyPreviousStatus AS row
    ),
    NavigateChanges AS (
        SELECT row.*,
            LAST_VALUE(row.ChangedAtRank) IGNORE NULLS OVER (
                PARTITION BY row.mongo_hostname, row.CollectionName
                ORDER BY row.partition_rank ASC
                ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
            ) AS PriorChangeRank,
            LAST_VALUE(row.ChangedUseStatusTo) IGNORE NULLS OVER (
                PARTITION BY row.mongo_hostname, row.CollectionName
                ORDER BY row.partition_rank ASC
                ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
            ) AS PriorChangeStatus
        FROM MarkStatusChanged AS row
    )
    SELECT row.mongo_hostname, row.CollectionName,
        row.ascending_label AS AscendingLabel,
        row.this_event_at AS ThisEventAt,
        row.days_post_epoch, row.seconds_of_day,
        row.partition_rank AS CurrentPartitionRank,
        row.prior_event_at AS PriorEventAt,
        row.UseCountBefore, row.UseCount,
        row.ChangedAtRank, row.ChangedUseStatusTo,
        row.CollectionUseStatus, row.CollectionUseStatusBefore,
        row.IsFutureCollection, row.IsPurgedCollection,
        row.PriorChangeRank, row.PriorChangeStatus,
        row.PriorChangeRankTwo, row.PriorChangeStatusTwo,
        row.IsKnownAsOf
    FROM NavigateChanges AS row
""")
df1.createOrReplaceTempView("LabelEnrichment")
df1.show(750000, truncate=False)
  
# Save as a partitioned Parquet table
df1.write \
    .partitionBy("mongo_hostname", "days_post_epoch", "seconds_of_day") \
    .option("partitionOverwriteMode", "dynamic") \
    .mode("overwrite") \
    .parquet(f"{parquet_lake_base_path}/collectionActivityLabels/")

# Group and count for a flow that carries stepwise changes to each pool
# at each stage.  Pools show the absolute quantity present at each step
df3 = spark.sql("""
    SELECT
        row.mongo_hostname, row.AscendingLabel,
        row.CurrentPartitionRank, row.ThisEventAt,
        p.ascending_label AS PriorAscendingLabel,
        row.CollectionUseStatusBefore, row.CollectionUseStatus,
        row.PriorEventAt, COUNT(1) AS EdgeCount
    FROM LabelEnrichment AS row
        JOIN partition_ranges AS p
            ON p.mongo_hostname = row.mongo_hostname
            AND p.this_event_at = row.PriorEventAt
    GROUP BY
        row.mongo_hostname, row.AscendingLabel,
        row.CurrentPartitionRank, p.ascending_label,
        row.ThisEventAt, row.PriorEventAt,
        row.CollectionUseStatusBefore, row.CollectionUseStatus
""")
  
# Save as a partitioned Orc table
df3.write \
    .partitionBy("mongo_hostname") \
    .option("partitionOverwriteMode", "dynamic") \
    .mode("overwrite") \
    .parquet(f"{parquet_lake_base_path}/basicSamkey/")

# Group and count for a flow that carries stepwise changes to each pool
# at each stage.  Pools show the relative change occuring at each step
df4 = spark.sql("""
    SELECT row.mongo_hostname, row.AscendingLabel,
        p.ascending_label AS PriorAscendingLabel,
        row.ChangedAtRank, row.CollectionUseStatus,
        row.PriorChangeRank, row.PriorChangeStatus,
        COUNT(1) AS EdgeCount
    FROM LabelEnrichment AS row
        JOIN partition_ranges AS p
            ON p.mongo_hostname = row.mongo_hostname
            AND p.partition_rank = row.PriorChangeRank
    WHERE row.ChangedAtRank IS NOT NULL
    GROUP BY
        row.mongo_hostname, row.AscendingLabel,
        row.ChangedAtRank, row.CollectionUseStatus,
        row.PriorChangeStatus, row.PriorChangeRank,
        p.ascending_label
    ORDER BY
        row.mongo_hostname ASC, row.ChangedAtRank ASC,
        COUNT(1) DESC, row.CollectionUseStatus ASC,
        row.PriorChangeRank ASC, row.PriorChangeStatus ASC
""")
  
# Save as a partitioned Orc table
df4.write \
    .partitionBy("mongo_hostname") \
    .option("partitionOverwriteMode", "dynamic") \
    .mode("overwrite") \
    .parquet(f"{parquet_lake_base_path}/advancedSamkey/")

spark.stop();
#     IdentifyFinalChanges AS (
#         SELECT row.*,
#             CASE
#                 WHEN row.partition_rank = row.PriorChangeRank
#                     THEN row.CollectionUseStatus
#                 ELSE NULL
#             END AS PriorChangePropagationState,
#             CASE
#                 WHEN row.partition_rank = row.NextChangeRank
#                     THEN row.CollectionUseStatus
#                 ELSE NULL
#             END AS NextChangePropagationState,
#             CASE
#                 WHEN row.partition_rank = row.FirstChangeRank
#                     THEN row.CollectionUseStatus
#                 ELSE NULL
#             END AS FirstChangePropagationState,
#             CASE
#                 WHEN row.partition_rank = row.LastChangeRank
#                     THEN row.CollectionUseStatus
#                 ELSE NULL
#             END AS LastChangePropagationState
#         FROM TrackFinalChanges AS row
#     ),
