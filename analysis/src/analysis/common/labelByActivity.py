from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType
from pyspark.sql.functions import col, input_file_name, regexp_extract, lit
from glob import glob
import re

# odef main(command: dict, spark=None) -> None:
    # if spark is None:
 
spark = SparkSession.builder \
    .appName("OuterJoin") \
    .config("spark.driver.memory", "4g") \
    .config("spark.executor.memory", "16g") \
    .config("spark.executor.memoryOverhead", "2g") \
    .getOrCreate()

orc_lake_base_path = "/home/ionadmin/Git/mongo-catalog-tracker/data_lake/orc"

# Now, when you read from output_path, Spark will automatically pick up the partitions
df_re_read1 = spark.read.orc(f"{orc_lake_base_path}/collectionOpsAgg")
df_re_read2 = spark.read.orc(f"{orc_lake_base_path}/opCountItem")
df_re_read3 = spark.read.orc(f"{orc_lake_base_path}/partitionRanges")

df_re_read1.createOrReplaceTempView("collection_agg")
df_re_read2.createOrReplaceTempView("op_count_item")
df_re_read3.createOrReplaceTempView("partition_ranges")

# Compute and store raw per-sample collection state labels.
# Persisting at the single-interval categories facilitates computing Sankey with any
# number of days used for the inactivity threshold using window functions in terms of
# those labels that are more easily understood.
df1 = spark.sql("""
    WITH AllKnownCollectionsEver AS (
        SELECT v.mongo_hostname, v.CollectionName,
            from_unixtime(MIN((v.days_post_epoch * 86400) + v.seconds_of_day)) AS IsKnownAsOf
        FROM op_count_item AS v
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
            (v.IsKnownAsOf > p.first_event_at) AS IsFutureCollection
        FROM AllKnownCollectionsEver AS v
            JOIN partition_ranges AS p
                ON v.mongo_hostname = p.mongo_hostname
            LEFT OUTER JOIN collection_agg AS row
                ON p.mongo_hostname = row.mongo_hostname
                AND p.days_post_epoch = row.days_post_epoch
                AND p.seconds_of_day = row.seconds_of_day
                AND v.CollectionName = row.CollectionName
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
    )
    SELECT
        row.mongo_hostname,
        row.days_post_epoch,
        row.seconds_of_day,
        row.partition_rank AS PartitionRank,
        row.CollectionName,
        row.UseCount,
        row.CollectionUseStatus
    FROM ClassifyUse AS row
""")

df1.write \
    .partitionBy("mongo_hostname", "days_post_epoch", "seconds_of_day") \
    .option("partitionOverwriteMode", "dynamic") \
    .mode("overwrite") \
    .orc(f"{orc_lake_base_path}/collectionDiscreteLabels/")

spark.stop()
