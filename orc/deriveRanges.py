from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType
from pyspark.sql.functions import col, input_file_name, regexp_extract, lit
from glob import glob
import re

spark = SparkSession.builder \
    .appName("OuterJoin") \
    .config("spark.sql.debug.maxToStringFields", "1024") \
    .getOrCreate()

orc_lake_base_path = "/home/ionadmin/Git/mongo-catalog-tracker/data_lake/orc"

# Now, when you read from output_path, Spark will automatically pick up the partitions
df_re_read1 = spark.read.orc(f"{orc_lake_base_path}/indexOpsAgg")
df_re_read1.createOrReplaceTempView("op_count_item")

df1 = spark.sql("""
    WITH DistinctPartitions AS (
        SELECT
            DISTINCT p.mongo_hostname, p.days_post_epoch, p.seconds_of_day
            FROM op_count_item as p
    ),
    WithPairedRefs AS (
        SELECT p.mongo_hostname, p.days_post_epoch, p.seconds_of_day,
            ROW_NUMBER() OVER (
                PARTITION BY p.mongo_hostname
                ORDER BY p.days_post_epoch ASC, p.seconds_of_day ASC
            ) AS partition_rank,
            MIN(p.days_post_epoch) OVER (
                PARTITION BY p.mongo_hostname
                ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) AS first_days_post,
            FIRST_VALUE(p.seconds_of_day) OVER (
                PARTITION BY p.mongo_hostname
                ORDER BY p.days_post_epoch ASC, p.seconds_of_day ASC
                ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) AS first_seconds_of,
            MAX(p.days_post_epoch) OVER (
                PARTITION BY p.mongo_hostname
                ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) AS last_days_post,
            LAST_VALUE(p.seconds_of_day) OVER (
                PARTITION BY p.mongo_hostname
                ORDER BY p.days_post_epoch ASC, p.seconds_of_day ASC
                ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) AS last_seconds_of,
            LAG(p.days_post_epoch, 1) OVER (
                PARTITION BY p.mongo_hostname
                ORDER BY p.days_post_epoch ASC, p.seconds_of_day ASC
            ) AS prior_days_post,
            LAG(p.seconds_of_day, 1) OVER (
                PARTITION BY p.mongo_hostname
                ORDER BY p.days_post_epoch ASC, p.seconds_of_day ASC
            ) AS prior_seconds_of,
            LEAD(p.days_post_epoch, 1) OVER (
                PARTITION BY p.mongo_hostname
                ORDER BY p.days_post_epoch ASC, p.seconds_of_day ASC
            ) AS next_days_post,
            LEAD(p.seconds_of_day, 1) OVER (
                PARTITION BY p.mongo_hostname
                ORDER BY p.days_post_epoch ASC, p.seconds_of_day ASC
            ) AS next_seconds_of
        FROM DistinctPartitions AS p
    ),
    WithDeltas AS (
        SELECT p.*,
            CASE p.seconds_of_day - p.first_seconds_of >= 0
                WHEN TRUE THEN p.seconds_of_day - p.first_seconds_of
                ELSE 86400 + p.seconds_of_day - p.first_seconds_of
            END AS since_first_seconds_of,
            CASE p.seconds_of_day - p.first_seconds_of >= 0
                WHEN TRUE THEN p.days_post_epoch - p.first_days_post
                ELSE p.days_post_epoch - p.first_days_post - 1
            END AS since_first_days_post,
            CASE p.last_seconds_of - p.seconds_of_day >= 0
                WHEN TRUE THEN p.last_seconds_of - p.seconds_of_day
                ELSE 86400 + p.last_seconds_of - p.seconds_of_day
            END AS until_last_seconds_of,
            CASE p.last_seconds_of - p.seconds_of_day >= 0
                WHEN TRUE THEN p.last_days_post - p.days_post_epoch
                ELSE p.last_days_post - p.days_post_epoch - 1
            END AS until_last_days_post,
            CASE p.seconds_of_day - p.prior_seconds_of >= 0
                WHEN TRUE THEN p.seconds_of_day - p.prior_seconds_of
                ELSE 86400 + p.seconds_of_day - p.prior_seconds_of
            END AS since_prior_seconds_of,
            CASE p.seconds_of_day - p.prior_seconds_of >= 0
                WHEN TRUE THEN p.days_post_epoch - p.prior_days_post
                ELSE p.days_post_epoch - p.prior_days_post - 1
            END AS since_prior_days_post,
            CASE p.next_seconds_of - p.seconds_of_day >= 0
                WHEN TRUE THEN p.next_seconds_of - p.seconds_of_day
                ELSE 86400 + p.next_seconds_of - p.seconds_of_day
            END AS until_next_seconds_of,
            CASE p.next_seconds_of - p.seconds_of_day >= 0
                WHEN TRUE THEN p.next_days_post - p.days_post_epoch
                ELSE p.next_days_post - p.days_post_epoch - 1
            END AS until_next_days_post
        FROM WithPairedRefs AS p
    ),
    EnableLabelExtension AS (
        SELECT p.*,
            ROW_NUMBER() OVER (
                PARTITION BY p.mongo_hostname, p.since_first_days_post
                ORDER BY p.partition_rank ASC
            ) AS label_rel_first_sequence,
            ROW_NUMBER() OVER (
                PARTITION BY p.mongo_hostname, p.until_last_days_post
                ORDER BY p.partition_rank DESC
            ) AS label_rel_last_sequence,
            ROW_NUMBER() OVER (
                PARTITION BY p.mongo_hostname, (p.days_post_epoch - p.first_days_post)
                ORDER BY p.partition_rank ASC
            ) AS label_abs_first_sequence,
            ROW_NUMBER() OVER (
                PARTITION BY p.mongo_hostname, (p.last_days_post - p.days_post_epoch)
                ORDER BY p.partition_rank DESC
            ) AS label_abs_last_sequence
        FROM WithDeltas as p
    ),
    EvalExtensionMethods AS (
        SELECT p.*,
           SUM(CASE WHEN p.label_rel_first_sequence > 1 THEN 1 ELSE 0 END) OVER (
               PARTITION BY p.mongo_hostname
           ) AS rel_first_conflicts,
           SUM(CASE WHEN p.label_rel_last_sequence > 1 THEN 1 ELSE 0 END) OVER (
               PARTITION BY p.mongo_hostname
           ) AS rel_last_conflicts,
           SUM(CASE WHEN p.label_abs_first_sequence > 1 THEN 1 ELSE 0 END) OVER (
               PARTITION BY p.mongo_hostname
           ) AS abs_first_conflicts,
           SUM(CASE WHEN p.label_abs_last_sequence > 1 THEN 1 ELSE 0 END) OVER (
               PARTITION BY p.mongo_hostname
           ) AS abs_last_conflicts
        FROM EnableLabelExtension AS p
    )
    SELECT p.mongo_hostname, p.partition_rank,
        p.days_post_epoch, p.seconds_of_day,
        from_unixtime( (p.days_post_epoch * 86400) + p.seconds_of_day ) AS this_event_at,
        p.prior_days_post, p.prior_seconds_of,
        p.since_prior_days_post, p.since_prior_seconds_of,
        from_unixtime( (p.prior_days_post * 86400) + p.prior_seconds_of ) AS prior_event_at,
        p.next_days_post, p.next_seconds_of,
        p.until_next_days_post, p.until_next_seconds_of,
        from_unixtime( (p.next_days_post * 86400) + p.next_seconds_of ) AS next_event_at,
        p.first_days_post, p.first_seconds_of,
        p.since_first_days_post, p.since_first_seconds_of,
        from_unixtime( (p.first_days_post * 86400) + p.first_seconds_of ) AS first_event_at,
        p.prior_days_post IS NULL AS is_first_partition,
        p.last_days_post, p.last_seconds_of,
        p.until_last_days_post, p.until_last_seconds_of,
        from_unixtime( (p.last_days_post * 86400) + p.last_seconds_of ) AS last_event_at,
        p.next_days_post IS NULL AS is_last_partition,
        CASE
            WHEN p.rel_first_conflicts <= p.abs_first_conflicts
                AND p.label_rel_first_sequence = 1
                THEN p.since_first_days_post
            WHEN p.rel_first_conflicts > p.abs_first_conflicts
                AND p.label_abs_first_sequence = 1
                THEN (p.days_post_epoch - p.first_days_post)
            WHEN p.rel_first_conflicts <= p.abs_first_conflicts
                AND p.label_rel_first_sequence > 1
                THEN CONCAT(p.since_first_days_post, '_', p.label_rel_first_sequence)
            WHEN p.rel_first_conflicts > p.abs_first_conflicts
                AND p.label_abs_first_sequence > 1
                THEN CONCAT((p.days_post_epoch - p.first_days_post), '_', p.label_abs_first_sequence)
            ELSE 'NEVER'
        END AS ascending_label,
        CASE
            WHEN p.rel_last_conflicts <= p.abs_last_conflicts
                AND p.label_rel_last_sequence = 1
                THEN p.until_last_days_post
            WHEN p.rel_last_conflicts > p.abs_last_conflicts
                AND p.label_abs_last_sequence = 1
                THEN p.last_days_post - p.days_post_epoch
            WHEN p.rel_last_conflicts <= p.abs_last_conflicts
                AND p.label_rel_last_sequence > 1
                THEN CONCAT(p.until_last_days_post, '_', p.label_rel_last_sequence)
            WHEN p.rel_last_conflicts > p.abs_last_conflicts
                AND p.label_abs_last_sequence > 1
                THEN CONCAT((p.last_days_post - p.days_post_epoch), '_', p.label_abs_last_sequence)
            ELSE 'NEVER'
        END AS descending_label
    FROM EvalExtensionMethods AS p
""")
df1.write \
    .partitionBy("mongo_hostname") \
    .option("partitionOverwriteMode", "dynamic") \
    .mode("overwrite") \
    .orc(f"{orc_lake_base_path}/partitionRanges/")

df1.show(100, truncate=False)

spark.stop()
