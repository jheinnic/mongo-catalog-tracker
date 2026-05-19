import argparse
import re

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType
from pyspark.sql.functions import col, input_file_name, regexp_extract, lit
from glob import glob

_KNOWN_SCHEMES = re.compile(r'^(s3a|s3|hdfs|gs|abfs|file)://')

def _warehouse_path(value):
    if not value:
        raise argparse.ArgumentTypeError("warehouse_root must not be empty")
    if not (value.startswith('/') or _KNOWN_SCHEMES.match(value)):
        raise argparse.ArgumentTypeError(
            f"{value!r} is not an absolute path or a recognised scheme "
            f"(s3a://, hdfs://, gs://, etc.)"
        )
    return value

parser = argparse.ArgumentParser(description="Label collections by inactivity policy")
parser.add_argument("warehouse_root", type=_warehouse_path,
                    help="Root path of the data lake warehouse")
parser.add_argument("n_inactivity", type=int, nargs="?", default=14,
                    help="Lookback window in samples for inactivity detection (default: 14)")
args, _ = parser.parse_known_args()
orc_lake_base_path = args.warehouse_root
n_inactivity = args.n_inactivity

spark = SparkSession.builder \
    .appName("OuterJoin") \
    .config("spark.driver.memory", "4g") \
    .config("spark.executor.memory", "16g") \
    .config("spark.executor.memoryOverhead", "2g") \
    .getOrCreate()

df_re_read1 = spark.read.orc(f"{orc_lake_base_path}/collectionDiscreteLabels")
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
    .orc(f"{orc_lake_base_path}/collectionActivityPolicyLabels/")

spark.stop()
