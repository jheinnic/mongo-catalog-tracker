from pyspark.sql import SparkSession


def main(command: dict, spark=None) -> None:
    warehouse_root = command["warehouse_root"]
    n_inactivity   = command.get("n_inactivity", 14)

    if spark is None:
        spark = (
            SparkSession.builder
            .appName("OuterJoin")  # pyright: ignore[reportAttributeAccessIssue]
            .config("spark.driver.memory", "4g")
            .config("spark.executor.memory", "16g")
            .config("spark.executor.memoryOverhead", "2g")
            .getOrCreate()
        )

    spark.read.parquet(f"{warehouse_root}/collectionDiscreteLabels") \
        .createOrReplaceTempView("collection_discrete_labels")

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
        .parquet(f"{warehouse_root}/collectionActivityPolicyLabels/")

    spark.stop()


if __name__ == "__main__":
    import sys
    n = int(sys.argv[2]) if len(sys.argv) > 2 else 14
    main({"warehouse_root": sys.argv[1], "n_inactivity": n})
