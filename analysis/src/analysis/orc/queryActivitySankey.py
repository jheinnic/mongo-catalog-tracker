from pyspark.sql import SparkSession

from analysis._types import QueryActivitySankeyCommand


def main(command: QueryActivitySankeyCommand, spark=None) -> None:
    warehouse_root = command["warehouse_root"]

    if spark is None:
        spark = (
            SparkSession.builder
            .appName("OuterJoin")  # pyright: ignore[reportAttributeAccessIssue]
            .config("spark.driver.memory", "4g")
            .config("spark.executor.memory", "8g")
            .config("spark.executor.memoryOverhead", "4g")
            .getOrCreate()
        )

    spark.read.orc(f"{warehouse_root}/collectionActivityPolicyLabels") \
        .createOrReplaceTempView("collection_activity_policy_labels")

    df1 = spark.sql("""
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
            row.StateBefore ASC,
            row.StateAfter ASC
    """)

    df1.createOrReplaceTempView("transition_counts")

    df3 = spark.sql("""
        SELECT
            CONCAT(row.StateBefore, ' [', row.EdgeCount, '] ', row.StateAfter) AS sankey_row
        FROM transition_counts as row
    """)
    df3.show(5000, truncate=False)

    spark.stop()


if __name__ == "__main__":
    import sys
    main({"warehouse_root": sys.argv[1]})
