from pyspark.sql import SparkSession

from analysis._types import RedoIndexAggCommand


def main(command: RedoIndexAggCommand, spark=None) -> None:
    warehouse_root = command["warehouse_root"]

    if spark is None:
        spark = (
            SparkSession.builder
            .appName("OuterJoin")  # pyright: ignore[reportAttributeAccessIssue]
            .config("spark.driver.memory", "8g")
            .config("spark.driver.maxResultSize", "6g")
            .config("spark.sql.shuffle.partitions", "500")
            .config("spark.executor.memory", "32g")
            .config("spark.executor.memoryOverhead", "6g")
            .config("spark.memory.fraction", "0.75")
            .getOrCreate()
        )

    spark.read.orc(f"{warehouse_root}/opCountItem").createOrReplaceTempView("op_count_item")
    spark.read.orc(f"{warehouse_root}/collectionOpsAgg").createOrReplaceTempView("collection_agg")

    df3 = spark.sql("""
        SELECT row.mongo_hostname, row.days_post_epoch, row.seconds_of_day, row.IndexName,
            sum(row.UseCount) AS UseCount,
            sum(if(row.UseCount > 0, 1, 0)) AS ActiveInCollectionCount,
            sum(if(collection_agg.ActiveIndexCount > 0, 1, 0)) AS LoadedInCollectionCount,
            count(1) AS PresentInCollectionCount
        FROM op_count_item AS row
            JOIN collection_agg
                ON row.mongo_hostname = collection_agg.mongo_hostname
                AND row.days_post_epoch = collection_agg.days_post_epoch
                AND row.seconds_of_day = collection_agg.seconds_of_day
                AND row.CollectionName = collection_agg.CollectionName
        GROUP BY row.mongo_hostname, row.days_post_epoch, row.seconds_of_day, row.IndexName
        ORDER By row.mongo_hostname ASC, row.days_post_epoch ASC, row.seconds_of_day ASC, sum(row.UseCount) DESC
    """)
    df3.show(210, truncate=False)

    df3.write \
        .partitionBy("mongo_hostname", "days_post_epoch", "seconds_of_day") \
        .option("partitionOverwriteMode", "dynamic") \
        .mode("overwrite") \
        .orc(f"{warehouse_root}/indexOpsAgg/")

    spark.stop()


if __name__ == "__main__":
    import sys
    main({"warehouse_root": sys.argv[1]})
