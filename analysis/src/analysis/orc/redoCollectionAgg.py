from pyspark.sql import SparkSession

from analysis._types import RedoCollectionAggCommand


def main(command: RedoCollectionAggCommand, spark=None) -> None:
    warehouse_root = command["warehouse_root"]

    if spark is None:
        spark = (
            SparkSession.builder
            .appName("OuterJoin")  # pyright: ignore[reportAttributeAccessIssue]
            .config("spark.driver.memory", "4g")
            .config("spark.executor.memory", "8g")
            .config("spark.driver.maxResultSize", "2g")
            .config("spark.executor.memoryOverhead", "2g")
            .getOrCreate()
        )

    spark.read.orc(f"{warehouse_root}/opCountItem").createOrReplaceTempView("op_count_item")

    df1 = spark.sql("""
        SELECT row.mongo_hostname, row.days_post_epoch, row.seconds_of_day, row.CollectionName,
            sum(row.UseCount) AS UseCount,
            sum(if(row.UseCount > 0, 1, 0)) AS ActiveIndexCount,
            count(1) AS TotalIndexCount
        FROM op_count_item AS row
        GROUP BY row.mongo_hostname, row.days_post_epoch, row.seconds_of_day, row.CollectionName
        ORDER By row.mongo_hostname ASC, row.days_post_epoch ASC, row.seconds_of_day ASC, sum(row.UseCount) DESC
    """)
    df1.show(210, truncate=False)
    df1.createOrReplaceTempView("collection_agg")

    df1.write \
        .partitionBy("mongo_hostname", "days_post_epoch", "seconds_of_day") \
        .option("partitionOverwriteMode", "dynamic") \
        .mode("overwrite") \
        .orc(f"{warehouse_root}/collectionOpsAgg/")

    spark.stop()


if __name__ == "__main__":
    import sys
    main({"warehouse_root": sys.argv[1]})
