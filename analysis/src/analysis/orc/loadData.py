from glob import glob

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
from pyspark.sql.functions import col, input_file_name, regexp_extract

from analysis._types import LoadDataCommand


def main(command: LoadDataCommand, spark=None) -> None:
    data_lake_root = command["data_lake_root"]
    warehouse_root = command["warehouse_root"]
    label          = command["label"]

    if spark is None:
        spark = (
            SparkSession.builder
            .appName("OuterJoin")  # pyright: ignore[reportAttributeAccessIssue]
            .config("spark.driver.memory", "4g")
            .config("spark.driver.maxResultSize", "4g")
            .config("spark.executor.memory", "16g")
            .config("spark.executor.memoryOverhead", "2g")
            .getOrCreate()
        )

    def add_partition_columns(base_df):
        df = (
            base_df
            .withColumn("full_dir_name",
                regexp_extract(input_file_name(), r'\/(\d{5}_\d{1,5})\/', 1))
            .withColumn("days_post_epoch_str",
                regexp_extract(col("full_dir_name"), r'(\d{5})_\d{1,5}', 1))
            .withColumn("seconds_of_day_str",
                regexp_extract(col("full_dir_name"), r'\d{5}_(\d{1,5})', 1))
            .withColumn("mongo_hostname",
                regexp_extract(input_file_name(), r'\/([a-zA-Z0-9-_]+)\/\d{5}_\d{1,5}\/', 1))
            .withColumn("days_post_epoch", col("days_post_epoch_str").cast("integer"))
            .withColumn("seconds_of_day",  col("seconds_of_day_str").cast("integer"))
            .drop("full_dir_name", "days_post_epoch_str", "seconds_of_day_str")
        )
        df.printSchema()
        df.show()
        return df

    def process_second_extract(input_dir_path):
        itemSchema = StructType([
            StructField("CollectionName", StringType(),   False),
            StructField("IndexName",      StringType(),   False),
            StructField("UseCount",       IntegerType(),  False),
            StructField("Since",          TimestampType(), False),
        ])
        itemDf = (
            spark.read
            .option("sep", "|")
            .option("header", True)
            .option("timestampFormat", "yyyy-MM-dd HH:mm:ss.SSS Z z")
            .schema(itemSchema)
            .csv(f"{input_dir_path}/allCollectionIndices.csv")
        )
        return [["opCountItem", add_partition_columns(itemDf)]]

    for raw_dir in glob(f"{data_lake_root}/{label}/*_*"):
        for name, df in process_second_extract(raw_dir):
            df.write \
                .partitionBy("mongo_hostname", "days_post_epoch", "seconds_of_day") \
                .option("partitionOverwriteMode", "dynamic") \
                .mode("overwrite") \
                .orc(f"{warehouse_root}/{name}/")

    spark.stop()


if __name__ == "__main__":
    import sys
    main({"data_lake_root": sys.argv[1], "warehouse_root": sys.argv[2], "label": sys.argv[3]})
