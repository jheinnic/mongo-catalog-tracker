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
    .config("spark.driver.maxResultSize", "4g") \
    .config("spark.executor.memoryOverhead", "4g") \
    .getOrCreate()

def add_partition_columns(base_df):
    df_with_partitions = base_df.withColumn(
        "full_dir_name",
        regexp_extract(input_file_name(), r'\/(\d{5}_\d{1,5})\/', 1)
    ).withColumn(
        "days_post_epoch_str",
        regexp_extract(col("full_dir_name"), r'(\d{5})_\d{1,5}', 1)
    ).withColumn(
        "seconds_of_day_str",
        regexp_extract(col("full_dir_name"), r'\d{5}_(\d{1,5})', 1)
    ).withColumn(
        "mongo_hostname",
        regexp_extract(input_file_name(), r'\/([a-zA-Z0-9-_]+)\/\d{5}_\d{1,5}\/', 1)
    )
    
    # Cast to integer types for better querying
    df_with_partitions = df_with_partitions.withColumn(
        "days_post_epoch", col("days_post_epoch_str").cast("integer")
    ).withColumn(
        "seconds_of_day", col("seconds_of_day_str").cast("integer")
    )
    
    # You might want to drop the temporary string columns and full_dir_name
    df_with_partitions = df_with_partitions.drop("full_dir_name", "days_post_epoch_str", "seconds_of_day_str")
    
    df_with_partitions.printSchema()
    df_with_partitions.show()

    # Add the partition columns as actual columns in the DataFrame
    return df_with_partitions

    # Extract days_post_epoch and seconds_of_day from the directory name

# Example of processing a single "second" directory
# In a batch job, you'd feed this function with each directory path
def process_second_extract(spark_session, input_dir_path):
    # Describe the CSV structures
    itemSchema = StructType([
        StructField("CollectionName", StringType(), False),
        StructField("IndexName", StringType(), False),
        StructField("UseCount", IntegerType(), False),
        StructField("Since", DateType(), False)
    ])
    
    # Read the two sorted files into DataFrames
    itemDf = spark.read.csv(f"{input_dir_path}/allByCollection.csv", itemSchema, "|", header=True)

    return [
        ["opCountItem", add_partition_columns(itemDf)]
    ]
    
# --- Main processing logic ---
# Imagine you have a list of your source directories
source_base_path = "/home/ionadmin/Documents/MongoAnalysis/IndexOpCount/index"

# Define your target Orc lake path
orc_lake_base_path = "/home/ionadmin/Git/mongo-catalog-tracker/data_lake/orc"

all_raw_second_dirs = glob(f"{source_base_path}/mongo02/*_*")
all_raw_second_dirs = glob(f"{source_base_path}/mongo02/20242_15218")
all_raw_second_dirs = glob(f"{source_base_path}/mongo02/20026_74820")
all_raw_second_dirs = glob(f"{source_base_path}/mongo02/20249_10442")
all_raw_second_dirs = glob(f"{source_base_path}/mongo04/20027_12328")

for raw_dir in all_raw_second_dirs:
    for name_df_pair in process_second_extract(spark, raw_dir):
        # Save as a partitioned Orc table
        df_to_write = name_df_pair[1]
        df_to_write.write \
            .partitionBy("mongo_hostname", "days_post_epoch", "seconds_of_day") \
            .option("partitionOverwriteMode", "dynamic") \
            .mode("overwrite") \
            .orc(f"{orc_lake_base_path}/{name_df_pair[0]}/")

spark.stop();
