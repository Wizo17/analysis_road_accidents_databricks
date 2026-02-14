from pyspark import pipelines as dp
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import col, current_timestamp, to_date

volume_path = spark.conf.get("volume_ingestion_path")

schema = StructType([
    StructField("accident_num", StringType(), True),
    StructField("vehicle_id", StringType(), True),
    StructField("vehicle_num", StringType(), True),
    StructField("vehicle_direction", IntegerType(), True),
    StructField("vehicle_category", IntegerType(), True),
    StructField("fixed_obstacle_struck", IntegerType(), True),
    StructField("moving_obstacle_struck", IntegerType(), True),
    StructField("initial_point_of_impact", IntegerType(), True),
    StructField("main_maneuver_prior_accident", IntegerType(), True),
    StructField("vehicle_engine_type", IntegerType(), True),
    StructField("number_of_occupants", IntegerType(), True)
])

@dp.table(
    name="accident_vehicles_raw",
    comment="Raw accident vehicles with ingestion metadata.",
    partition_cols=["ingestion_date"]
)
def accident_vehicles_raw():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.useNotifications", "false")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("sep", ";")
        .option("quote", '"')
        .option("escape", '"')
        .schema(schema)
        .load(f"{volume_path}/*.csv")
        .withColumn("source_file", col("_metadata.file_path"))
        .withColumn("ingestion_timestamp", current_timestamp())
        .withColumn("ingestion_date", to_date(current_timestamp()))
    )

