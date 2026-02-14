from pyspark import pipelines as dp
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import col, current_timestamp, to_date

volume_path = spark.conf.get("volume_ingestion_path")

schema = StructType([
    StructField("accident_num", StringType(), True),
    StructField("user_id", StringType(), True),
    StructField("vehicle_id", StringType(), True),
    StructField("vehicle_num", StringType(), True),
    StructField("seat_position", StringType(), True),
    StructField("user_category", StringType(), True),
    StructField("injury_severity", StringType(), True),
    StructField("user_gender", StringType(), True),
    StructField("user_birth_year", StringType(), True),
    StructField("reason_for_travel", StringType(), True),
    StructField("safety_equipment_1", StringType(), True),
    StructField("safety_equipment_2", StringType(), True),
    StructField("safety_equipment_3", StringType(), True),
    StructField("pedestrian_location", StringType(), True),
    StructField("pedestrian_action", StringType(), True),
    StructField("pedestrian_alone", StringType(), True)
])

@dp.table(
    name="accident_victims_raw",
    comment="Raw accident victims with ingestion metadata.",
    partition_cols=["ingestion_date"]
)
def accident_victims_raw():
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

