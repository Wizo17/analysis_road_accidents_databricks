from pyspark import pipelines as dp
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import col, current_timestamp, to_date

volume_path = spark.conf.get("volume_ingestion_path")

schema = StructType([
    StructField("accident_id", StringType(), True),
    StructField("vehicle_convention_letter", StringType(), True),
    StructField("accident_year", StringType(), True),
    StructField("current_administrative_location_territory_name", StringType(), True),
    StructField("accident_type_description_old", StringType(), True),
    StructField("cnit", StringType(), True),
    StructField("vehicle_category", StringType(), True),
    StructField("vehicle_age", StringType(), True)
])

@dp.table(
    name="baac_vehicle_registration_raw",
    comment="Raw BAAC vehicle registration data with ingestion metadata.",
    partition_cols=["ingestion_date"]
)
def baac_vehicle_registration_raw():
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

