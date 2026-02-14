from pyspark import pipelines as dp
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import col, current_timestamp, to_date

volume_path = spark.conf.get("volume_ingestion_path")

schema = StructType([
    StructField("accident_num", StringType(), True),
    StructField("road_category", StringType(), True),
    StructField("road_name", StringType(), True),
    StructField("road_index_number", StringType(), True),
    StructField("road_alphanumeric_index", StringType(), True),
    StructField("road_traffic_system", StringType(), True),
    StructField("total_number_of_traffic_lanes", StringType(), True),
    StructField("reserved_lane", StringType(), True),
    StructField("road_gradient", StringType(), True),
    StructField("number_of_associated_pr", StringType(), True),
    StructField("distance_to_pr_in_meters", StringType(), True),
    StructField("map", StringType(), True),
    StructField("width_of_central_reservation_tpc", StringType(), True),
    StructField("width_of_roadway", StringType(), True),
    StructField("surface_condition", StringType(), True),
    StructField("layout_infrastructure", StringType(), True),
    StructField("location_of_accident", StringType(), True),
    StructField("maximum_speed_limit", StringType(), True)
])

@dp.table(
    name="accident_sites_raw",
    comment="Raw accident sites with ingestion metadata.",
    partition_cols=["ingestion_date"]
)
def accident_sites_raw():
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

