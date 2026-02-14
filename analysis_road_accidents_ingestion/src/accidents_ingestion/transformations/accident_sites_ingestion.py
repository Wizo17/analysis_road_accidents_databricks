from pyspark import pipelines as dp
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import col, current_timestamp, to_date

volume_path = spark.conf.get("volume_ingestion_path")

schema = StructType([
    StructField("accident_num", StringType(), True),
    StructField("road_category", IntegerType(), True),
    StructField("road_name", StringType(), True),
    StructField("road_index_number", StringType(), True),
    StructField("road_alphanumeric_index", StringType(), True),
    StructField("road_traffic_system", IntegerType(), True),
    StructField("total_number_of_traffic_lanes", IntegerType(), True),
    StructField("reserved_lane", IntegerType(), True),
    StructField("road_gradient", IntegerType(), True),
    StructField("number_of_associated_pr", IntegerType(), True),
    StructField("distance_to_pr_in_meters", IntegerType(), True),
    StructField("map", IntegerType(), True),
    StructField("width_of_central_reservation_tpc", IntegerType(), True),
    StructField("width_of_roadway", IntegerType(), True),
    StructField("surface_condition", IntegerType(), True),
    StructField("layout_infrastructure", IntegerType(), True),
    StructField("location_of_accident", IntegerType(), True),
    StructField("maximum_speed_limit", IntegerType(), True)
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

