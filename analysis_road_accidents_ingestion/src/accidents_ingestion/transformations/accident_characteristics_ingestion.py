from pyspark import pipelines as dp
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import col, current_timestamp, to_date

volume_path = spark.conf.get("volume_ingestion_path")

schema = StructType([
    StructField("accident_num", StringType(), True),
    StructField("accident_day", IntegerType(), True),
    StructField("accident_month", IntegerType(), True),
    StructField("accident_year", IntegerType(), True),
    StructField("accident_time", StringType(), True),
    StructField("light_conditions", IntegerType(), True),
    StructField("department_code", IntegerType(), True),
    StructField("commune_code", IntegerType(), True),
    StructField("accident_location", IntegerType(), True),
    StructField("accident_intersection", IntegerType(), True),
    StructField("accident_atmospheric_conditions", IntegerType(), True),
    StructField("accident_collision_type", IntegerType(), True),
    StructField("accident_address", StringType(), True),
    StructField("accident_latitude", StringType(), True),
    StructField("accident_longitude", StringType(), True)
])

@dp.table(
    name="accident_characteristics_raw",
    comment="Raw accident characteristics with ingestion metadata.",
    partition_cols=["ingestion_date"]
)
def accident_characteristics_raw():
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

