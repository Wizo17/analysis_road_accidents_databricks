from pyspark import pipelines as dp
from pyspark.sql import functions as F

catalog = spark.conf.get("param_catalog")
schema_dataprep = spark.conf.get("param_schema_dataprep")

@dp.table(
    name="risk_zones"
)
def risk_zones():
    return (
        spark.read
        .table(f"{catalog}.{schema_dataprep}.road_accidents")
    )
