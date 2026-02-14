from pyspark import pipelines as dp
from pyspark.sql import functions as F

catalog = spark.conf.get("param_catalog")
schema_dataprep = spark.conf.get("param_schema_dataprep")

@dp.table(
    name="risk_zones"
)
def risk_zones():
    precision = 500 / 111000  # Conversion m → degrees (approximate)
    group_by_keys = [
        "zone_id",
        "grid_lat",
        "grid_lon",
        "department_code",
        "commune_code",
        "accident_day",
        "accident_month",
        "accident_year"
    ]
    
    df_accidents = (spark.read.table(f"{catalog}.{schema_dataprep}.road_accidents"))

    df_gridded = (
        df_accidents
            .withColumn("grid_lat", F.round(F.col("accident_latitude") / precision) * precision)
            .withColumn("grid_lon", F.round(F.col("accident_longitude") / precision) * precision)
            .withColumn("zone_id", F.sha2(F.concat_ws("_", F.col("grid_lat"), F.col("grid_lon")), 256))
    )

    # Aggregate for compute accident counts
    df_agg_acc_counts = (
        df_gridded
            .select(*group_by_keys, "accident_num", "vehicle_id", "user_id")
            .distinct()
            .groupBy(*group_by_keys)
            .agg(F.count("*").alias("accident_count"))
    )

    # Aggregate for compute injured counts
    df_agg_injured_counts = (
        df_gridded
            .select(*group_by_keys, "accident_num", "vehicle_id", "user_id", "injury_severity")
            .distinct()
            .withColumn(
                "injury_severity",
                F.when(F.col("injury_severity") == "Indemne", "injury_severity_unharmed")
                 .when(F.col("injury_severity") == "Tué", "injury_severity_killed")
                 .when(F.col("injury_severity") == "Blessé hospitalisé", "injury_severity_hospitalized")
                 .when(F.col("injury_severity") == "Blessé léger", "injury_severity_lightly_injured")
                 .otherwise("injury_severity_unknown")
            )
            .groupBy(*group_by_keys)
            .pivot("injury_severity")
            .count()
            .fillna(0)
    )

    df_final = (
        df_agg_acc_counts
            .join(df_agg_injured_counts, on=group_by_keys, how="left")
    )

    return df_final
