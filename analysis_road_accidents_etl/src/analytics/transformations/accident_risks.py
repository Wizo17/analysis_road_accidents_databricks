import math
from pyspark import pipelines as dp
from pyspark.sql import functions as F
from utilities.accident_functions import add_zones_info, summarize_characteristics, summarize_sites, summarize_vehicles, summarize_victims

catalog = spark.conf.get("param_catalog")
schema_dataprep = spark.conf.get("param_schema_dataprep")
schema_analytics = spark.conf.get("param_schema_analytics")

@dp.table(
    name="accident_risks"
)
def accident_risks():
    group_by_cols = [
        "zone_id",
        "grid_lat",
        "grid_lon",
        "department_code",
        "commune_code",
        "accident_day",
        "accident_month",
        "accident_year"
    ]

    df_accidents = (spark.read.table(f"{catalog}.{schema_analytics}.road_accidents"))
    df_accidents = add_zones_info(df_accidents)

    df_cha = summarize_characteristics(df_accidents, group_by_cols)
    df_sit = summarize_sites(df_accidents, group_by_cols)
    df_veh = summarize_vehicles(df_accidents, group_by_cols)
    df_vic = summarize_victims(df_accidents, group_by_cols)

    df_accident_risks = (
        df_cha
        .join(df_sit, group_by_cols, "left")
        .join(df_veh, group_by_cols, "left")
        .join(df_vic, group_by_cols, "left")
    )

    return df_accident_risks
