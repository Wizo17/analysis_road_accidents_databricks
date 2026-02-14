from pyspark import pipelines as dp
from pyspark.sql import functions as F
from utilities.tools import delete_technical_columns

catalog = spark.conf.get("param_catalog")
schema_dataprep = spark.conf.get("param_schema_dataprep")

@dp.table(
    name="road_accidents",
)
@dp.expect_or_drop("valid_accident_num", "accident_num IS NOT NULL")
def road_accidents():
    df_acc_char = delete_technical_columns((spark.read.table(f"{catalog}.{schema_dataprep}.accident_characteristics_formated")))
    df_acc_st = delete_technical_columns((spark.read.table(f"{catalog}.{schema_dataprep}.accident_sites_formated")))
    df_acc_ve = delete_technical_columns((spark.read.table(f"{catalog}.{schema_dataprep}.accident_vehicles_formated")))
    df_acc_vic = delete_technical_columns((spark.read.table(f"{catalog}.{schema_dataprep}.accident_victims_formated")))

    df_accidents = join_accidents_table(df_acc_char, df_acc_st, df_acc_ve, df_acc_vic)

    df_geo_ref = (spark.read.table(f"{catalog}.{schema_dataprep}.geographic_reference_system_formated"))
    df_main = join_accidents_with_geo_ref(df_accidents, df_geo_ref)
    
    return (
        df_main
            .withColumn("accident_latitude", F.regexp_replace(F.col("accident_latitude"), ",", ".").cast("double"))
            .withColumn("accident_longitude", F.regexp_replace(F.col("accident_longitude"), ",", ".").cast("double"))
    )


def join_accidents_table(df_acc_char, df_acc_st, df_acc_ve, df_acc_vic):
    df_acc_char_formated = (
        df_acc_char
            .withColumnRenamed("accident_num", "accident_num_char")
    )

    df_acc_st_formated = (
        df_acc_st
            .withColumnRenamed("accident_num", "accident_num_st")
    )

    df_acc_ve_formated = (
        df_acc_ve
            .withColumnRenamed("accident_num", "accident_num_ve")
            .withColumnRenamed("vehicle_id", "vehicle_id_ve")
            .withColumnRenamed("vehicle_num", "vehicle_num_ve")
    )

    df_acc_vic_formated = (
        df_acc_vic
            .withColumnRenamed("accident_num", "accident_num_vic")
            .withColumnRenamed("vehicle_id", "vehicle_id_vic")
            .withColumnRenamed("vehicle_num", "vehicle_num_vic")
    )
    
    df_final = (
        df_acc_char_formated
            .join(
                df_acc_st_formated,
                df_acc_char_formated["accident_num_char"] == df_acc_st_formated["accident_num_st"],
                "left"
            )
            .join(
                df_acc_ve_formated,
                df_acc_st_formated["accident_num_st"] == df_acc_ve_formated["accident_num_ve"],
                "left"
            )
            .join(
                df_acc_vic_formated,
                (df_acc_ve_formated["accident_num_ve"] == df_acc_vic_formated["accident_num_vic"]) & (df_acc_ve_formated["vehicle_id_ve"] == df_acc_vic_formated["vehicle_id_vic"]),
                "left"
            )
    )

    return (
        df_final
            .withColumnRenamed("accident_num_char", "accident_num")
            .withColumnRenamed("vehicle_id_ve", "vehicle_id")
            .withColumnRenamed("vehicle_num_ve", "vehicle_num")
            .drop("accident_num_st", "accident_num_ve", "accident_num_vic", "vehicle_id_vic", "vehicle_num_vic")
    )


def join_accidents_with_geo_ref(df_accidents, df_geo_ref):
    return (
        df_accidents
            .join(
                df_geo_ref,
                (df_accidents["department_code"] == df_geo_ref["ref_department_code"]) & (df_accidents["commune_code"] == df_geo_ref["ref_municipality_insee_code"]),
                "left"
            )
    )

