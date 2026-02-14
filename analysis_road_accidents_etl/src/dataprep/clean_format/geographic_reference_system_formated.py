from pyspark import pipelines as dp
from pyspark.sql import functions as F

catalog = spark.conf.get("param_catalog")
schema_raw = spark.conf.get("param_schema_raw")

@dp.table(
    name="geographic_reference_system_formated",
)
@dp.expect_or_drop("valid_ref_municipality_insee_code", "ref_municipality_insee_code IS NOT NULL")
def geographic_reference_system_formated():
    return (
        spark.read
        .table(f"{catalog}.{schema_raw}.geographic_reference_system_raw")
        .select(
            "REGRGP_NOM",
            "REG_NOM",
            "ACA_NOM",
            "DEP_CODE",
            "DEP_NOM",
            "DEP_NOM_NUM",
            "COM_CODE",
            "COM_NOM_MAJ"
        )
        .withColumnRenamed("REGRGP_NOM", "ref_region_group_name")
        .withColumnRenamed("REG_NOM", "ref_region_name")
        .withColumnRenamed("ACA_NOM", "ref_academy_name")
        .withColumnRenamed("DEP_CODE", "ref_department_code")
        .withColumnRenamed("DEP_NOM", "ref_department_name")
        .withColumnRenamed("DEP_NOM_NUM", "ref_department_name_num")
        .withColumnRenamed("COM_CODE", "ref_municipality_insee_code")
        .withColumnRenamed("COM_NOM_MAJ", "ref_municipality_name")
    )
