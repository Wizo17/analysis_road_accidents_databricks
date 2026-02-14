from pyspark import pipelines as dp
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import col, current_timestamp, to_date

volume_path = spark.conf.get("volume_ingestion_path")

schema = StructType([
    StructField("REGRGP_NOM", StringType(), True),
    StructField("REG_NOM", StringType(), True),
    StructField("REG_NOM_OLD", StringType(), True),
    StructField("ACA_NOM", StringType(), True),
    StructField("DEP_NOM", StringType(), True),
    StructField("COM_CODE", StringType(), True),
    StructField("COM_CODE1", StringType(), True),
    StructField("COM_CODE2", StringType(), True),
    StructField("COM_ID", StringType(), True),
    StructField("COM_NOM_MAJ_COURT", StringType(), True),
    StructField("COM_NOM_MAJ", StringType(), True),
    StructField("COM_NOM", StringType(), True),
    StructField("UU_CODE", StringType(), True),
    StructField("UU_ID", StringType(), True),
    StructField("UUCR_ID", StringType(), True),
    StructField("UUCR_NOM", StringType(), True),
    StructField("UU_WIKIDATA", StringType(), True),
    StructField("UU_PAYSAGE", StringType(), True),
    StructField("ZE_ID", StringType(), True),
    StructField("DEP_CODE", StringType(), True),
    StructField("DEP_ID", StringType(), True),
    StructField("DEP_NOM_NUM", StringType(), True),
    StructField("DEP_NUM_NOM", StringType(), True),
    StructField("DEP_WIKIDATA", StringType(), True),
    StructField("DEP_PAYSAGE", StringType(), True),
    StructField("NUTS_CODE_3", StringType(), True),
    StructField("ACA_CODE", StringType(), True),
    StructField("ACA_ID", StringType(), True),
    StructField("ACA_WIKIDATA", StringType(), True),
    StructField("ACA_PAYSAGE", StringType(), True),
    StructField("REG_CODE", StringType(), True),
    StructField("REG_ID", StringType(), True),
    StructField("REG_WIKIDATA", StringType(), True),
    StructField("NUTS_CODE_1", StringType(), True),
    StructField("REG_PAYSAGE", StringType(), True),
    StructField("REG_CODE_OLD", StringType(), True),
    StructField("REG_ID_OLD", StringType(), True),
    StructField("FD_ID", StringType(), True),
    StructField("FR_ID", StringType(), True),
    StructField("FE_ID", StringType(), True),
    StructField("UU_ID_10", StringType(), True),
    StructField("UU_ID_99", StringType(), True),
    StructField("AU_CODE", StringType(), True),
    StructField("AU_ID", StringType(), True),
    StructField("AUC_ID", StringType(), True),
    StructField("AUC_NOM", StringType(), True),
    StructField("EPCI_ID", StringType(), True),
    StructField("EPCI_NOM", StringType(), True),
    StructField("ATLAS_ID", StringType(), True),
    StructField("ATLAS_NOM", StringType(), True),
    StructField("ATLAS_WIKIDATA", StringType(), True),
    StructField("ATLAS_PAYSAGE", StringType(), True),
    StructField("geolocalisation", StringType(), True),
    StructField("COM_NOM_DEBUT", StringType(), True),
    StructField("COM_NOM_FIN", StringType(), True),
    StructField("COM_NOM_ANCIENS", StringType(), True),
    StructField("COM_NOM_ANCIENS_DATES", StringType(), True),
    StructField("COM_CODE_ACTUEL", StringType(), True),
    StructField("COM_CODE_ACTUEL_DEPUIS", StringType(), True)
])

@dp.table(
    name="geographic_reference_system_raw",
    comment="Raw geographic reference system data with ingestion metadata.",
    partition_cols=["ingestion_date"]
)
def geographic_reference_system_raw():
    return (
        spark.read
        .format("csv")
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

