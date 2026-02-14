from pyspark import pipelines as dp
from pyspark.sql import functions as F

catalog = spark.conf.get("param_catalog")
schema_raw = spark.conf.get("param_schema_raw")

@dp.table(
    name="accident_victims_formated",
    comment="Raw accident victims with ingestion metadata.",
)
@dp.expect_or_drop("valid_accident_num", "accident_num IS NOT NULL")
def accident_victims_formated():
    return (
        spark.read
        .table(f"{catalog}.{schema_raw}.accident_victims_raw")
        .withColumn(
            "user_category",
            F.when(F.col("user_category") == 1, "Conducteur")
             .when(F.col("user_category") == 2, "Passager")
             .when(F.col("user_category") == 3, "Piéton")
             .otherwise("")
        )
        .withColumn(
            "injury_severity",
            F.when(F.col("injury_severity") == 1, "Indemne")
             .when(F.col("injury_severity") == 2, "Tué")
             .when(F.col("injury_severity") == 3, "Blessé hospitalisé")
             .when(F.col("injury_severity") == 4, "Blessé léger")
             .otherwise("")
        )
        .withColumn(
            "user_gender",
            F.when(F.col("user_gender") == 1, "Masculin")
             .when(F.col("user_gender") == 2, "Féminin")
             .otherwise("")
        )
        .withColumn(
            "reason_for_travel",
            F.when(F.col("reason_for_travel") == -1, "Non renseigné")
             .when(F.col("reason_for_travel") == 0, "Non renseigné")
             .when(F.col("reason_for_travel") == 1, "Domicile – travail")
             .when(F.col("reason_for_travel") == 2, "Domicile – école")
             .when(F.col("reason_for_travel") == 3, "Courses – achats")
             .when(F.col("reason_for_travel") == 4, "Utilisation professionnelle")
             .when(F.col("reason_for_travel") == 5, "Promenade – loisirs")
             .when(F.col("reason_for_travel") == 9, "Autre")
             .otherwise("")
        )
    )
