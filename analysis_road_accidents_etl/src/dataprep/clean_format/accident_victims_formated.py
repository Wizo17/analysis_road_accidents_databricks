from pyspark import pipelines as dp
from pyspark.sql import functions as F

catalog = spark.conf.get("param_catalog")
schema_raw = spark.conf.get("param_schema_raw")

@dp.table(
    name="accident_victims_formated"
)
@dp.expect_or_drop("valid_accident_num", "accident_num IS NOT NULL")
def accident_victims_formated():
    return (
        spark.read
        .table(f"{catalog}.{schema_raw}.accident_victims_raw")
        .withColumn("user_category", F.trim(F.col("user_category")))
        .withColumn(
            "user_category",
            F.when(F.col("user_category") == "1", "Conducteur")
             .when(F.col("user_category") == "2", "Passager")
             .when(F.col("user_category") == "3", "Piéton")
             .otherwise("")
        )
        .withColumn("injury_severity", F.trim(F.col("injury_severity")))
        .withColumn(
            "injury_severity",
            F.when(F.col("injury_severity") == "1", "Indemne")
             .when(F.col("injury_severity") == "2", "Tué")
             .when(F.col("injury_severity") == "3", "Blessé hospitalisé")
             .when(F.col("injury_severity") == "4", "Blessé léger")
             .otherwise("")
        )
        .withColumn("user_gender", F.trim(F.col("user_gender")))
        .withColumn(
            "user_gender",
            F.when(F.col("user_gender") == "1", "Masculin")
             .when(F.col("user_gender") == "2", "Féminin")
             .otherwise("")
        )
        .withColumn("reason_for_travel", F.trim(F.col("reason_for_travel")))
        .withColumn(
            "reason_for_travel",
            F.when(F.col("reason_for_travel") == "-1", "Non renseigné")
             .when(F.col("reason_for_travel") == "0", "Non renseigné")
             .when(F.col("reason_for_travel") == "1", "Domicile – travail")
             .when(F.col("reason_for_travel") == "2", "Domicile – école")
             .when(F.col("reason_for_travel") == "3", "Courses – achats")
             .when(F.col("reason_for_travel") == "4", "Utilisation professionnelle")
             .when(F.col("reason_for_travel") == "5", "Promenade – loisirs")
             .when(F.col("reason_for_travel") == "9", "Autre")
             .otherwise("")
        )
        .withColumn("safety_equipment_1", F.trim(F.col("safety_equipment_1")))
        .withColumn(
            "safety_equipment_1",
            F.when(F.col("safety_equipment_1") == "-1", "Non renseigné")
             .when(F.col("safety_equipment_1") == "0", "Aucun équipement")
             .when(F.col("safety_equipment_1") == "1", "Ceinture")
             .when(F.col("safety_equipment_1") == "2", "Casque")
             .when(F.col("safety_equipment_1") == "3", "Dispositif enfants")
             .when(F.col("safety_equipment_1") == "4", "Gilet réfléchissant")
             .when(F.col("safety_equipment_1") == "5", "Airbag (2RM/3RM)")
             .when(F.col("safety_equipment_1") == "6", "Gants (2RM/3RM)")
             .when(F.col("safety_equipment_1") == "7", "Gants + Airbag (2RM/3RM)")
             .when(F.col("safety_equipment_1") == "9", "Autre")
             .otherwise("")
        )
        .withColumn("safety_equipment_2", F.trim(F.col("safety_equipment_2")))
        .withColumn(
            "safety_equipment_2",
            F.when(F.col("safety_equipment_2") == "-1", "Non renseigné")
             .when(F.col("safety_equipment_2") == "0", "Aucun équipement")
             .when(F.col("safety_equipment_2") == "1", "Ceinture")
             .when(F.col("safety_equipment_2") == "2", "Casque")
             .when(F.col("safety_equipment_2") == "3", "Dispositif enfants")
             .when(F.col("safety_equipment_2") == "4", "Gilet réfléchissant")
             .when(F.col("safety_equipment_2") == "5", "Airbag (2RM/3RM)")
             .when(F.col("safety_equipment_2") == "6", "Gants (2RM/3RM)")
             .when(F.col("safety_equipment_2") == "7", "Gants + Airbag (2RM/3RM)")
             .when(F.col("safety_equipment_2") == "9", "Autre")
             .otherwise("")
        )
        .withColumn("safety_equipment_3", F.trim(F.col("safety_equipment_3")))
        .withColumn(
            "safety_equipment_3",
            F.when(F.col("safety_equipment_3") == "-1", "Non renseigné")
             .when(F.col("safety_equipment_3") == "0", "Aucun équipement")
             .when(F.col("safety_equipment_3") == "1", "Ceinture")
             .when(F.col("safety_equipment_3") == "2", "Casque")
             .when(F.col("safety_equipment_3") == "3", "Dispositif enfants")
             .when(F.col("safety_equipment_3") == "4", "Gilet réfléchissant")
             .when(F.col("safety_equipment_3") == "5", "Airbag (2RM/3RM)")
             .when(F.col("safety_equipment_3") == "6", "Gants (2RM/3RM)")
             .when(F.col("safety_equipment_3") == "7", "Gants + Airbag (2RM/3RM)")
             .when(F.col("safety_equipment_3") == "9", "Autre")
             .otherwise("")
        )
        .withColumn("pedestrian_location", F.trim(F.col("pedestrian_location")))
        .withColumn(
            "pedestrian_location_group",
            F.when(F.col("pedestrian_location").isin(["-1", "0"]), "Inconnu ou sans objet")
             .when(F.col("pedestrian_location").isin(["1", "2"]), "Sur chaussée")
             .when(F.col("pedestrian_location").isin(["3", "4"]), "Sur passage piéton")
             .when(F.col("pedestrian_location").isin(["5", "6", "7", "8", "9"]), "Divers")
             .otherwise("")
        )
        .withColumn(
            "pedestrian_location_type",
            F.when(F.col("pedestrian_location") == "-1", "Non renseigné")
             .when(F.col("pedestrian_location") == "0", "Sans objet")
             .when(F.col("pedestrian_location") == "1", "A + 50 m du passage piéton")
             .when(F.col("pedestrian_location") == "2", "A – 50 m du passage piéton")
             .when(F.col("pedestrian_location") == "3", "Sans signalisation lumineuse")
             .when(F.col("pedestrian_location") == "4", "Avec signalisation lumineuse")
             .when(F.col("pedestrian_location") == "5", "Sur trottoir")
             .when(F.col("pedestrian_location") == "6", "Sur accotement")
             .when(F.col("pedestrian_location") == "7", "Sur refuge ou BAU")
             .when(F.col("pedestrian_location") == "8", "Sur contre allée")
             .when(F.col("pedestrian_location") == "9", "Inconnue")
             .otherwise("")
        )
        .drop("pedestrian_location")
        .withColumn("pedestrian_action", F.trim(F.col("pedestrian_action")))
        .withColumn(
            "pedestrian_action_group",
            F.when(F.col("pedestrian_action").isin(["-1"]), "Inconnu")
             .when(F.col("pedestrian_action").isin(["0", "1", "2"]), "Se déplaçant")
             .when(F.col("pedestrian_action").isin(["3", "4", "5", "6", "7", "8", "9", "A", "B"]), "Divers")
             .otherwise("")
        )
        .withColumn(
            "pedestrian_action_type",
            F.when(F.col("pedestrian_action") == "-1", "Non renseigné")
             .when(F.col("pedestrian_action") == "0", "Non renseigné ou sans objet")
             .when(F.col("pedestrian_action") == "1", "Sens véhicule heurtant")
             .when(F.col("pedestrian_action") == "2", "Sens inverse du véhicule")
             .when(F.col("pedestrian_action") == "3", "Traversant")
             .when(F.col("pedestrian_action") == "4", "Masqué")
             .when(F.col("pedestrian_action") == "5", "Jouant – courant")
             .when(F.col("pedestrian_action") == "6", "Avec animal")
             .when(F.col("pedestrian_action") == "9", "Autre")
             .when(F.col("pedestrian_action") == "A", "Monte/descend du véhicule")
             .when(F.col("pedestrian_action") == "B", "Inconnue")
             .otherwise("")
        )
        .drop("pedestrian_action")
        .withColumn(
            "pedestrian_alone",
            F.when(F.col("pedestrian_alone") == "-1", "Non renseigné")
             .when(F.col("pedestrian_alone") == "1", "Seul")
             .when(F.col("pedestrian_alone") == "2", "Accompagné")
             .when(F.col("pedestrian_alone") == "3", "En groupe")
             .otherwise("")
        )

    )
