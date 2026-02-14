from pyspark import pipelines as dp
from pyspark.sql import functions as F

catalog = spark.conf.get("param_catalog")
schema_raw = spark.conf.get("param_schema_raw")

@dp.table(
    name="accident_sites_formated"
)
@dp.expect_or_drop("valid_accident_num", "accident_num IS NOT NULL")
def accident_sites_formated():
    return (
        spark.read
        .table(f"{catalog}.{schema_raw}.accident_sites_raw")
        .withColumn("road_category", F.trim(F.col("road_category")))
        .withColumn(
            "road_category",
            F.when(F.col("road_category") == "1", "Autoroute")
             .when(F.col("road_category") == "2", "Route nationale")
             .when(F.col("road_category") == "3", "Route Départementale")
             .when(F.col("road_category") == "4", "Voie Communales")
             .when(F.col("road_category") == "5", "Hors réseau public")
             .when(F.col("road_category") == "6", "Parc de stationnement ouvert à la circulation publique")
             .when(F.col("road_category") == "7", "Routes de métropole urbaine")
             .when(F.col("road_category") == "9", "autre")
             .otherwise("")
        )
        .withColumn("road_traffic_system", F.trim(F.col("road_traffic_system")))
        .withColumn(
            "road_traffic_system",
            F.when(F.col("road_traffic_system") == "-1", "Non renseigné")
             .when(F.col("road_traffic_system") == "1", "A sens unique")
             .when(F.col("road_traffic_system") == "2", "Bidirectionnelle")
             .when(F.col("road_traffic_system") == "3", "A chaussées séparées")
             .when(F.col("road_traffic_system") == "4", "Avec voies d’affectation variable")
             .otherwise("")
        )
        .withColumn("reserved_lane", F.trim(F.col("reserved_lane")))
        .withColumn(
            "reserved_lane",
            F.when(F.col("reserved_lane") == "-1", "Non renseigné")
             .when(F.col("reserved_lane") == "1", "Sans objet")
             .when(F.col("reserved_lane") == "2", "Piste cyclable")
             .when(F.col("reserved_lane") == "3", "Bande cyclable")
             .when(F.col("reserved_lane") == "4", "Voie réservée")
             .otherwise("")
        )
        .withColumn("road_gradient", F.trim(F.col("road_gradient")))
        .withColumn(
            "road_gradient",
            F.when(F.col("road_gradient") == "-1", "Non renseigné")
             .when(F.col("road_gradient") == "1", "Plat")
             .when(F.col("road_gradient") == "2", "Pente")
             .when(F.col("road_gradient") == "3", "Sommet de côte")
             .when(F.col("road_gradient") == "4", "Bas de côte")
             .otherwise("")
        )
        .withColumn("map", F.trim(F.col("map")))
        .withColumn(
            "map",
            F.when(F.col("map") == "-1", "Non renseigné")
             .when(F.col("map") == "1", "Partie rectiligne")
             .when(F.col("map") == "2", "En courbe à gauche")
             .when(F.col("map") == "3", "En courbe à droite")
             .when(F.col("map") == "4", "En « S »")
             .otherwise("")
        )
        .withColumn("surface_condition", F.trim(F.col("surface_condition")))
        .withColumn(
            "surface_condition",
            F.when(F.col("surface_condition") == "-1", "Non renseigné")
             .when(F.col("surface_condition") == "1", "Normale")
             .when(F.col("surface_condition") == "2", "Mouillée")
             .when(F.col("surface_condition") == "3", "Flaques")
             .when(F.col("surface_condition") == "4", "Inondée")
             .when(F.col("surface_condition") == "5", "Enneigée")
             .when(F.col("surface_condition") == "6", "Boue")
             .when(F.col("surface_condition") == "7", "Verglacée")
             .when(F.col("surface_condition") == "8", "Corps gras – huile")
             .when(F.col("surface_condition") == "9", "Autre")
             .otherwise("")
        )
        .withColumn("layout_infrastructure", F.trim(F.col("layout_infrastructure")))
        .withColumn(
            "layout_infrastructure",
            F.when(F.col("layout_infrastructure") == "-1", "Non renseigné")
             .when(F.col("layout_infrastructure") == "1", "Aucun")
             .when(F.col("layout_infrastructure") == "2", "Souterrain - tunnel")
             .when(F.col("layout_infrastructure") == "3", "Pont - autopont")
             .when(F.col("layout_infrastructure") == "4", "Bretelle d’échangeur ou de raccordement")
             .when(F.col("layout_infrastructure") == "5", "Voie ferrée")
             .when(F.col("layout_infrastructure") == "6", "Carrefour aménagé")
             .when(F.col("layout_infrastructure") == "7", "Zone piétonne")
             .when(F.col("layout_infrastructure") == "8", "Zone de péage")
             .when(F.col("layout_infrastructure") == "9", "Chantier")
             .otherwise("")
        )
        .withColumn("location_of_accident", F.trim(F.col("location_of_accident")))
        .withColumn(
            "location_of_accident",
            F.when(F.col("location_of_accident") == "-1", "Non renseigné")
             .when(F.col("location_of_accident") == "0", "Aucun")
             .when(F.col("location_of_accident") == "1", "Sur chaussée")
             .when(F.col("location_of_accident") == "2", "Sur bande d’arrêt d’urgence")
             .when(F.col("location_of_accident") == "3", "Sur accotement")
             .when(F.col("location_of_accident") == "4", "Sur trottoir")
             .when(F.col("location_of_accident") == "5", "Sur piste cyclable")
             .when(F.col("location_of_accident") == "6", "Sur autre voie spéciale")
             .when(F.col("location_of_accident") == "8", "Autres")
             .otherwise("")
        )
    )
