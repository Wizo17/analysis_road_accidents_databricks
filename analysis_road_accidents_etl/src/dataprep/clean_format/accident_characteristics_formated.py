from pyspark import pipelines as dp
from pyspark.sql import functions as F

catalog = spark.conf.get("param_catalog")
schema_raw = spark.conf.get("param_schema_raw")

@dp.table(
    name="accident_characteristics_formated"
)
@dp.expect_or_drop("valid_accident_num", "accident_num IS NOT NULL")
def accident_characteristics_formated():
    return (
        spark.read
        .table(f"{catalog}.{schema_raw}.accident_characteristics_raw")
        .withColumn("light_conditions", F.trim(F.col("light_conditions")))
        .withColumn(
            "light_conditions",
            F.when(F.col("light_conditions") == "1", "Plein jour")
             .when(F.col("light_conditions") == "2", "Crépuscule ou aube")
             .when(F.col("light_conditions") == "3", "Nuit sans éclairage public")
             .when(F.col("light_conditions") == "4", "Nuit avec éclairage public non allumé")
             .when(F.col("light_conditions") == "5", "Nuit avec éclairage public allumé")
             .otherwise("")
        )
        .withColumn("accident_location", F.trim(F.col("accident_location")))
        .withColumn(
            "accident_location",
            F.when(F.col("accident_location") == "1", "Hors agglomération")
             .when(F.col("accident_location") == "2", "En agglomération")
             .otherwise("")
        )
        .withColumn("accident_intersection", F.trim(F.col("accident_intersection")))
        .withColumn(
            "accident_intersection",
            F.when(F.col("accident_intersection") == "1", "Hors intersection")
             .when(F.col("accident_intersection") == "2", "Intersection en X")
             .when(F.col("accident_intersection") == "3", "Intersection en T")
             .when(F.col("accident_intersection") == "4", "Intersection en Y")
             .when(F.col("accident_intersection") == "5", "Intersection à plus de 4 branches")
             .when(F.col("accident_intersection") == "6", "Giratoire")
             .when(F.col("accident_intersection") == "7", "Place")
             .when(F.col("accident_intersection") == "8", "Passage à niveau")
             .when(F.col("accident_intersection") == "9", "Autre intersection")
             .otherwise("")
        )
        .withColumn("accident_atmospheric_conditions", F.trim(F.col("accident_atmospheric_conditions")))
        .withColumn(
            "accident_atmospheric_conditions",
            F.when(F.col("accident_atmospheric_conditions") == "-1", "Non renseigné")
             .when(F.col("accident_atmospheric_conditions") == "1", "Normale")
             .when(F.col("accident_atmospheric_conditions") == "2", "Pluie légère")
             .when(F.col("accident_atmospheric_conditions") == "3", "Pluie forte")
             .when(F.col("accident_atmospheric_conditions") == "4", "Neige - grêle")
             .when(F.col("accident_atmospheric_conditions") == "5", "Brouillard - fumée")
             .when(F.col("accident_atmospheric_conditions") == "6", "Vent fort - tempête")
             .when(F.col("accident_atmospheric_conditions") == "7", "Temps éblouissant")
             .when(F.col("accident_atmospheric_conditions") == "8", "Temps couvert")
             .when(F.col("accident_atmospheric_conditions") == "9", "Autre")
             .otherwise("")
        )
        .withColumn("accident_collision_type", F.trim(F.col("accident_collision_type")))
        .withColumn(
            "accident_collision_type",
            F.when(F.col("accident_collision_type") == "-1", "Non renseigné")
             .when(F.col("accident_collision_type") == "1", "Deux véhicules - frontale")
             .when(F.col("accident_collision_type") == "2", "Deux véhicules – par l’arrière")
             .when(F.col("accident_collision_type") == "3", "Deux véhicules – par le coté")
             .when(F.col("accident_collision_type") == "4", "Trois véhicules et plus – en chaîne")
             .when(F.col("accident_collision_type") == "5", "Trois véhicules et plus - collisions multiples")
             .when(F.col("accident_collision_type") == "6", "Autre collision")
             .when(F.col("accident_collision_type") == "7", "Sans collision")
             .otherwise("")
        )
    )
