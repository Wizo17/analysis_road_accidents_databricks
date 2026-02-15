"""
Utility functions for accident data processing.
"""

import math
from pyspark.sql import functions as F

def add_zones_info(df):
    """Add grid and zone information to the dataframe."""
    # 1 degree latitude = ~111,111 m
    # 1 degree longitude = 111,111 * cos(latitude)
    # For France (average 47°N), cos(47°) ≈ 0.682

    precision_lat = 500 / 111111
    precision_lon = 500 / (111111 * 0.682) # Correction for France

    df_grid = (
        df
        .withColumn("grid_lat", F.round(F.col("accident_latitude") / precision_lat) * precision_lat)
        .withColumn("grid_lon", F.round(F.col("accident_longitude") / precision_lon) * precision_lon)
        .withColumn("zone_id", F.sha2(F.concat_ws("_", F.col("grid_lat"), F.col("grid_lon")), 256))
    )

    return df_grid


def summarize_characteristics(df, group_by_cols):
    """Summarize accident characteristics."""
    df_selected = (
        df
        .select(group_by_cols + ["accident_num", "light_conditions", "accident_location", "accident_atmospheric_conditions", "accident_collision_type"])
        .distinct()
    )

    df_grouped = (
        df_selected
        .groupBy(group_by_cols)
        .agg(
            F.countDistinct("accident_num").alias("num_accidents"),

            F.sum(F.when(F.lower(F.col("light_conditions")).contains("jour"), 1).otherwise(0)).alias("count_light_conditions_jour"),
            F.sum(F.when(F.lower(F.col("light_conditions")).contains("nuit"), 1).otherwise(0)).alias("count_light_conditions_nuit"),
            F.sum(F.when(F.col("light_conditions") == "Crépuscule ou aube", 1).otherwise(0)).alias("count_light_conditions_crepuscule_aube"),

            F.sum(F.when(F.col("accident_location") == "Hors agglomération", 1).otherwise(0)).alias("count_location_hors_agglomeration"),
            F.sum(F.when(F.col("accident_location") == "En agglomération", 1).otherwise(0)).alias("count_location_en_agglomeration"),

            F.sum(F.when(F.col("accident_atmospheric_conditions") == "Normale", 1).otherwise(0)).alias("count_atmospheric_conditions_normale"),
            F.sum(F.when(F.lower(F.col("accident_atmospheric_conditions")).contains("pluie"), 1).otherwise(0)).alias("count_atmospheric_conditions_pluie"),
            F.sum(F.when(F.col("accident_atmospheric_conditions") == "Neige - grêle", 1).otherwise(0)).alias("count_atmospheric_conditions_neige_grele"),
            F.sum(F.when(F.col("accident_atmospheric_conditions") == "Brouillard - fumée", 1).otherwise(0)).alias("count_atmospheric_conditions_brouillard_fumee"),
            F.sum(F.when(F.col("accident_atmospheric_conditions") == "Vent fort - tempête", 1).otherwise(0)).alias("count_atmospheric_conditions_vent_fort_tempete"),

            F.sum(F.when(F.lower(F.col("accident_collision_type")).contains("deux véhicules"), 1).otherwise(0)).alias("count_collision_type_deux_vehicules"),
            F.sum(F.when(F.lower(F.col("accident_collision_type")).contains("trois véhicules"), 1).otherwise(0)).alias("count_collision_type_trois_vehicules"),
            F.sum(F.when(F.lower(F.col("accident_collision_type")).contains("sans collision"), 1).otherwise(0)).alias("count_collision_type_sans_collision")
        )
    )

    return df_grouped


def summarize_sites(df, group_by_cols):
    """Summarize accident sites."""
    df_selected = (
        df
        .select(group_by_cols + ["accident_num", "road_category", "reserved_lane", "surface_condition", "layout_infrastructure"])
        .distinct()
    )

    df_grouped = (
        df_selected
        .groupBy(group_by_cols)
        .agg(
            F.sum(F.when(F.col("road_category") == "Autoroute", 1).otherwise(0)).alias("count_road_category_autoroute"),
            F.sum(F.when(F.col("road_category") == "Route nationale", 1).otherwise(0)).alias("count_road_category_nationale"),
            F.sum(F.when(F.col("road_category") == "Route Départementale", 1).otherwise(0)).alias("count_road_category_departementale"),
            F.sum(F.when(F.col("road_category") == "Voie Communales", 1).otherwise(0)).alias("count_road_category_communales"),
            F.sum(F.when(F.col("road_category") == "Hors réseau public", 1).otherwise(0)).alias("count_road_category_hors_reseau_public"),
            F.sum(F.when(F.col("road_category") == "Routes de métropole urbaine", 1).otherwise(0)).alias("count_road_category_metropole_urbaine"),

            F.sum(F.when(F.lower(F.col("reserved_lane")).isin(["Non renseigné", "Sans objet", ""]), 1).otherwise(0)).alias("count_reserved_lane_none"),
            F.sum(F.when(F.lower(F.col("reserved_lane")).contains("cyclable"), 1).otherwise(0)).alias("count_reserved_lane_cyclable"),
            F.sum(F.when(F.lower(F.col("reserved_lane")).contains("réservée"), 1).otherwise(0)).alias("count_reserved_lane_reservee"),

            F.sum(F.when(F.col("surface_condition") == "Normale", 1).otherwise(0)).alias("count_surface_condition_normale"),
            F.sum(F.when(F.lower(F.col("surface_condition")).isin(["Mouillée", "Flaques", "Inondée"]), 1).otherwise(0)).alias("count_surface_condition_mouillee"),
            F.sum(F.when(F.col("surface_condition") == "Enneigée", 1).otherwise(0)).alias("count_surface_condition_enneigee"),
            F.sum(F.when(F.col("surface_condition") == "Boue", 1).otherwise(0)).alias("count_surface_condition_boue"),
            F.sum(F.when(F.col("surface_condition") == "Verglacée", 1).otherwise(0)).alias("count_surface_condition_verglacee"),
            F.sum(F.when(F.col("surface_condition") == "Corps gras – huile", 1).otherwise(0)).alias("count_surface_condition_corps_gras_huile"),

            F.sum(F.when(F.col("layout_infrastructure") == "Souterrain - tunnel", 1).otherwise(0)).alias("count_layout_infrastructure_souterrain_tunnel"),
            F.sum(F.when(F.col("layout_infrastructure") == "Pont - autopont", 1).otherwise(0)).alias("count_layout_infrastructure_pont_autopont"),
            F.sum(F.when(F.col("layout_infrastructure") == "Bretelle d’échangeur ou de raccordement", 1).otherwise(0)).alias("count_layout_infrastructure_bretelle"),
            F.sum(F.when(F.col("layout_infrastructure") == "Voie ferrée", 1).otherwise(0)).alias("count_layout_infrastructure_voie_ferree"),
            F.sum(F.when(F.col("layout_infrastructure") == "Carrefour aménagé", 1).otherwise(0)).alias("count_layout_infrastructure_carrefour_amenage"),
            F.sum(F.when(F.col("layout_infrastructure") == "Zone piétonne", 1).otherwise(0)).alias("count_layout_infrastructure_zone_pietonne"),
            F.sum(F.when(F.col("layout_infrastructure") == "Zone de péage", 1).otherwise(0)).alias("count_layout_infrastructure_zone_peage"),
            F.sum(F.when(F.col("layout_infrastructure") == "Chantier", 1).otherwise(0)).alias("count_layout_infrastructure_chantier"),
        )
    )

    return df_grouped



def summarize_vehicles(df, group_by_cols):
    """Summarize vehicles involved in accidents."""
    df_selected = (
        df
        .select(group_by_cols + ["accident_num", "vehicle_id", "vehicle_category", "moving_obstacle_struck", "vehicle_engine_type"])
        .distinct()
    )

    df_grouped = (
        df_selected
        .groupBy(group_by_cols)
        .agg(
            F.countDistinct("vehicle_id").alias("num_vehicles"),

            F.sum(F.when(F.col("vehicle_category") == "Bicyclette", 1).otherwise(0)).alias("count_vehicle_category_bicycles"),
            F.sum(F.when(F.lower(F.col("vehicle_category")).contains("scooter"), 1).otherwise(0)).alias("count_vehicle_category_scooters"),
            F.sum(F.when(F.lower(F.col("vehicle_category")).contains("tracteur"), 1).otherwise(0)).alias("count_vehicle_category_tractors"),
            F.sum(F.when(F.lower(F.col("vehicle_category")).contains("tramway"), 1).otherwise(0)).alias("count_vehicle_category_tramways"),
            F.sum(F.when(F.lower(F.col("vehicle_category")).isin(["autobus", "autocar"]), 1).otherwise(0)).alias("count_vehicle_category_autobuses"),
            F.sum(F.when(F.lower(F.col("vehicle_category")).contains("Train"), 1).otherwise(0)).alias("count_vehicle_category_trains"),

            F.sum(F.when(F.col("moving_obstacle_struck") == "Aucun", 1).otherwise(0)).alias("count_obstacle_struck_none"),
            F.sum(F.when(F.col("moving_obstacle_struck") == "Piéton", 1).otherwise(0)).alias("count_obstacle_struck_pedestrian"),
            F.sum(F.when(F.lower(F.col("moving_obstacle_struck")).contains("véhicule"), 1).otherwise(0)).alias("count_obstacle_struck_vehicles"),
            F.sum(F.when(F.lower(F.col("moving_obstacle_struck")).contains("Animal"), 1).otherwise(0)).alias("count_obstacle_struck_animals"),

            F.sum(F.when(F.col("vehicle_engine_type") == "Hydrocarbures", 1).otherwise(0)).alias("count_vehicle_engine_hydrocarbures"),
            F.sum(F.when(F.col("vehicle_engine_type") == "Hybride électrique", 1).otherwise(0)).alias("count_vehicle_engine_hybride_electrique"),
            F.sum(F.when(F.col("vehicle_engine_type") == "Electrique", 1).otherwise(0)).alias("count_vehicle_engine_electrique"),
            F.sum(F.when(F.col("vehicle_engine_type") == "Hydrogène", 1).otherwise(0)).alias("count_vehicle_engine_hydrogene")
        )
    )
    
    return df_grouped



def summarize_victims(df, group_by_cols):
    """Summarize victims of accidents."""
    df_selected = (
        df
        .select(group_by_cols + ["accident_num", "vehicle_id", "user_id", "user_category", "user_gender", "user_birth_year", "injury_severity"])
        .distinct()
    )

    df_grouped = (
        df_selected
        .groupBy(group_by_cols)
        .agg(
            F.countDistinct("user_id").alias("num_victims"),

            F.sum(F.when(F.col("user_category") == "Conducteur", 1).otherwise(0)).alias("count_victims_drivers"),
            F.sum(F.when(F.col("user_category") == "Passager", 1).otherwise(0)).alias("count_victims_passengers"),
            F.sum(F.when(F.col("user_category") == "Piéton", 1).otherwise(0)).alias("count_victims_pedestrians"),

            F.sum(F.when(F.col("user_gender") == "Féminin", 1).otherwise(0)).alias("count_victims_female"),
            F.sum(F.when(F.col("user_gender") == "Masculin", 1).otherwise(0)).alias("count_victims_male"),

            F.mode("user_birth_year").alias("most_victims_common_birth_year"),

            F.sum(F.when(F.col("injury_severity") == "Tué", 1).otherwise(0)).alias("count_victims_killed"),
            F.sum(F.when(F.col("injury_severity") == "Blessé hospitalisé", 1).otherwise(0)).alias("count_victims_hospitalized"),
            F.sum(F.when(F.col("injury_severity") == "Blessé léger", 1).otherwise(0)).alias("count_victims_lightly_injured")
        )
    )
    
    return df_grouped


