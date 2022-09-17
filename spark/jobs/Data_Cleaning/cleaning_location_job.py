import pyspark.sql.functions as f
import pyspark.sql.types as t
from pyspark.sql import SparkSession
from pygeodesy.sphericalNvector import LatLon
from pygeodesy.errors import PointsError

from dependencies.spark import start_spark
from dependencies.utils import (
    extract_tsv_data,
    extract_csv_data,
    load_data_to_s3_as_parquet,
)


def main():
    spark, log, config = start_spark(
        app_name="cleaning_location_job", files=["../configs/etl_config.json"]
    )

    log.warn("Cleaning Location Job is up and running")

    data = extract_tsv_data(spark, f"{config['raw_data_s3_path']}location.tsv")
    country_shapes_cleaned = extract_csv_data(
        spark, f"{config['cleaned_data_s3_path']}country_shapes.csv"
    )
    country_info = extract_tsv_data(
        spark, path=f"{config['raw_data_s3_path']}countryInfo.txt", partition_number=1
    )

    data_transformed = transform_data(spark, data, country_shapes_cleaned, country_info)

    load_data_to_s3_as_parquet(
        spark,
        data_transformed,
        path=f"{config['cleaned_data_s3_path']}location.parquet",
    )

    log.warn("Cleaning Location Job is finished")
    spark.stop()
    return None


def map_us_state_to_country(state):
    list_of_states_in_us = [
        "AL",
        "AK",
        "AS",
        "AZ",
        "AR",
        "CA",
        "CO",
        "CT",
        "DE",
        "DC",
        "FL",
        "GA",
        "GU",
        "HI",
        "ID",
        "IL",
        "IN",
        "IA",
        "KS",
        "KY",
        "LA",
        "ME",
        "MD",
        "MA",
        "MI",
        "MN",
        "MS",
        "MO",
        "MT",
        "NE",
        "NV",
        "NH",
        "NJ",
        "NM",
        "NY",
        "NC",
        "ND",
        "MP",
        "OH",
        "OK",
        "OR",
        "PA",
        "PR",
        "RI",
        "SC",
        "SD",
        "TN",
        "TX",
        "UT",
        "VT",
        "VA",
        "VI",
        "WA",
        "WV",
        "WI",
        "WY",
    ]
    if state and state in list_of_states_in_us:
        return "US"
    else:
        return None


def consolidate_country(
    country, derived_country_based_on_latlon, derived_country_based_on_state
):
    if country:
        return country
    elif derived_country_based_on_latlon:
        return derived_country_based_on_latlon
    elif derived_country_based_on_state:
        return derived_country_based_on_state
    else:
        return None


def consolidate_place_col(place, place_based_on_code):
    if place:
        return place
    if place_based_on_code:
        return place_based_on_code
    return None


def transform_map_state_to_country(data):
    map_us_state_to_country_udf = f.udf(map_us_state_to_country, t.StringType())

    transformed_data = data.withColumn(
        "derived_country_based_on_state", map_us_state_to_country_udf(f.col("state"))
    )
    return transformed_data


def transform_get_country_from_latlon(
    spark: SparkSession, data, country_shapes_cleaned
):
    country_shapes_min_max = country_shapes_cleaned.groupBy("polygon_id").agg(
        f.min("latitude").alias("latitude_min"),
        f.max("latitude").alias("latitude_max"),
        f.min("longitude").alias("longitude_min"),
        f.max("longitude").alias("longitude_max"),
    )

    country_shapes_broadcast = spark.sparkContext.broadcast(
        country_shapes_cleaned.collect()
    )
    country_shapes_min_max_broadcast = spark.sparkContext.broadcast(
        country_shapes_min_max.collect()
    )

    def get_country_based_on_latitude_longitude(latitude, longitude):
        country_shapes_values = country_shapes_broadcast.value
        country_shapes_min_max_values = country_shapes_min_max_broadcast.value

        if latitude and longitude:
            filtered_polygon = [
                polygon.polygon_id
                for polygon in country_shapes_min_max_values
                if (
                    polygon.latitude_min <= latitude <= polygon.latitude_max
                    and polygon.longitude_min <= longitude <= polygon.longitude_max
                )
            ]
            filtered_country_shapes_values = [
                p for p in country_shapes_values if p.polygon_id in filtered_polygon
            ]
            total_polygons = len(filtered_country_shapes_values)

            target = LatLon(latitude, longitude)
            polygon = []
            prev_polygon_id = None
            prev_country_id = None
            for idx, point in enumerate(filtered_country_shapes_values):
                if idx == total_polygons - 1:
                    if len(polygon) > 0:
                        try:
                            if target.isenclosedBy(polygon):
                                return point.ISO
                        except PointsError:
                            print("Insufficient number of points")
                    else:
                        return None
                elif point.polygon_id == prev_polygon_id:
                    polygon.append(LatLon(point.latitude, point.longitude))
                else:
                    if len(polygon) > 0:
                        try:
                            if target.isenclosedBy(polygon):
                                return prev_country_id
                        except PointsError:
                            print("Insufficient number of points")
                    else:
                        polygon = [LatLon(point.latitude, point.longitude)]
                prev_polygon_id = point.polygon_id
                prev_country_id = point.ISO
        else:
            return None

    get_country_based_on_latitude_longitude_udf = f.udf(
        get_country_based_on_latitude_longitude, t.StringType()
    )
    location_with_latlon = data.withColumn(
        "derived_country_based_on_latlon",
        get_country_based_on_latitude_longitude_udf(
            f.col("latitude"), f.col("longitude")
        ),
    )

    return location_with_latlon


def transform_consolidate_country(data):
    consolidate_country_udf = f.udf(consolidate_country, t.StringType())

    location_combined = data.withColumn(
        "final_country",
        consolidate_country_udf(
            f.col("country"),
            f.col("derived_country_based_on_latlon"),
            f.col("derived_country_based_on_state"),
        ),
    )
    return location_combined


def transform_add_country_full_name(spark: SparkSession, data, country_info):
    # Enrich using country_info
    location_combined = data.join(
        country_info, (data.final_country == country_info.ISO), "left"
    ).drop(data.country)

    # Enrich using manual mapping
    data = [
        ("SU", "Soviet Union", "EU"),
        ("DT", "Germany", "EU"),
        ("JA", "Japan", "AS"),
        ("EN", "United Kingdom", "EU"),
        ("UK", "United Kingdom", "EU"),
        ("YU", "Yugoslavia", "EU"),
        ("SW", "Sweden", "EU"),
        ("DD", "Germany", "EU"),
    ]

    columns = ["code", "country_based_on_code", "continent_based_on_code"]

    additional_code = spark.createDataFrame(data=data, schema=columns)

    location_combined = location_combined.join(
        additional_code, location_combined.final_country == additional_code.code, "left"
    ).drop(additional_code.code)

    return location_combined


def transform_consolidate_country_and_continent(data):
    consolidate_place_col_udf = f.udf(consolidate_place_col, t.StringType())

    location_combined = data.withColumn(
        "new_country", consolidate_place_col_udf("Country", "country_based_on_code")
    )

    location_combined = location_combined.withColumn(
        "new_continent",
        consolidate_place_col_udf("Continent", "continent_based_on_code"),
    )

    location_combined = location_combined.select("id", "new_country", "new_continent")

    return location_combined


def transform_generate_new_id(data):
    location_with_country_id = (
        data.groupBy("new_country")
        .count()
        .withColumn("new_id", f.sha2(f.col("new_country"), 256))
        .select(["new_country", "new_id"])
    )

    location_combined = data.join(
        location_with_country_id,
        (location_with_country_id.new_country == data.new_country),
        "left",
    ).drop(location_with_country_id.new_country)

    return location_combined


def transform_data(spark: SparkSession, data, country_shapes_cleaned, country_info):
    location = transform_map_state_to_country(data)

    location_with_latlon = location.filter(
        f.col("country").isNull() & f.col("latitude").isNotNull()
    )
    location_without_latlon = location.filter(
        ~(f.col("country").isNull() & f.col("latitude").isNotNull())
    )
    location_without_latlon = location_without_latlon.withColumn(
        "derived_country_based_on_latlon", f.lit(None)
    )

    location_with_latlon = transform_get_country_from_latlon(
        spark, location_with_latlon, country_shapes_cleaned
    )

    location_combined = location_without_latlon.union(location_with_latlon)

    location_combined = transform_consolidate_country(location_combined)

    location_combined = transform_add_country_full_name(
        spark, location_combined, country_info
    )

    location_combined = transform_consolidate_country_and_continent(location_combined)

    location_combined = transform_generate_new_id(location_combined)

    location_combined = location_combined.select(
        "id",
        "new_id",
        f.col("new_country").alias("Country"),
        f.col("new_continent").alias("Continent"),
    )

    location_combined = location_combined.filter(f.col("Country").isNotNull())

    return location_combined


# entry point for PySpark ETL application
if __name__ == "__main__":
    main()
