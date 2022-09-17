import pyspark.sql.functions as f
import pyspark.sql.types as t
import json

from dependencies.spark import start_spark
from dependencies.utils import (
    extract_tsv_data,
    extract_json_data,
    load_data_to_s3_as_csv,
)


def main():
    spark, log, config = start_spark(
        app_name="cleaning_country_shapes_job", files=["../configs/etl_config.json"]
    )

    log.warn("Cleaning Country Shapes Job is up and running")

    country_shapes = extract_json_data(
        spark, path=f"{config['raw_data_s3_path']}shapes_simplified_low.json"
    )

    country_info = extract_tsv_data(
        spark, path=f"{config['raw_data_s3_path']}countryInfo.txt", partition_number=1
    )

    data_transformed = transform_data(country_shapes, country_info)

    load_data_to_s3_as_csv(
        spark,
        data_transformed,
        path=f"{config['cleaned_data_s3_path']}country_shapes.csv",
    )

    log.warn("Cleaning Country Shapes Job is finished")
    spark.stop()
    return None


def convert_string_to_array(string):
    result = json.loads(string)
    return result


def clean_country_shapes(country_shapes):
    # Explode the features column
    country_shapes = country_shapes.select(f.explode("features"))
    country_shapes = country_shapes.select(
        "col.properties.geoNameId", "col.geometry.coordinates", "col.geometry.type"
    )

    # Explode the coordinates column
    country_shapes = country_shapes.select(
        "geoNameId", "type", f.explode("coordinates").alias("polygon")
    )

    # Create unique id for each polygon
    country_shapes = country_shapes.withColumn(
        "polygon_id", f.sha2(f.col("polygon").cast("string"), 256)
    ).select("geoNameId", "type", "polygon", "polygon_id")

    # Explode the polygon column
    country_shapes = country_shapes.select(
        "geoNameId", "type", f.explode("polygon").alias("polygon"), "polygon_id"
    )

    # Separate the polygon and multi-polygon type
    country_polygon = country_shapes.filter(f.col("type") == "Polygon")
    country_multi_polygon = country_shapes.filter(f.col("type") == "MultiPolygon")

    # Explode the polygon for country with multi-polygon type
    country_multi_polygon = country_multi_polygon.select(
        "geoNameId", "type", f.explode("polygon").alias("polygon"), "polygon_id"
    )

    # Convert array which is represented as string to array
    convert_string_to_array_udf = f.udf(
        convert_string_to_array, t.ArrayType(t.StringType())
    )
    country_multi_polygon = country_multi_polygon.withColumn(
        "new_polygon", convert_string_to_array_udf(f.col("polygon"))
    )
    country_multi_polygon = country_multi_polygon.select(
        "geoNameId", "type", f.col("new_polygon").alias("polygon"), "polygon_id"
    )

    # Combine the country with polygon type and country with multi-polygon type
    country_shapes = country_polygon.union(country_multi_polygon)

    # Extract the latitude and longitude
    country_shapes = country_shapes.withColumn(
        "latitude", f.col("polygon").getItem(1)
    ).withColumn("longitude", f.col("polygon").getItem(0))

    country_shapes = country_shapes.select(
        "geoNameId", "type", "polygon_id", "latitude", "longitude"
    )
    return country_shapes


def transform_data(country_shapes, country_info):
    country_shapes_cleaned = clean_country_shapes(country_shapes)
    transformed_data = country_shapes_cleaned.join(
        country_info, (country_shapes_cleaned.geoNameId == country_info.geonameid)
    ).drop(country_info.geonameid)

    transformed_data = transformed_data.select(
        "geoNameId",
        "type",
        "polygon_id",
        f.col("latitude").cast("double").alias("latitude"),
        f.col("longitude").cast("double").alias("longitude"),
        "ISO",
        "Country",
        "Continent",
    )

    return transformed_data


# entry point for PySpark ETL application
if __name__ == "__main__":
    main()
