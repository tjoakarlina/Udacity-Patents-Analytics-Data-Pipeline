import pyspark.sql.functions as f

from dependencies.spark import start_spark
from dependencies.utils import extract_tsv_data, load_data_to_s3_as_parquet


def main():
    spark, log, config = start_spark(
        app_name="cleaning_patent_job", files=["../configs/etl_config.json"]
    )

    log.warn("Cleaning Patent Job is up and running")

    data = extract_tsv_data(spark, f"{config['raw_data_s3_path']}patent.tsv")

    data_transformed = transform_data(data)

    load_data_to_s3_as_parquet(
        spark,
        data_transformed,
        path=f"{config['cleaned_data_s3_path']}patent.parquet",
        partition="year",
    )

    log.warn("Cleaning Patent Job is finished")
    spark.stop()
    return None


def transform_data(data):
    transformed_data = (
        data.filter(f.col("withdrawn") == 0)
        .filter(f.year(f.col("date")) > 1999)
        .select("id", "type", "date", "abstract", "title", "kind", "num_claims")
    )
    transformed_data = transformed_data.withColumn("year", f.year(f.col("date")))

    return transformed_data


# entry point for PySpark ETL application
if __name__ == "__main__":
    main()
