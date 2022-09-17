import pyspark.sql.functions as f

from dependencies.spark import start_spark
from dependencies.utils import extract_tsv_data, load_data_to_s3_as_parquet


def main():
    spark, log, config = start_spark(
        app_name="cleaning_wipo_field_job", files=["../configs/etl_config.json"]
    )

    log.warn("Cleaning Wipo Field Job is up and running")

    data = extract_tsv_data(spark, f"{config['raw_data_s3_path']}wipo_field.tsv")

    data_transformed = transform_data(data)

    load_data_to_s3_as_parquet(
        spark,
        data_transformed,
        path=f"{config['cleaned_data_s3_path']}wipo_field.parquet",
    )

    log.warn("Cleaning Wipo Field Job is finished")
    spark.stop()
    return None


def transform_data(data):
    transformed_data = data.filter(~f.col("id").startswith("D"))
    return transformed_data


# entry point for PySpark ETL application
if __name__ == "__main__":
    main()
