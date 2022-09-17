import pyspark.sql.functions as f
import pyspark.sql.types as t

from dependencies.spark import start_spark
from dependencies.utils import extract_tsv_data, load_data_to_s3_as_parquet


def main():
    spark, log, config = start_spark(
        app_name="cleaning_inventor_job", files=["../configs/etl_config.json"]
    )

    log.warn("Cleaning Inventor Job is up and running")

    data = extract_tsv_data(spark, f"{config['raw_data_s3_path']}inventor.tsv")

    data_transformed = transform_data(data)

    load_data_to_s3_as_parquet(
        spark,
        data_transformed,
        path=f"{config['cleaned_data_s3_path']}inventor.parquet",
    )

    log.warn("Cleaning Inventor Job is finished")
    spark.stop()
    return None


def combine_name(name_first, name_last):
    if name_first and name_last:
        return f"{name_first} {name_last}"
    elif name_last:
        return name_last
    elif name_first:
        return name_first
    else:
        return None


def transform_data(inventor):
    combine_name_udf = f.udf(combine_name, t.StringType())
    inventor = inventor.withColumn(
        "name", combine_name_udf(f.col("name_first"), f.col("name_last"))
    )
    inventor = inventor.withColumn("type", f.lit("individual")).select(
        "id", "name", "type"
    )

    inventor_cleaned_with_id = (
        inventor.groupBy(["name", "type"])
        .count()
        .withColumn("new_id", f.sha2(f.concat(f.col("name"), f.col("type")), 256))
        .select(["name", "new_id"])
    )

    inventor_cleaned_with_id = inventor.join(
        inventor_cleaned_with_id, inventor.name == inventor_cleaned_with_id.name
    ).drop(inventor_cleaned_with_id.name)

    return inventor_cleaned_with_id


# entry point for PySpark ETL application
if __name__ == "__main__":
    main()
