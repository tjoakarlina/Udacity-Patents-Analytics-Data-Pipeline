from dependencies.spark import start_spark
from dependencies.utils import extract_tsv_data, load_data_to_s3_as_parquet


def main():
    spark, log, config = start_spark(
        app_name="cleaning_patent_assignee_job", files=["../configs/etl_config.json"]
    )

    log.warn("Cleaning Patent Assignee Job is up and running")

    data = extract_tsv_data(spark, f"{config['raw_data_s3_path']}patent_assignee.tsv")

    data_transformed = transform_data(data)

    load_data_to_s3_as_parquet(
        spark,
        data_transformed,
        path=f"{config['cleaned_data_s3_path']}patent_assignee.parquet",
    )

    log.warn("Cleaning Patent Assignee Job is finished")
    spark.stop()
    return None


def transform_data(patent_assignee):
    patent_assignee = patent_assignee.dropDuplicates(["patent_id"])
    return patent_assignee


# entry point for PySpark ETL application
if __name__ == "__main__":
    main()
