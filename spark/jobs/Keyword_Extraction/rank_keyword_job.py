import pyspark.sql.functions as f
import pyspark.sql.window as w

from dependencies.spark import start_spark
from dependencies.utils import extract_parquet_data


def main():
    spark, log, config = start_spark(
        app_name="rank_keyword_job", files=["../configs/etl_config.json"]
    )

    log.warn("Rank Keyword Job is up and running")

    data = extract_parquet_data(
        spark, path=f"{config['cleaned_data_s3_path']}patent_keyword_raw.parquet"
    )
    data_transformed = transform_data(data)

    data_transformed.write.mode("overwrite").parquet(
        path=f"{config['cleaned_data_s3_path']}patent_keyword.parquet"
    )

    log.warn("Rank Keyword Job is finished")
    spark.stop()
    return None


def transform_data(data):
    data = (
        data.selectExpr(
            "id",
            "explode(arrays_zip(keywords.result, keywords.metadata)) as resultTuples",
        )
        .selectExpr(
            "id", "resultTuples['0'] as keyword", "resultTuples['1'].score as score"
        )
        .dropDuplicates(["id", "keyword"])
    )

    window = w.Window.partitionBy("id").orderBy(f.col("score").asc())

    return data.withColumn("row", f.row_number().over(window)).filter(
        f.col("row") <= 10
    )


# entry point for PySpark ETL application
if __name__ == "__main__":
    main()
