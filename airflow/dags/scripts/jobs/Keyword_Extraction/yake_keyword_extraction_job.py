import pyspark.sql.functions as f
import pyspark.sql.types as t

from sparknlp.base import *
from sparknlp.annotator import *
from pyspark.ml import Pipeline

from dependencies.spark import start_spark
from dependencies.utils import extract_parquet_data


def main():
    spark, log, config = start_spark(
        app_name="yake_keyword_extraction_job", files=["../configs/etl_config.json"]
    )

    log.warn("YAKE Keyword Extraction Job is up and running")

    data = extract_parquet_data(
        spark, path=f"{config['cleaned_data_s3_path']}patent.parquet"
    )
    data_transformed = transform_data(data)

    data_transformed.write.mode("overwrite").parquet(
        path=f"{config['cleaned_data_s3_path']}patent_keyword_raw.parquet"
    )

    log.warn("YAKE Keyword Extraction Job is finished")
    spark.stop()
    return None


def combine_details(abstract, title):
    details = ""
    if title:
        details = details + title
    if abstract:
        details = details + abstract
    return details


def create_yake_pipeline():
    document_assembler_stage = (
        DocumentAssembler().setInputCol("details").setOutputCol("document")
    )
    sentence_detector_stage = (
        SentenceDetector().setInputCols("document").setOutputCol("sentence")
    )
    tokenizing_stage = Tokenizer().setInputCols(["sentence"]).setOutputCol("token")
    keyword_extraction_stage = (
        YakeKeywordExtraction()
        .setInputCols(["token"])
        .setOutputCol("keywords")
        .setMinNGrams(1)
        .setMaxNGrams(2)
        .setWindowSize(2)
        .setThreshold(0.6)
    )

    pipeline = Pipeline().setStages(
        [
            document_assembler_stage,
            sentence_detector_stage,
            tokenizing_stage,
            keyword_extraction_stage,
        ]
    )
    return pipeline


def transform_data(data):
    combine_details_udf = f.udf(combine_details, t.StringType())
    data = data.withColumn(
        "details", combine_details_udf(f.col("abstract"), f.col("title"))
    ).select(f.col("id"), f.col("details"))

    yake_pipeline = create_yake_pipeline()

    result = yake_pipeline.fit(data).transform(data)

    result = result.select("id", "keywords")

    return result


# entry point for PySpark ETL application
if __name__ == "__main__":
    main()
