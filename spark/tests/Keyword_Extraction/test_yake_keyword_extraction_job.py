"""
test_yake_keyword_extraction_job.py
~~~~~~~~~~~~~~~

This module contains unit tests for the transformation steps of the ETL
job defined in yake_keyword_extraction_job.py. It makes use of a local version of PySpark
that is bundled with the PySpark package.
"""
import unittest
from dependencies.spark import start_spark
from jobs.Keyword_Extraction.yake_keyword_extraction_job import transform_data
from tests.common import SparkETLTests
from tests.data import CleanedData, KeywordExtractionData


class YakeKeywordExtractionTests(SparkETLTests):
    """Test suite for transformation in yake_keyword_extraction_job.py"""

    def setUp(self):
        self.spark, *_ = start_spark(
            jar_packages=["com.johnsnowlabs.nlp:spark-nlp_2.12:4.1.0"]
        )

    def test_transform_data(self):
        """Test data transformer.

        Using small chunks of input data and expected output data, we
        test the transformation step to make sure it's working as
        expected.
        """
        # assemble

        patent_cleaned_df = self.spark.createDataFrame(
            data=CleanedData.patent, schema=CleanedData.patent_schema
        )

        expected_data_df = self.spark.createDataFrame(
            data=[], schema=KeywordExtractionData.patent_keyword_raw_schema
        )

        data_transformed = transform_data(patent_cleaned_df)
        self.check_schema(data_transformed, expected_data_df)


if __name__ == "__main__":
    unittest.main()
