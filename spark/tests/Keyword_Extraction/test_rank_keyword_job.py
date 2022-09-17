"""
test_rank_keyword_job.py
~~~~~~~~~~~~~~~

This module contains unit tests for the transformation steps of the ETL
job defined in rank_keyword_job.py. It makes use of a local version of PySpark
that is bundled with the PySpark package.
"""
import unittest
from jobs.Keyword_Extraction.rank_keyword_job import transform_data
from tests.common import SparkETLTests
from tests.data import KeywordExtractionData


class RankKeywordTests(SparkETLTests):
    """Test suite for transformation in rank_keyword_job.py"""

    def test_transform_data(self):
        """Test data transformer.

        Using small chunks of input data and expected output data, we
        test the transformation step to make sure it's working as
        expected.
        """
        # assemble

        patent_cleaned_df = self.spark.createDataFrame(
            data=KeywordExtractionData.patent_keyword_raw,
            schema=KeywordExtractionData.patent_keyword_raw_schema,
        )

        expected_data_df = self.spark.createDataFrame(
            data=KeywordExtractionData.patent_keyword,
            schema=KeywordExtractionData.patent_keyword_schema,
        )

        data_transformed = transform_data(patent_cleaned_df)
        self.check_schema(data_transformed, expected_data_df)


if __name__ == "__main__":
    unittest.main()
