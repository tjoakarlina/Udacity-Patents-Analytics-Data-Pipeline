"""
test_cleaning_patent_inventor_job.py
~~~~~~~~~~~~~~~

This module contains unit tests for the transformation steps of the ETL
job defined in cleaning_patent_inventor_job.py. It makes use of a local version of PySpark
that is bundled with the PySpark package.
"""
import unittest

from jobs.Data_Cleaning.cleaning_patent_inventor_job import transform_data
from tests.common import SparkETLTests
from tests.data import CleanedData, RawData


class CleaningPatenInventorTests(SparkETLTests):
    """Test suite for transformation in cleaning_patent_inventor_job.py"""

    def test_transform_data(self):
        """Test data transformer.

        Using small chunks of input data and expected output data, we
        test the transformation step to make sure it's working as
        expected.
        """
        # assemble

        patent_inventor_df = self.spark.createDataFrame(
            data=RawData.patent_inventor, schema=RawData.patent_inventor_schema
        )

        expected_data_df = self.spark.createDataFrame(
            data=CleanedData.patent_inventor, schema=CleanedData.patent_inventor_schema
        )

        data_transformed = transform_data(patent_inventor_df)

        self.check_schema(data_transformed, expected_data_df)
        self.check_data(data_transformed, expected_data_df)


if __name__ == "__main__":
    unittest.main()
