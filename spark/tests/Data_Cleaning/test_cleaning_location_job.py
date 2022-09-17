"""
test_cleaning_location_job.py
~~~~~~~~~~~~~~~

This module contains unit tests for the transformation steps of the ETL
job defined in cleaning_location_job.py. It makes use of a local version of PySpark
that is bundled with the PySpark package.
"""
import unittest

from jobs.Data_Cleaning.cleaning_location_job import transform_data
from tests.common import SparkETLTests
from tests.data import CleanedData, RawData


class CleaningLocationTests(SparkETLTests):
    """Test suite for transformation in cleaning_location_job.py"""

    def test_transform_data(self):
        """Test data transformer.

        Using small chunks of input data and expected output data, we
        test the transformation step to make sure it's working as
        expected.
        """
        # assemble

        location_df = self.spark.createDataFrame(
            data=RawData.location, schema=RawData.location_schma
        )

        country_info_df = self.spark.read.options(
            header="True",
            delimiter="\t",
            inferSchema="True",
            escape='"',
            multiline="True",
        ).csv(f"{self.test_data_path}country_info.txt")

        country_shapes_cleaned_df = self.spark.read.options(
            header="True",
            delimiter=",",
            inferSchema="True",
            escape='"',
            multiline="True",
        ).csv(f"{self.test_data_path}country_shapes_cleaned.csv")

        expected_data_df = self.spark.createDataFrame(
            data=CleanedData.location, schema=CleanedData.location_schema
        )

        data_transformed = transform_data(
            self.spark, location_df, country_shapes_cleaned_df, country_info_df
        )

        self.check_schema(data_transformed, expected_data_df)
        self.check_data(data_transformed, expected_data_df)


if __name__ == "__main__":
    unittest.main()
