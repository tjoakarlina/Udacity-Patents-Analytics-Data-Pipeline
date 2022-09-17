"""
test_cleaning_country_shapes_job.py
~~~~~~~~~~~~~~~

This module contains unit tests for the transformation steps of the ETL
job defined in cleaning_country_shapes_job.py. It makes use of a local version of PySpark
that is bundled with the PySpark package.
"""
import unittest

from jobs.Data_Cleaning.cleaning_country_shapes_job import transform_data
from tests.common import SparkETLTests
from tests.data import RawData


class CleaningCountryShapesTests(SparkETLTests):
    """Test suite for transformation in cleaning_country_shapes_job.py"""

    def test_transform_data(self):
        """Test data transformer.

        Using small chunks of input data and expected output data, we
        test the transformation step to make sure it's working as
        expected.
        """
        # assemble

        country_shapes_df = self.spark.createDataFrame(
            data=RawData.country_shapes, schema=RawData.country_shapes_schema
        )

        country_info_df = self.spark.read.options(
            header="True",
            delimiter="\t",
            inferSchema="True",
            escape='"',
            multiline="True",
        ).csv(f"{self.test_data_path}country_info.txt")

        expected_data_df = self.spark.read.options(
            header="True",
            delimiter=",",
            inferSchema="True",
            escape='"',
            multiline="True",
        ).csv(f"{self.test_data_path}country_shapes_cleaned.csv")

        data_transformed = transform_data(country_shapes_df, country_info_df)

        self.check_schema(data_transformed, expected_data_df)
        self.check_data(data_transformed, expected_data_df)


if __name__ == "__main__":
    unittest.main()
