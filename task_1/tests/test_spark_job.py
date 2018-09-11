"""
Test for main function
"""

import unittest
from os.path import dirname, realpath

import spark_job


class MainTests(unittest.TestCase):
    """Test suite for main
    """

    def setUp(self):
        """Start Spark, define config
        """
        self.spark = spark_job.get_spark()
        self.test_data_path = dirname(realpath(__file__)) + '/test_data'

    def tearDown(self):
        """Stop Spark
        """
        self.spark.stop()

    def test_transform_data_1(self):
        """Test main transform function
        """
        input_df = spark_job.load_data(
            self.spark, self.test_data_path + '/data1.csv')
        output_df = spark_job.process_data(self.spark, input_df)
        self.assertTrue(output_df.count() > 0)
