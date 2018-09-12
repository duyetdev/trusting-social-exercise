"""
Test for main function
"""

import unittest
from os import listdir, path
from os.path import dirname, realpath, join

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

    def test_transform_data(self):
        """Test main transform function

        Only test process_data due to lack of time
        """

        list_test_data = [filename
                          for filename in listdir(self.test_data_path)
                          if filename.endswith('_test.csv')]

        # Load input test file (e.g. data1_test.csv) and validation
        # file (e.g. data1_validation.csv)
        for input_file in list_test_data:
            print('Load', input_file)
            input_df = spark_job.load_data(
                self.spark, join(self.test_data_path, input_file))
            output_validation = spark_job.load_data(
                self.spark, join(self.test_data_path, input_file.replace(
                    '_test.csv', '_validation.csv')),
                auto_schema=True)

            output = spark_job.process_data(
                self.spark, input_df, drop_duplicated=True)
            output.show()

            self.assertTrue(output.count() > 0)
            self.assertTrue(output.subtract(output_validation).count() == 0)
            self.assertTrue(output_validation.subtract(output).count() == 0)
