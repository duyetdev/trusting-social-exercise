"""
spark_job.py
~~~~~

This Python python contains an process code for task_1
It can be submitted to a Spark cluster (or locally) using the 'spark-submit'
command found in the '/bin' directory of all Spark distributions
(necessary for running any Spark job, locally or otherwise).
This example script can be executed as follows,


    $SPARK_HOME/bin/spark-submit \
    --master local[*] \
    --py-files dependencies.zip \
    --files spark_job_config.json \
    spark_job.py

where dependencies.zip contains Python modules required by Spark job.
"""

import argparse
import sys
from os import listdir, path
import json

from pyspark import SparkFiles
from pyspark.sql import Row, SparkSession
from pyspark.sql.functions import *
from pyspark.sql.functions import col, lag, rank, row_number
from pyspark.sql.types import DateType, StringType, StructField, StructType
from pyspark.sql.window import Window
from utils import logger


def main():
    """
    Main spark job definition

    :return: None
    """

    # Get args for spark job
    parser = argparse.ArgumentParser(description='Find real activation date')
    parser.add_argument('--format', default='csv',
                        help='format of input file: csv or parquet')
    parser.add_argument('--path', help='path of input data set '
                                       '(e.g. file:///home/duyetdev/data.csv, hdfs:///data.csv)')
    parser.add_argument('--output', help='path of input data set '
                                         '(e.g. file:///home/duyetdev/output.csv, hdfs:///output.csv)')
    parser.add_argument(
        '--debug', help='turn on debug mode', action='store_true')
    args = parser.parse_args()

    if not args.path:
        parser.print_help()
        sys.exit(-1)

    # We got --format file and source --path
    INPUT_FORMAT = args.format
    INPUT_PATH = args.path
    OUTPUT_PATH = args.output
    DEBUG = True if args.debug else False

    # Start Spark application and get logger, config
    spark = get_spark()
    log = get_logger()
    config = get_spark_config()
    log.warn(config)

    # log that main ETL job is starting
    log.warn('spark_job is up !!')
    log.warn('INPUT_PATH={}\nINPUT_FORMAT={}'
             '\nOUTPUT_PATH={}\nDEBUG={}'.format(INPUT_PATH, INPUT_FORMAT,
                                                 OUTPUT_PATH, DEBUG))

    # Pipeline
    data = load_data(spark, INPUT_PATH, INPUT_FORMAT)
    if DEBUG:
        data.show()

    data_output = process_data(
        spark, data, drop_duplicated=config.get('drop_duplicated_output'))
    if DEBUG:
        data_output.show()

    if args.output:
        log.warn('Write output to %s' % args.output)
        write_data(data_output, OUTPUT_PATH,
                   format=INPUT_FORMAT, mode='overwrite')
    else:
        data_output.show()

    # Log success, stop spark
    log.warn('spark_job is finished')
    spark.stop()
    return None


def get_spark(app_name='trusting_social_task_1'):
    """Start the spark session

    :param app_name: Name of Spark app
    :return: Spark session object
    """
    spark_builder = SparkSession.builder.appName(app_name)
    return spark_builder.getOrCreate()


def get_logger(app_name='trusting_social_task_1', spark_sess=None):
    """Get the Spark logger, given app_name or Spark session object.

    :param app_name: Name of Spark app
    :param spark_sess: Spark session object
    :return: logger object
    """
    if not spark_sess:
        spark = get_spark(app_name)
    else:
        spark = spark_sess

    return logger.Log4j(spark)


def get_spark_config():
    """Get the spark config dict.
    This function looks for a file ending with '.json'
    that can be send to Spark job by --files param.
    If it is found, it is opened, the contents parsed
    (assuming it contains valid JSON for the ETL job
    configuration)
    :return: dict contains configuration
    """
    # get config file if sent to cluster with --files
    spark_files_dir = SparkFiles.getRootDirectory()
    config_files = [filename
                    for filename in listdir(spark_files_dir)
                    if filename.endswith('.json')]

    if len(config_files) != 0:
        path_to_config_file = path.join(spark_files_dir, config_files[0])
        with open(path_to_config_file, 'r') as config_file:
            config_json = config_file.read().replace('\n', '')
        config_dict = json.loads(config_json)
    else:
        config_dict = {}
    return config_dict


def load_data(spark=None, input_path=None, format='csv', auto_schema=False):
    """Load data from CSV or parquet file

    :param spark: Spark session object.
    :param input_path: Path of input file.
    :param format: Format of input file (expert csv or parquet).
    :return: Spark Dataframe.
    """
    assert format in ['csv', 'parquet'], 'Format should be "csv" or "parquet"'

    reader = spark.read.option("header", "true")

    if not auto_schema:
        schema = StructType([
            StructField('PHONE_NUMBER', StringType(), False),
            StructField('ACTIVATION_DATE', DateType(), False),
            StructField('DEACTIVATION_DATE', DateType(), True)
        ])
        reader = reader.schema(schema)

    df = reader.parquet(
        input_path) if format == 'parquet' else reader.csv(input_path)

    return df


def process_data(spark, df, drop_duplicated=False):
    """Process data

    :param spark: Spark session object.
    :param df: Input DataFrame
    :return: DataFrame
    """
    w1 = Window().partitionBy('PHONE_NUMBER').orderBy(
        col('ACTIVATION_DATE').asc())
    w2 = Window().partitionBy('PHONE_NUMBER').orderBy(
        col('ACTIVATION_DATE').desc())

    # Check if the activation_date == deactive_date of previous record?
    df2 = df.withColumn('is_first_of_current_user', col('ACTIVATION_DATE') !=
                        lag('DEACTIVATION_DATE').over(w1)).fillna({'is_first_of_current_user': True})

    # Get the latest user, first row of this one
    df3 = df2.where('is_first_of_current_user == True') \
             .withColumn('rank', rank().over(w2))

    # Select latest user
    df3 = df3.filter('rank == 1').select(col('PHONE_NUMBER'),
                                         col('ACTIVATION_DATE').alias('REAL_ACTIVATION_DATE'))

    return df3.drop_duplicates() if drop_duplicated else df3


def write_data(df, path, format='csv', mode='overwrite', coalesce=1):
    """Write output data to csv or parquet file

    :param df: DataFrame to be written to file
    :param path: output path
    :param format: output format: csv for parquet
    :param mode: write mode (overwrite, append, ignore, error)
    :param coalesce: number of partittion
    :return: void
    """
    assert format in ['csv', 'parquet'], 'Format should be "csv" or "parquet"'
    assert mode in ['overwrite', 'append', 'ignore', 'error'], \
        'Write mode is invalid'

    (df.coalesce(coalesce)
       .write
       .format(format)
       .option('header', True)
       .save(path, mode=mode))


if __name__ == '__main__':
    main()
