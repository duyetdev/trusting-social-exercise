spark-submit --files spark_job_config.json \
    spark_job.py --format csv \
                 --path tests/test_data/data1_test.csv \
                 --output data1_output.csv
