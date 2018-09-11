## Task 1 [Algorithm and coding]: Find the actual activation date of a phone number

## Project structure

```
.
├── build_dependencies.sh
├── README.md
├── spark_job_config.json
├── spark_job.py
├── tests
│   ├── test_data
│   │   ├── phone_number_1
│   │   └── phone_number_2
│   └── test_spark_job.py
└── utils
    └── logger.py
```

- `spark_job.py`: main module which will be sent to the Spark cluster.
- `spark_job_config.json`: external configuration parameters required by `spark_job.py`, stored in JSON format.
- `build.sh`: a bash script for building the dependencies into a zip-file to be sent to the cluster.
- `utils/`: additional modules that support spark job.
- `tests/`: Unit test modules, includes `test_data` folder.

## Submit the job

Assuming that:

    - The `$SPARK_HOME` environment variable points to your local Spark installation folder.
    - You install spark in local.

From this folder, build dependencies and submit to Spark:

```sh
sh build_dependencies.sh 

$SPARK_HOME/bin/spark-submit \
    --master local[*] \
    --py-files dependencies.zip \
    --files spark_job_config.json \
    spark_job.py
```

Modify the `--master` option with your Spark IP (either in single-executor mode locally or something larger in the cloud) - e.g. `--master spark://localhost:7077`

## Testing
