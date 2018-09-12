# Databricks notebook source
# Author: Le Van-Duyet
# Task_2
# Transformation code

# COMMAND ----------

import re
from pyspark.sql.functions import udf, col, \
                                  to_timestamp

# COMMAND ----------

# Config
DEBUG=True

# This notebook using Databricks, it can be break in EMR because of missing some utils function.

# COMMAND ----------

df = spark.read.csv('call_histories_20151201.csv', header=True, sep=';')

if DEBUG: display(df)

# COMMAND ----------

clean_call_duration = udf(lambda a: int(re.sub('[^0-9]','', str(a))))
clean_imei = udf(lambda a: int(re.sub('[^0-9]','', str(a))))

df = df.withColumn('CALL_DURATION', clean_call_duration(col('CALL_DURATION')))
df = df.withColumn('IMEI', clean_imei(col('IMEI')))
df = df.withColumn('START_TIME', to_timestamp('START_TIME', 'dd/MM/yyyy HH:mm:ss'))
# TODO: clean all column, using MetaData table

if DEBUG: display(df)

# COMMAND ----------

df = df.drop_duplicates()
# TODO: drop duplicate with previous data, using hashtable, bloom filter, ...

if DEBUG: display(df)

# COMMAND ----------

df.write.parquet('processed/call_histories_20151201.parquet', mode='overwrite')

# COMMAND ----------


