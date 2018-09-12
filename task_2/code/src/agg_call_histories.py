# Databricks notebook source
# Author: Le Van-Duyet
# Task_2
# Aggregation code

# COMMAND ----------

# Config
DEBUG=True

# This notebook using Databricks, it can be break in EMR because of missing some utils function.

# COMMAND ----------

df = spark.read.parquet('processed/call_histories_20151201.parquet')
if DEBUG: display(df)

# COMMAND ----------

from pyspark.sql.functions import count, sum, when, hour,\
                                  col, row_number, desc, collect_list
from pyspark.sql.window import Window

df = df.withColumn('working_hour', hour('START_TIME'))

df_agg = df.groupby('FROM_PHONE_NUMBER').agg(
  count('*').alias('NUMBER_OF_CALL'),
  sum('CALL_DURATION').alias('TOTAL_CALL_DURATION'),
  count(when((col('working_hour') >= 8) & (col('working_hour') <= 17),
             col('working_hour'))).alias('CALL_IN_WORKING_HOUR'),
)

# Find the IMEI which make most call. 
df_grouped_imei = df.groupby('FROM_PHONE_NUMBER', 'IMEI').count()
window = Window.partitionBy('FROM_PHONE_NUMBER').orderBy(desc('count'))
most_imei = df_grouped_imei\
                  .withColumn('order', row_number().over(window))\
                  .where(col('order') == 1)\
                  .select('FROM_PHONE_NUMBER', col('IMEI').alias('MOST_IMEI'))

# Find top 2 locations which make most call
df_grouped_location = df.groupby('FROM_PHONE_NUMBER', 'LOCATION').count()
window = Window.partitionBy('FROM_PHONE_NUMBER').orderBy(desc('count'))
most_2_loc = df_grouped_location\
                  .withColumn('order', row_number().over(window))\
                  .where(col('order') <= 2)\
                  .select('FROM_PHONE_NUMBER', col('LOCATION').alias('MOST_LOCATION'))
most_2_loc = most_2_loc.groupby('FROM_PHONE_NUMBER') \
                       .agg(collect_list('MOST_LOCATION').alias('MOST_LOCATION'))

# Join output
output_df = df_agg.join(most_imei, on='FROM_PHONE_NUMBER').join(most_2_loc, on='FROM_PHONE_NUMBER')

if DEBUG: display(output_df)

# COMMAND ----------

output_df.write.parquet('application/call_histories_20151201.parquet', mode='overwrite')
