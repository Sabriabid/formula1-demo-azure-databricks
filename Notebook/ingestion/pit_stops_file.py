# Databricks notebook source
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType

# COMMAND ----------

pit_stops_schema = StructType(fields=[StructField("raceId", IntegerType(), False),
                                      StructField("driverId", IntegerType(), True),
                                      StructField("stop", StringType(), True),
                                      StructField("lap", IntegerType(), True),
                                      StructField("time", StringType(), True),
                                      StructField("duration", StringType(), True),
                                      StructField("milliseconds", IntegerType(), True)
                                     ])

# COMMAND ----------

pit_stops_df = spark.read \
.schema(pit_stops_schema) \
.option("multiLine", True) \
.json("/mnt/raw/pit_stops.json")

# COMMAND ----------

pit_stops_df = spark.read \
.option("multiLine", True) \
.json("/mnt/raw/pit_stops.json")

# COMMAND ----------

display(pit_stops_df)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

final_df = pit_stops_df.withColumnRenamed("driverId", "driver_id") \
.withColumnRenamed("raceId", "race_id") \
.withColumn("ingestion_date", current_timestamp())

# COMMAND ----------

final_df.write.mode("overwrite").parquet("/mnt/processed/pit_stops")

# COMMAND ----------


