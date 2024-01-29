# Databricks notebook source
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType

# COMMAND ----------

name_schema =  StructType(fields=[StructField("forename", StringType(), True),
                                  StructField("surname", StringType(), True)
])

# COMMAND ----------

driver_schema =  StructType(fields=[StructField("driverId", IntegerType(), True),
                                  StructField("driverREF", StringType(), True),
                                  StructField("number", IntegerType(), True),
                                  StructField("code", IntegerType(), True),
                                  StructField("name", name_schema),
                                  StructField("nationality", StringType(), True),
                                  StructField("url", StringType(), True),
                                  StructField("dob", DateType(), True)
])

# COMMAND ----------

drivers_df = spark.read \
    .schema(driver_schema) \
    .json("/mnt/raw/drivers.json")

# COMMAND ----------


