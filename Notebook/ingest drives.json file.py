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

drivers_df.printSchema()

# COMMAND ----------

display(drivers_df)

# COMMAND ----------

from pyspark.sql.functions import col, concat, current_timestamp, lit

# COMMAND ----------

drivers_with_columns_df = drivers_df.withColumnRenamed("driverId", "driver_id") \
                                    .withColumnRenamed("driverRef", "driver_ref") \
                                    .withColumn("ingestion_date", current_timestamp()) \
                                    .withColumn("name", concat(col("name.forename"), lit(" "), concat("name.surname")))

# COMMAND ----------

display(drivers_with_columns_df)

# COMMAND ----------

drivers_final_df = drivers_with_columns_df.drop("url")

# COMMAND ----------

drivers_final_df.write.mode("overwrite").parquet("/mnt/raw/processed/drivers")

# COMMAND ----------

display(spark.read.parquet("/mnt/raw/processed/drivers"))

# COMMAND ----------


