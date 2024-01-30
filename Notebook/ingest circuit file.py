# Databricks notebook source
# MAGIC %md 
# MAGIC ### Read the csv

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------

# MAGIC %fs 
# MAGIC ls /mnt/processed

# COMMAND ----------

from pyspark.sql.types import StructField, StructType, IntegerType, StringType, DoubleType

# COMMAND ----------

circuits_schema = StructType(fields=[StructField("circuitId", IntegerType(), False),
                                      StructField("circuitref", StringType(), False),
                                      StructField("name", StringType(), False),
                                      StructField("location", StringType(), False),
                                      StructField("country", StringType(), False),
                                      StructField("lat", DoubleType(), False),
                                      StructField("lng", DoubleType(), False),
                                      StructField("alt", IntegerType(), False),
                                      StructField("url", StringType(), False),
])

# COMMAND ----------

circuits_df = spark.read \
    .option("header", True) \
    .schema(circuits_schema) \
    .csv("dbfs:/mnt/raw/circuits.csv")

# COMMAND ----------

display(circuits_df)

# COMMAND ----------

circuits_df.show()

# COMMAND ----------

circuits_df.printSchema()

# COMMAND ----------

circuits_df.describe().show()

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

circuits_selected_df = circuits_df.select("circuitId", "circuitRef", "name", "location", "country", "lat", "lng", "alt")

# COMMAND ----------

circuits_renamed_df = circuits_selected_df.withColumnRenamed("circuitId", "circuit_id") \
    .withColumnRenamed("circuitRef", "circuit_ref") \
    .withColumnRenamed("lat", "latitude") \
    .withColumnRenamed("lng", "longitude") \
    .withColumnRenamed("alt", "altitude")

# COMMAND ----------

display(circuits_renamed_df)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp
circuits_final_df = circuits_renamed_df.withColumn("ingestion_date", current_timestamp())

# COMMAND ----------

display(circuits_final_df)

# COMMAND ----------

# MAGIC %md 
# MAGIC ###### Write data to our dataframe

# COMMAND ----------

circuits_final_df.write.mode("overwrite").parquet("/mnt/processed/circuits")

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/processed/circuits

# COMMAND ----------

df = spark.read.parquet("/mnt/processed/circuits")
display(df)

# COMMAND ----------


