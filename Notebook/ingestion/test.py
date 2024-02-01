# Databricks notebook source


# COMMAND ----------

dbutils.fs.ls("/Repos/Demo/formula1-demo-azure-databricks/data/raw")

# COMMAND ----------

dbutils.fs.mount(
  source="wasbs://raw@testformula1.blob.core.windows.net",
  mount_point="/mnt/raw",
  extra_configs={"fs.azure.account.key.testformula1.blob.core.windows.net":"Wdbc2b3mh+uCQrvo5/0qPEtLEujEd2glXgRukn/zvJi/tNl1oXCY7n8Hi7FXvEFEaselcserybFj+AStwFOAjg=="})


# COMMAND ----------

display(dbutils.fs.ls("/mnt/raw"))


# COMMAND ----------

constructors_schema = "constructorId INT, constructorREF STRING, name STRING, nationality STRING, url STRING"

# COMMAND ----------

constructor_df = spark.read \
    .schema(constructors_schema) \
    .json("/mnt/raw/constructors.json")

# COMMAND ----------

display(constructor_df)

# COMMAND ----------

constructor_dropped_df = constructor_df.drop('url')

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

construcor_final_df = constructor_dropped_df.withColumnRenamed("constructorId", "constructor_id") \
                                            .withColumnRenamed("constructorREF", "constructor_ref") \
                                            .withColumn("ingestion_date", current_timestamp())

# COMMAND ----------

display(construcor_final_df)

# COMMAND ----------

display(construcor_final_df)

# COMMAND ----------

construcor_final_df.write.mode("overwrite").parquet("/mnt/raw/processed/constructors")

# COMMAND ----------

# MAGIC %fs 
# MAGIC ls /mnt/raw/processed/constructors

# COMMAND ----------


