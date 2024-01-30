# Databricks notebook source
# MAGIC %md 
# MAGIC ### Read the csv

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------

# MAGIC %fs 
# MAGIC ls /mnt/raw

# COMMAND ----------

circuits_df = spark.read.option("header", True).csv("dbfs:/mnt/raw/circuits.csv")

# COMMAND ----------

display(circuits_df)

# COMMAND ----------

circuits_df.show()

# COMMAND ----------


