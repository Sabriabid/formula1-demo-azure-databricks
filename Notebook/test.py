# Databricks notebook source
# MAGIC %md
# MAGIC Ingest constructors.json file

# COMMAND ----------

constructors_schema = "constructorId INT, constructorREF STRING, name STRING, nationality STRING, url STRING"

# COMMAND ----------

constructor_df = spark.read \
    .schema(constructors_schema) \
    .json("/Workspace/Repos/Demo/formula1-demo-azure-databricks/data/raw/raw/constructors.json")

# COMMAND ----------

dbutils.fs.ls("/Repos/Demo/formula1-demo-azure-databricks/data/raw")

# COMMAND ----------


