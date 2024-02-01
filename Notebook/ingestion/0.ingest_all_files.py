# Databricks notebook source
v_result = dbutils.notebook.run("data ingestion race", 0, {"p_data_source": "Ergast API"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result = dbutils.notebook.run("ingest circuit file", 0, {"p_data_source": "Ergast API"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result = dbutils.notebook.run("ingest drives.json file", 0, {"p_data_source": "Ergast API"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result = dbutils.notebook.run("ingestion multiple files", 0, {"p_data_source": "Ergast API"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result = dbutils.notebook.run("ingestion qualifying file", 0, {"p_data_source": "Ergast API"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result = dbutils.notebook.run("pit_stops_file", 0, {"p_data_source": "Ergast API"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result = dbutils.notebook.run("results.json file", 0, {"p_data_source": "Ergast API"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result = dbutils.notebook.run("test", 0, {"p_data_source": "Ergast API"})

# COMMAND ----------

v_result
