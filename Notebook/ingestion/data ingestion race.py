# Databricks notebook source
from pyspark.sql.types import StructField, StructType, IntegerType, StringType, DoubleType, DateType

# COMMAND ----------

races_schema = StructType(fields=[StructField("raceId", IntegerType(), False),
                                 StructField("year", IntegerType(), True),
                                 StructField("round", IntegerType(), True),
                                 StructField("circuitId", IntegerType(), True),
                                 StructField("name", StringType(), True),
                                 StructField("date", DateType(), True),
                                 StructField("time", StringType(), True),
                                 StructField("url", StringType(), True)
])

# COMMAND ----------

races_df = spark.read \
    .option("header", True) \
    .schema(races_schema) \
    .csv("/mnt/raw/races.csv")


# COMMAND ----------

display(races_df)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, to_timestamp, concat, col, lit

# COMMAND ----------

races_with_timestamp_df = races_df.withColumn("ingestion_date", current_timestamp()) \
                                  .withColumn("race_timestamp", to_timestamp(concat(col('date'), lit(' '), col('time')), 'yyyy-MM-dd HH:mm:ss'))

# COMMAND ----------

races_selected_df = races_with_timestamp_df.select(col('raceId').alias('race_id'), col('year').alias('race_year'), col('round'), col('circuitId').alias('circuit_id'), col('name'), col('ingestion_date'), col('race_timestamp'))

# COMMAND ----------

# MAGIC %fs 
# MAGIC
# MAGIC ls mnt/testformula1/processed/races

# COMMAND ----------

races_selected_df.write.mode('overwrite').partitionBy('race_year').parquet('/mnt/testformula1/processed/races')

# COMMAND ----------

display(spark.read.parquet("/mnt/processed/races"))

# COMMAND ----------


