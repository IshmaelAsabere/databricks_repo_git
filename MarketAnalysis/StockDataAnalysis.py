# Databricks notebook source
from pyspark.sql.functions import max
import plotly.express as px

# COMMAND ----------

# Import data
df = spark.read.format("delta")\
          .load("dbfs:/user/hive/warehouse/tesla_stock_price")

display(df)

# COMMAND ----------

# COMMAND to perform a very simple operation on the data ----------
df.select(max("Adj_Close"), max("Volume"))\
  .withColumnRenamed("max(Adj_Close)", "Max_Close")\
  .withColumnRenamed("max(Volume)", "Max_Volume")\
  .show(truncate=False)

# COMMAND ----------


