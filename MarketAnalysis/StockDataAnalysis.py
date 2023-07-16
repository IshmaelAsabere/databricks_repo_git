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

# Code to filter high volume days and write to file
df.select("Date", "Adj_Close", "Volume")\
  .where(df.Volume > 150000000)\
  .write.option("header","true")\
        .mode('overwrite')\
        .csv("dbfs:/FileStore/outputs/tesla_highvoldays.csv")

