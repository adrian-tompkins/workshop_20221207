# Databricks notebook source
# MAGIC %pip install git+https://github.com/intel/project-example-for-python

# COMMAND ----------

import dlt
from project_example_for_python.importable import SomeClass
import pyspark.sql.functions as F 

# COMMAND ----------

@dlt.table()
def taxi_pickup():
  return (spark.read.json("dbfs:/databricks-datasets/nyctaxi/sample/json/pep_pickup_date_txt=2020-03-29/")
  .withColumn("function_call", F.lit(SomeClass().hello()))
  )
