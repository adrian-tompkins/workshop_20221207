# Databricks notebook source
import os
source_data_path = f"file:{os.getcwd()}/data/dlt_example.parquet"
tmp_location = "dbfs:/tmp/full_snapshot_cdc/"
source_df = spark.read.parquet(source_data)

# COMMAND ----------

display(source_df)

# COMMAND ----------

from delta.tables import DeltaTable
import pyspark.sql.functions as F
from pyspark.sql.dataframe import DataFrame
def generate_full_snapshot_cdc(snapshot: DataFrame, primary_key: str, delta_table_path: str):
  if not DeltaTable.isDeltaTable(spark, delta_table_path):
    snapshot.withColumn("_op", F.lit("INSERT")).withColumn("_seq", F.lit(0)).write.format("delta").save(delta_table_path)
  else:
    table = DeltaTable.forPath(spark, delta_table_path)
    table_df = table.toDF().withColumn("_hash", F.hash(*snapshot.columns))
    latest_version = DeltaTable.forPath(spark, f"{tmp_location}/delta_table").history().agg(F.max("version")).collect()[0][0]
    snapshot = snapshot.withColumn("_hash", F.hash(*snapshot.columns))
    compare = (snapshot
      .join(table_df, table_df[primary_key] == snapshot[primary_key], "fullouter")
    )
    changes = (compare
      .select(
        F.col("*"), 
        F.when(
          snapshot[primary_key].isNotNull() & 
            table_df[primary_key].isNotNull() & 
            (snapshot["_hash"] != table_df["_hash"]), 
          "UPDATE"
        )
          .when(
            snapshot[primary_key].isNotNull() & table_df[primary_key].isNull(),
            "INSERT"
          )
          .when(
            snapshot[primary_key].isNull() & table_df[primary_key].isNotNull(),
            "DELETE"
          )
          .otherwise(F.lit(None))
          .alias("_newop"),
        F.when(snapshot[primary_key].isNotNull(), snapshot[primary_key]).otherwise(table_df[primary_key]).alias("_pk")
      )
      .select(*[snapshot[col] for col in snapshot.columns], "_pk", "_newop")
      .drop(primary_key, "_hash")
      .withColumnRenamed("_pk", primary_key)
      .withColumnRenamed("_newop", "_op")
      .withColumn("_seq", F.lit(latest_version + 1))
      .where(F.col("_op").isNotNull())
    )
    changes.write.format("delta").mode("append").save(delta_table_path)

# COMMAND ----------

generate_full_snapshot_cdc(source_df, "Id", f"{tmp_location}/delta_table")

# COMMAND ----------

display(spark.read.format("delta").load(f"{tmp_location}/delta_table"))

# COMMAND ----------

source_df.createOrReplaceTempView("source_data")
next_source_df = spark.sql("""
    select
      Id,
      FirstName,
      LastName,
      City,
      case when Balance = 30 then 20 else Balance end as Balance -- simulate an update 
    from source_data
    where id != 3 -- simulate a delete
    union select 11, 'Rod', 'Thompson', 'Perth', 88 -- simulate an insert
  """
)
display(next_source_df)

# COMMAND ----------

generate_full_snapshot_cdc(next_source_df, "Id", f"{tmp_location}/delta_table")

# COMMAND ----------

display(spark.read.format("delta").load(f"{tmp_location}/delta_table"))

# COMMAND ----------

dbutils.fs.rm(tmp_location, True)
