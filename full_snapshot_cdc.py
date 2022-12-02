# Databricks notebook source
import os

source_data_path = f"file:{os.getcwd()}/data/dlt_example.parquet"
tmp_location = "dbfs:/tmp/full_snapshot_cdc/"
table_name = "computed_cdc"
schema = 'adrian_tompkins_snapshot_cdc'

# COMMAND ----------

spark.sql(f"create schema if not exists {schema}")
spark.sql(f"use schema {schema}")

# COMMAND ----------

# here is some simulated data we will be using
# our source_df is the inital full snapshot that has been unloaded
source_df = spark.read.parquet(source_data_path)
display(source_df)

# COMMAND ----------

# Define a function that can compute CDC operations from full snapshot.
# This function relies on a single primary key and is not tolerant to a changing source schema.
# The mechanism to compute differences is to perform a full outer join on the primary key
#   between the previous snapshot and the new incoming snapshot.
# There may be a more intelligent way to do this by computing the latest snapshot from the target table
#   rather than keeping a copy of the previous snapshot

# WARNING I have not thoroughly tested this code; please don't put into production without rigorous testing
# Additonally, because this solution requires mantaining two tables, if the _fullsnapshot table fails to write
#   after the target table has been updated, then the next run may produce incorrect results

from delta.tables import DeltaTable
import pyspark.sql.functions as F
from pyspark.sql.dataframe import DataFrame

def generate_full_snapshot_cdc(snapshot: DataFrame, primary_key: str, target_table_name: str):
  fullsnapshot_table_name = f"_fullsnapshot_{target_table_name}"
  if not spark.catalog.tableExists(target_table_name):
    snapshot.withColumn("_op", F.lit("INSERT")).withColumn("_seq", F.lit(0)).write.format("delta").saveAsTable(target_table_name)
    snapshot.write.mode("overwrite").saveAsTable(fullsnapshot_table_name)
  else:
    target_table = DeltaTable.forName(spark, target_table_name)
    fullsnapshot_table = DeltaTable.forName(spark, fullsnapshot_table_name)
    # we use a hash to compare row values
    table_df = fullsnapshot_table.toDF().withColumn("_hash", F.hash(*snapshot.columns))
    # use the table version as the column to sequence by
    # the assumtion here is that each snapshot that is processed by this function is the latest one
    latest_version = target_table.history().agg(F.max("version")).collect()[0][0]
    hashed_snapshot = snapshot.withColumn("_hash", F.hash(*snapshot.columns))
    compare = (hashed_snapshot
      .join(table_df, table_df[primary_key] == snapshot[primary_key], "fullouter")
    )
    changes = (compare
      .select(
        F.col("*"), 
        F.when(
          hashed_snapshot[primary_key].isNotNull() & 
            table_df[primary_key].isNotNull() & 
            (hashed_snapshot["_hash"] != table_df["_hash"]), 
          "UPDATE" # perform an update when there's a match in source & destination, but the rows are different
        )
          .when(
            hashed_snapshot[primary_key].isNotNull() & table_df[primary_key].isNull(),
            "INSERT" # perform an insert when there's no match in the target 
          )
          .when(
            hashed_snapshot[primary_key].isNull() & table_df[primary_key].isNotNull(),
            "DELETE" # perform a delete when there's no match in the source
          )
          .otherwise(F.lit(None)) # otherwise there's be no update so there's nothing to do
          .alias("_newop"),
        F.when(hashed_snapshot[primary_key].isNotNull(), hashed_snapshot[primary_key]).otherwise(table_df[primary_key]).alias("_pk")
      )
      .select(*[hashed_snapshot[col] for col in hashed_snapshot.columns], "_pk", "_newop")
      .drop(primary_key, "_hash")
      .withColumnRenamed("_pk", primary_key)
      .withColumnRenamed("_newop", "_op")
      .withColumn("_seq", F.lit(latest_version + 1))
      .where(F.col("_op").isNotNull()) # drop all rows where we didn't have to do anything
    )
    changes.write.format("delta").mode("append").saveAsTable(target_table_name)
    snapshot.write.mode("overwrite").saveAsTable(fullsnapshot_table_name)

# COMMAND ----------

generate_full_snapshot_cdc(source_df, "Id", table_name)

# COMMAND ----------

display(spark.table(table_name))

# COMMAND ----------

source_df.createOrReplaceTempView("source_data")
next_snapshot_1 = spark.sql("""
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
next_snapshot_1.createOrReplaceTempView("next_snapshot_1")
display(next_snapshot_1)

# COMMAND ----------

generate_full_snapshot_cdc(next_snapshot_1, "Id", table_name)

# COMMAND ----------

display(spark.table(table_name))

# COMMAND ----------

next_snapshot_2 = spark.sql("""
    select
      Id,
      FirstName,
      LastName,
      City,
      case when Balance = 20 then 40 else Balance end as Balance -- simulate another update 
    from next_snapshot_1
    where id != 6 -- simplate another delete
    union select 3, 'Zack', 'Runofsdottir', 'Perth', -99 -- re-insert deleted row
  """
)
display(next_snapshot_2)

# COMMAND ----------

generate_full_snapshot_cdc(next_snapshot_2, "Id", table_name)

# COMMAND ----------

display(spark.table(table_name))

# COMMAND ----------

# Clean up
# spark.sql(f"drop schema {schema} cascade")
