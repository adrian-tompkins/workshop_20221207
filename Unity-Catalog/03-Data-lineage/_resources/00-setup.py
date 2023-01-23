# Databricks notebook source
import pyspark.sql.functions as F

import re
current_user = dbutils.notebook.entry_point.getDbutils().notebook().getContext().tags().apply('user')
if current_user.rfind('@') > 0:
  current_user_no_at = current_user[:current_user.rfind('@')]
else:
  current_user_no_at = current_user
current_user_no_at = re.sub(r'\W+', '_', current_user_no_at)

catalog = "uc_demos_"+current_user_no_at

catalog_exists = False
for r in spark.sql("SHOW CATALOGS").collect():
    if r['catalog'] == catalog:
        catalog_exists = True

#As non-admin users don't have permission by default, let's do that only if the catalog doesn't exist (an admin need to run it first)     
if not catalog_exists:
    spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog}")
    spark.sql(f"GRANT CREATE, USAGE on CATALOG {catalog} TO `account users`")
spark.sql(f"USE CATALOG {catalog}")


db_not_exist = len([db for db in spark.catalog.listDatabases() if db.name == 'uc_lineage']) == 0
if db_not_exist:
  print("creating lineage database")
  spark.sql(f"CREATE DATABASE IF NOT EXISTS {catalog}.uc_lineage ")
  spark.sql(f"GRANT CREATE, USAGE on DATABASE {catalog}.uc_lineage TO `account users`")


# COMMAND ----------

spark.sql(f"CREATE TABLE IF NOT EXISTS {catalog}.uc_lineage.dinner ( recipe_id INT, full_menu STRING)")
spark.sql(f"CREATE TABLE IF NOT EXISTS {catalog}.uc_lineage.dinner_price ( recipe_id INT, full_menu STRING, price DOUBLE)")
spark.sql(f"CREATE TABLE IF NOT EXISTS {catalog}.uc_lineage.menu ( recipe_id INT, app STRING, main STRING, desert STRING)")
spark.sql(f"CREATE TABLE IF NOT EXISTS {catalog}.uc_lineage.price ( recipe_id BIGINT, price DOUBLE)")

# COMMAND ----------

if db_not_exist:
  spark.sql(f"GRANT MODIFY, SELECT ON TABLE {catalog}.uc_lineage.dinner TO `account users`")
  spark.sql(f"GRANT MODIFY, SELECT ON TABLE {catalog}.uc_lineage.dinner_price TO `account users`")
  spark.sql(f"GRANT MODIFY, SELECT ON TABLE {catalog}.uc_lineage.menu TO `account users`")
  spark.sql(f"GRANT MODIFY, SELECT ON TABLE {catalog}.uc_lineage.menu_dinner TO `account users`")
  spark.sql(f"GRANT MODIFY, SELECT ON TABLE {catalog}.uc_lineage.price TO `account users`")
