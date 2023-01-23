# Databricks notebook source
# MAGIC %md 
# MAGIC ## Demo bundle configuration
# MAGIC Please ignore / do not delete, only used to prep and bundle the demo

# COMMAND ----------

{
  "name": "uc-03-data-lineage",
  "category": "governance",
  "title": "Data Lineage with Unity Catalog",
  "description": "Discover data lineage with Unity Catalog: table to table and column to column",
  "bundle": True,
  "tags": [{"uc": "Unity Catalog"}],
  "notebooks": [
    {
      "path": "_resources/00-setup",
      "pre_run": False,
      "publish_on_website": False,
      "add_cluster_setup_cell": False,
      "title":  "Setup",
      "description": "Init data for demo."
    },
    {
      "path": "00-UC-lineage", 
      "pre_run": False, 
      "publish_on_website": True, 
      "add_cluster_setup_cell": True, 
      "title":  "UC data lineage", 
      "description": "Data lineage from Table to Table & Column 2 column."
    }
  ],
  "cluster": {
      "spark_conf": {
        "spark.master": "local[*]",
        "spark.databricks.cluster.profile": "singleNode",
        "spark.databricks.dataLineage.enabled": "true"
    },
    "custom_tags": {
        "ResourceClass": "SingleNode"
    },
    "num_workers": 0,
    "single_user_name": "{{CURRENT_USER}}",
    "data_security_mode": "SINGLE_USER"
  }
}
