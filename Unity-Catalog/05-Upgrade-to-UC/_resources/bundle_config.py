# Databricks notebook source
# MAGIC %md 
# MAGIC ## Demo bundle configuration
# MAGIC Please ignore / do not delete, only used to prep and bundle the demo

# COMMAND ----------

{
  "name": "uc-05-upgrade",
  "category": "governance",
  "title": "Upgrade table to Unity Catalog",
  "description": "Discover how to upgrade your hive_metastore tables to Unity Catalog to benefit from UC capabilities: Security/ACL/Row-level/Lineage/Audit...",
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
      "path": "00-Upgrade-database-to-UC", 
      "pre_run": True, 
      "publish_on_website": True, 
      "add_cluster_setup_cell": True, 
      "title":  "Upgrade database to UC", 
      "description": "Migration example, from one table to multiple databases."
    }
  ],
  "cluster": {
      "spark_conf": {
        "spark.master": "local[*]",
        "spark.databricks.cluster.profile": "singleNode"
    },
    "custom_tags": {
        "ResourceClass": "SingleNode"
    },
    "num_workers": 0,
    "single_user_name": "{{CURRENT_USER}}",
    "data_security_mode": "SINGLE_USER"
  }
}
