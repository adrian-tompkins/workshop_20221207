# Databricks notebook source
# MAGIC %md 
# MAGIC ## Demo bundle configuration
# MAGIC Please ignore / do not delete, only used to prep and bundle the demo

# COMMAND ----------

{
  "name": "uc-01-acl",
  "category": "governance",
  "title": "Table ACL & Dynamic Views with UC",
  "description": "Discover to GRANT permission on your table with UC and implement more advanced control such as data masking at row-level, based on each user.",
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
      "path": "00-UC-Table-ACL", 
      "pre_run": True, 
      "publish_on_website": True, 
      "add_cluster_setup_cell": True, 
      "title":  "Table ACL with UC", 
      "description": "Secure table access with GRANT command."
    },
    {
      "path": "01-UC-Dynamic-view", 
      "pre_run": True, 
      "publish_on_website": True, 
      "add_cluster_setup_cell": True, 
      "title":  "Row-level & Data masking ACL", 
      "description": "Create dynamic views with UC for finer access control."
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
