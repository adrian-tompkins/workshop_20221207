# Databricks notebook source
# MAGIC %md 
# MAGIC ## Demo bundle configuration
# MAGIC Please ignore / do not delete, only used to prep and bundle the demo

# COMMAND ----------

{
  "name": "uc-04-audit-log",
  "category": "governance",
  "title": "Audit-log with Databricks",
  "description": "Track and analysis usage with UC Audit-log.",
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
      "path": "00-auditlog-activation", 
      "pre_run": False, 
      "publish_on_website": True, 
      "add_cluster_setup_cell": True, 
      "title":  "Enable audit log", 
      "description": "Use APIs to create enable Audit Log (run only once for setup)."
    },
    {
      "path": "01-AWS-Audit-log-ingestion", 
      "pre_run": True,
      "publish_on_website": True, 
      "add_cluster_setup_cell": True, 
      "title":  "Audit log ingestion", 
      "description": "Create an ingestion pipeline to ingest and analyse your logs."
    },
    {
      "path": "02-log-analysis-query", 
      "pre_run": True, 
      "publish_on_website": True, 
      "add_cluster_setup_cell": True, 
      "title":  "Log Analysis example", 
      "description": "SQL queries example to analyze your logs."
    }
  ],
  "cluster": {
    "num_workers": 4,
    "single_user_name": "{{CURRENT_USER}}",
    "data_security_mode": "SINGLE_USER"
  }
}
