# Databricks notebook source
# MAGIC %md 
# MAGIC ## Demo bundle configuration
# MAGIC Please ignore / do not delete, only used to prep and bundle the demo

# COMMAND ----------

{
  "name": "uc-02-external-location",
  "category": "governance",
  "title": "Access data on External Location",
  "description": "Discover how you can secure files/table in external location (cloud storage like S3/ADLS/GCS) with simple GRANT command.",
  "bundle": True,
  "tags": [{"uc": "Unity Catalog"}],
  "notebooks": [
    {
      "path": "AWS-Securing-data-on-external-locations", 
      "pre_run": True, 
      "publish_on_website": True, 
      "add_cluster_setup_cell": True, 
      "title":  "External location on AWS", 
      "description": "Secure file & table access with External location.",
      "parameters": {"external_bucket_url": "s3a://databricks-e2demofieldengwest"}
    },
    {
      "path": "Azure-Securing-data-on-external-locations", 
      "pre_run": False, 
      "publish_on_website": True, 
      "add_cluster_setup_cell": True, 
      "title":  "External location on Azure", 
      "description": "Secure file & table access with External location."
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
