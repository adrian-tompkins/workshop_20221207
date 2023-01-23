-- Databricks notebook source
-- create the gold table
CREATE STREAMING LIVE TABLE scd_type_2;

-- store all changes as SCD2
APPLY CHANGES INTO live.scd_type_2
FROM stream(adrian_tompkins_snapshot_cdc.computed_cdc)
  KEYS (Id)
  APPLY AS DELETE WHEN _op = 'DELETE'
  SEQUENCE BY _seq
  STORED AS SCD TYPE 2;
