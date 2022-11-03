-- Databricks notebook source
drop table main.default.wine

-- COMMAND ----------

sync table main.default.wine
from
  hive_metastore.default.wine dry run
