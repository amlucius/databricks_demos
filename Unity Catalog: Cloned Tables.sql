-- Databricks notebook source
-- DBTITLE 1,Managed table
desc extended main.default.diamonds

-- COMMAND ----------

-- DBTITLE 1,External table
desc extended main.default.wine

-- COMMAND ----------

CREATE TABLE main.default.wine_clone SHALLOW CLONE main.default.wine location 's3://alucius-sandbox-group-b/wine_clone'

-- COMMAND ----------

desc extended main.default.wine_clone

-- COMMAND ----------

CREATE EXTERNAL TABLE main.default.wine_clone_ext SHALLOW CLONE main.default.wine location 's3://alucius-sandbox-group-b/wine_clone_ext'

-- COMMAND ----------

desc extended main.default.wine_clone_ext
