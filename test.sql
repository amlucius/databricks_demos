-- Databricks notebook source
-- MAGIC %python
-- MAGIC spark.conf.set("spark.sql.legacy.allowNonEmptyLocationInCTAS", "TRUE")
-- MAGIC spark.conf.set("spark.sql.parquet.outputTimestampType", "TIMESTAMP_MILLIS")
-- MAGIC spark.conf.set("spark.sql.execution.arrow.enabled", "FALSE")

-- COMMAND ----------

create or replace table main.default.time (id INT, datetime Timestamp);
insert into  main.default.time values(1, '2023-12-31 11:00:00+00');
insert into  main.default.time values(2, '9999-01-01 00:00:00+00')

-- COMMAND ----------

select * from main.default.time

-- COMMAND ----------

drop table hive_metastore.default.time

-- COMMAND ----------

create table hive_metastore.default.time
using parquet
location 's3://alucius-sandbox-group-b/time'
as select * from main.default.time

-- COMMAND ----------

select * from hive_metastore.default.time

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df=spark.sql('select * from hive_metastore.default.time')
-- MAGIC display(df)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df1 = df.select("*").toPandas()
-- MAGIC print(df1)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df = spark.read.parquet('s3://alucius-sandbox-group-b/time')
-- MAGIC 
-- MAGIC df.write\
-- MAGIC .format("parquet")\
-- MAGIC .mode("overwrite")\
-- MAGIC .save('s3://alucius-sandbox-group-b/time_converted')

-- COMMAND ----------

-- MAGIC %python
-- MAGIC spark.conf.get("spark.sql.execution.arrow.enabled")
