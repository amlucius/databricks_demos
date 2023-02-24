-- Databricks notebook source
-- MAGIC %md-sandbox
-- MAGIC # Securing access to External Tables / Files with Unity Catalog
-- MAGIC 
-- MAGIC <img src="https://github.com/QuentinAmbard/databricks-demo/raw/main/product_demos/uc/external/uc-external-location-global.png" style="float:right; margin-left:10px" width="600"/>
-- MAGIC 
-- MAGIC By default, Unity Catalog will create managed tables in your primary storage, providing a secured table access for all your users.
-- MAGIC 
-- MAGIC In addition to these managed tables, you can manage access to External tables and files, located in another cloud storage (S3/ADLS/GCS). 
-- MAGIC 
-- MAGIC This give you capabilities to ensure a full data governance, storing your main tables in the managed catalog/storage while ensuring secure access for for specific cloud storage.
-- MAGIC 
-- MAGIC <!-- tracking, please do not remove -->
-- MAGIC <img width="1px" src="https://www.google-analytics.com/collect?v=1&gtm=GTM-NKQ8TT7&tid=UA-163989034-1&cid=555&aip=1&t=event&ec=field_demos&ea=display&dp=%2F42_field_demos%2Ffeatures%2Fuc%2Fexternal_location%2Faws&dt=FEATURE_UC_EXTERNAL_LOC_AZURE">

-- COMMAND ----------

-- DBTITLE 1,Wine dataset with Instance Profile
-- MAGIC %python
-- MAGIC from pyspark.sql import functions as F
-- MAGIC 
-- MAGIC df=spark.read.format('csv')\
-- MAGIC .option('inferSchema', 'true')\
-- MAGIC .option('header', 'true')\
-- MAGIC .option('sep', ';')\
-- MAGIC .load('/databricks-datasets/wine-quality')
-- MAGIC 
-- MAGIC df=df\
-- MAGIC .select([F.col(col).alias(col.replace(' ', '_')) for col in df.columns])\
-- MAGIC .filter("pH is NOT NULL")
-- MAGIC 
-- MAGIC display(df)

-- COMMAND ----------

-- DBTITLE 1,Write out to S3 and create Hive metastore reference
-- MAGIC %python
-- MAGIC catalog='hive_metastore'
-- MAGIC schema='default'
-- MAGIC table='wine'
-- MAGIC path='s3://alucius-sandbox-group-b/wine'
-- MAGIC 
-- MAGIC   
-- MAGIC df.write\
-- MAGIC .format("delta")\
-- MAGIC .mode("overwrite")\
-- MAGIC .save(path)
-- MAGIC  
-- MAGIC spark.sql("CREATE DATABASE IF NOT EXISTS {ct}.{db}".format(ct=catalog, db=schema))
-- MAGIC  
-- MAGIC spark.sql("""
-- MAGIC  create table if not exists {ct}.{db}.{tbl} 
-- MAGIC  using delta
-- MAGIC  location '{path}'""".format(ct=catalog, db=schema, tbl=table, path=path))

-- COMMAND ----------

-- MAGIC %python
-- MAGIC files=dbutils.fs.ls('s3://alucius-sandbox-group-b/wine')
-- MAGIC display(files)

-- COMMAND ----------

-- DBTITLE 1,Query from Legacy Hive Metastore
select * from hive_metastore.default.wine

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC ## Cluster setup for UC
-- MAGIC 
-- MAGIC <img src="https://github.com/QuentinAmbard/databricks-demo/raw/main/product_demos/uc/uc-cluster-setup-single-user.png" style="float: right"/>
-- MAGIC 
-- MAGIC 
-- MAGIC To be able to run this part of the demo, make sure you create a cluster with the security mode enabled.
-- MAGIC 
-- MAGIC Go in the compute page, create a new cluster.
-- MAGIC 
-- MAGIC Select "Single User" and your UC-user (the user needs to exist at the workspace and the account level)

-- COMMAND ----------

-- DBTITLE 1,Upgrade to Unity Catalog as External Table
CREATE TABLE main.default.wine
LOCATION 's3://alucius-sandbox-group-b/wine'
WITH (CREDENTIAL `alucius-sandbox-groupb`);

-- COMMAND ----------

select * from main.default.wine

-- COMMAND ----------

-- MAGIC %md-sandbox ## Where did the credential come from?
-- MAGIC 
-- MAGIC <img src="https://github.com/QuentinAmbard/databricks-demo/raw/main/product_demos/uc/external/uc-external-location-1.png" style="float:right; margin-left:10px" width="700px"/>
-- MAGIC 
-- MAGIC The first step is to create the `STORAGE CREDENTIAL`.
-- MAGIC 
-- MAGIC To do that, we'll use Databricks Unity Catalog UI:
-- MAGIC 
-- MAGIC 1. Open the Data Explorer in DBSQL
-- MAGIC 1. Select the "Storage Credential" menu
-- MAGIC 1. Click on "Create Credential"
-- MAGIC 1. Fill your credential information: the name and IAM role you will be using
-- MAGIC 
-- MAGIC Because you need to be ADMIN, this step has been created for you.
-- MAGIC 
-- MAGIC 
-- MAGIC <img src="https://github.com/QuentinAmbard/databricks-demo/raw/main/product_demos/uc/external/uc-external-location-cred.png" width="400"/>

-- COMMAND ----------

show storage credentials

-- COMMAND ----------

describe storage credential `alucius-sandbox-groupb`

-- COMMAND ----------

show external locations

-- COMMAND ----------

describe external location groupb

-- COMMAND ----------

LIST 's3://alucius-sandbox-group-b' WITH (CREDENTIAL `alucius-sandbox-groupb`)

-- COMMAND ----------

-- DBTITLE 1,Apply UC ACLs to external location
GRANT READ FILES, WRITE FILES ON EXTERNAL LOCATION groupb TO `Group A`;

-- COMMAND ----------

show grants on external location groupb

-- COMMAND ----------

-- DBTITLE 1,Write data in the external location from UC cluster
-- MAGIC %python
-- MAGIC df = spark.createDataFrame([("UC", "is awesome"), ("Delta Sharing", "is magic")])
-- MAGIC 
-- MAGIC df.write\
-- MAGIC .mode('overwrite')\
-- MAGIC .format('csv')\
-- MAGIC .save('s3://alucius-sandbox-group-b/test_write')

-- COMMAND ----------

-- DBTITLE 1,Read the external data
-- MAGIC %python
-- MAGIC spark.read.csv('s3://alucius-sandbox-group-b/test_write').display()

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## In contrast, UC Managed Tables are simple

-- COMMAND ----------

-- DBTITLE 1,Diamonds dataset with UC cluster
df2=spark.read.format('csv')\
.option('inferSchema', 'true')\
.option('header', 'true')\
.option('sep', ',')\
.load('/databricks-datasets/Rdatasets/data-001/csv/ggplot2/diamonds.csv')

display(df2)

-- COMMAND ----------

-- DBTITLE 1,Look Mom, no file path!
df2.write\
.mode('overwrite')\
.saveAsTable('main.default.diamonds')

-- COMMAND ----------

show grants on table main.default.wine

-- COMMAND ----------

revoke select, modify on table main.default.wine from `Group A`
