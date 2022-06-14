-- Databricks notebook source
-- MAGIC %md-sandbox
-- MAGIC # Databricks Unity Catalog - Table ACL
-- MAGIC 
-- MAGIC <img src="https://github.com/QuentinAmbard/databricks-demo/raw/main/product_demos/uc/us-base-0.png" style="float: right" width="500px"/> 
-- MAGIC 
-- MAGIC The main feature of Unity Catalog is to provide you an easy way to setup Table ACL (Access Control Level), but also build Dynamic Views based on each individual permission.
-- MAGIC 
-- MAGIC Typically, Analysts will only have access to customers from their country and won't be able to read GDPR/Sensitive informations (like email, firstname etc.)
-- MAGIC 
-- MAGIC A typical workflow in the Lakehouse architecture is the following:
-- MAGIC 
-- MAGIC * Data Engineers / Jobs can read and update the main data/schemas (ETL part)
-- MAGIC * Data Scientists can read the final tables and update their features tables
-- MAGIC * Data Analyst have READ access to the Data Engineering and Feature Tables and can ingest/transform additional data in a separate schema.
-- MAGIC * Data is masked/anonymized dynamically based on each user access level
-- MAGIC 
-- MAGIC With Unity Catalog, your tables, users and groups are defined at the account level, cross workspaces. Ideal to deploy and operate a Lakehouse Platform across all your teams.
-- MAGIC 
-- MAGIC Let's see how this can be done with the Unity Catalog
-- MAGIC 
-- MAGIC <!-- tracking, please do not remove -->
-- MAGIC <img width="1px" src="https://www.google-analytics.com/collect?v=1&gtm=GTM-NKQ8TT7&tid=UA-163989034-1&cid=555&aip=1&t=event&ec=field_demos&ea=display&dp=%2F42_field_demos%2Ffeatures%2Fuc%2Ftable_acl%2Facl&dt=FEATURE_UC_TABLE_ACL">

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC ## 3-level namespace
-- MAGIC 
-- MAGIC <img src="https://github.com/QuentinAmbard/databricks-demo/raw/main/product_demos/uc/uc-base-1.png" style="float: right" width="800px"/> 
-- MAGIC 
-- MAGIC Unity Catalog works with 3 layers:
-- MAGIC 
-- MAGIC * CATALOG
-- MAGIC * SCHEMA (or DATABASE)
-- MAGIC * TABLE
-- MAGIC 
-- MAGIC To access one table, you can specify the full path: ```SELECT * FROM <CATALOG>.<SCHEMA>.<TABLE>```
-- MAGIC 
-- MAGIC Note that the tables created before Unity Catalog are saved under the catalog named `hive_metastore`. Unity Catalog features are not available for this catalog.
-- MAGIC 
-- MAGIC Note that Unity Catalog comes in addition to your existing data, no hard change required!

-- COMMAND ----------

-- DBTITLE 1,Loading sample data to UC
df2=spark.read.format('csv')\
.option('inferSchema', 'true')\
.option('header', 'true')\
.option('sep', ',')\
.load('/databricks-datasets/Rdatasets/data-001/csv/ggplot2/diamonds.csv')

display(df2)

-- COMMAND ----------

-- DBTITLE 1,Writes don't require file paths!
df2.write\
.mode('overwrite')\
.saveAsTable('main.default.diamonds')

-- COMMAND ----------

-- MAGIC %md ## Granting user access
-- MAGIC 
-- MAGIC Let's now use Unity Catalog to GRANT permission on the table.
-- MAGIC 
-- MAGIC Unity catalog let you GRANT standard SQL permission to your objects, using the Unit Catalog users or group:

-- COMMAND ----------

SHOW GRANTS ON TABLE main.default.diamonds

-- COMMAND ----------

-- Let's grant SELECT to Group A
GRANT SELECT ON TABLE main.default.diamonds TO `Group A`;

-- We'll grant an extra MODIFY to our Data Engineer
GRANT SELECT, MODIFY ON TABLE main.default.diamonds TO `andrew.lucius@databricks.com`;

-- COMMAND ----------

SHOW GRANTS ON TABLE main.default.diamonds
