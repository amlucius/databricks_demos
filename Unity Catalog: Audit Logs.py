# Databricks notebook source
# MAGIC %md 
# MAGIC # Enabling audit logs
# MAGIC 
# MAGIC Audit logging isn't enabled by default and requires a few API calls to be initialized.
# MAGIC 
# MAGIC Please read the [Official documentation](https://docs.databricks.com/administration-guide/account-settings/audit-logs.html) for more details. You can also set them up via [Terraform](https://registry.terraform.io/providers/databricks/databricks/latest/docs/resources/mws_log_delivery).
# MAGIC 
# MAGIC <!-- tracking, please do not remove -->
# MAGIC <img width="1px" src="https://www.google-analytics.com/collect?v=1&gtm=GTM-NKQ8TT7&tid=UA-163989034-1&cid=555&aip=1&t=event&ec=field_demos&ea=display&dp=%2F42_field_demos%2Ffeatures%2Fuc%2Faudit_log%2Fsetup&dt=FEATURE_UC_AUDIT">

# COMMAND ----------

# MAGIC %md
# MAGIC # Audit logs for Governance, Traceability, and Compliance
# MAGIC 
# MAGIC Unity catalog collect logs on all your users actions. It's easy to get this data and monitor your data usage, including for your own compliance requirements.
# MAGIC 
# MAGIC Here are a couple of examples:
# MAGIC 
# MAGIC * How your data is used internally, what are the most critical tables
# MAGIC * Who created, updated or deleted Delta Shares
# MAGIC * Who are your most active users
# MAGIC * Who accessed which tables
# MAGIC * Audit all kinds of data access patterns...
# MAGIC 
# MAGIC <!-- tracking, please do not remove -->
# MAGIC <img width="1px" src="https://www.google-analytics.com/collect?v=1&gtm=GTM-NKQ8TT7&tid=UA-163989034-1&cid=555&aip=1&t=event&ec=field_demos&ea=display&dp=%2F42_field_demos%2Ffeatures%2Fuc%2Faudit_log%2Fingestion&dt=FEATURE_UC_AUDIT">

# COMMAND ----------

# DBTITLE 1,See contents of audit log bucket
display(dbutils.fs.ls("s3://alucius-sandbox-logs/audit-logs"))

# COMMAND ----------

# DBTITLE 1,Unity Catalog is account-level, so workspace 0
df=spark.read.json("s3://alucius-sandbox-logs/audit-logs/workspaceId=0/*")
display(df)

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import StringType, MapType

df1=df\
.withColumn("email", col("userIdentity.email")) \
.drop("userIdentity") \
.withColumn("date_time", from_utc_timestamp(from_unixtime(col("timestamp")/1000), "UTC"))\
.filter(col("serviceName") == "unityCatalog")\
.select("requestParams.*", "*")

display(df1)

# COMMAND ----------

df2=df1.select("email", "catalog_name", "changes", "actionName", "securable_full_name", "auditLevel")
display(df2)

# COMMAND ----------

df2=df\
.withColumn("email", col("userIdentity.email")) \
.drop("userIdentity") \
.withColumn("date_time", from_utc_timestamp(from_unixtime(col("timestamp")/1000), "UTC"))\
.filter((df.serviceName == "accounts") & (df.actionName  == "tokenLogin"))\
.select("requestParams.*", "*")

display(df2)

# COMMAND ----------

df2=df\
.withColumn("email", col("userIdentity.email")) \
.drop("userIdentity") \
.withColumn("date_time", from_utc_timestamp(from_unixtime(col("timestamp")/1000), "UTC"))\
.filter(df.actionName  == "tokenLogin")(df.serviceName == "accounts")\
.select("requestParams.*", "*")
display(df2)

# COMMAND ----------


