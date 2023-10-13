# Databricks notebook source
# DBTITLE 1,Using Boto3 and Volumes to reach external services on the Shared Cluster
# MAGIC %md
# MAGIC 1) Admin uses Lambda function (or similar) to call role with dynamodb privileges <br>
# MAGIC 2) Generates STS token <br>
# MAGIC 3) Writes to ini file and stores in S3
# MAGIC 4) Creates UC volume external location pointing to ini file <br>
# MAGIC 5) DB user grabs via UC Volume path and replaces shared credential file <br>
# MAGIC 6) DB user calls boto3 and reaches out to AWS service <br>

# COMMAND ----------

# DBTITLE 1,Create external volume pointing to .ini file with AWS Keys
# MAGIC %sql
# MAGIC create external volume if not exists main.default.al135_ini
# MAGIC location 's3://databricks-e2demofieldengwest/al135/keys/'

# COMMAND ----------

# DBTITLE 1,List files to verify
# MAGIC %python
# MAGIC dbutils.fs.ls("/Volumes/main/default/al135_ini")

# COMMAND ----------

# DBTITLE 1,Point AWS Shared Credential File to volume
import os

os.environ['AWS_SHARED_CREDENTIALS_FILE'] = '/Volumes/main/default/al135_ini/keys.ini'

# COMMAND ----------

# DBTITLE 1,Create boto3 client for user
import boto3

al135_dynamo = boto3.client(
    "dynamodb",
    region_name="us-west-2",
)

# COMMAND ----------

# DBTITLE 1,Call AWS service with client
response = al135_dynamo.list_tables()
print(response)

# COMMAND ----------

# MAGIC %md
# MAGIC [Link to user 2 notebook for concurrency test](https://e2-demo-field-eng.cloud.databricks.com/?o=1444828305810485#notebook/4180492550893027/command/4180492550893028)
