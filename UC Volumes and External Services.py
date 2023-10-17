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
# MAGIC create external volume if not exists main.default.alucius_dynamodb_credentials
# MAGIC location 's3://alucius-sandbox-group-b/ddb/'

# COMMAND ----------

# DBTITLE 1,List files to verify
# MAGIC %python
# MAGIC dbutils.fs.ls("/Volumes/main/default/alucius_dynamodb_credentials")

# COMMAND ----------

# DBTITLE 1,Point AWS Shared Credential File to volume
import os

os.environ['AWS_SHARED_CREDENTIALS_FILE'] = '/Volumes/main/default/alucius_dynamodb_credentials/dynamodb_credentials.ini'

# COMMAND ----------

# DBTITLE 1,Create boto3 client for user
import boto3

al135_dynamo = boto3.client(
    "dynamodb",
    region_name="us-east-1",
)

# COMMAND ----------

# DBTITLE 1,Call AWS service with client
response = al135_dynamo.list_tables()
print(response)
