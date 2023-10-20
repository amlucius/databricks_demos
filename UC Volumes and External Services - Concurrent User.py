# Databricks notebook source
# MAGIC %sql
# MAGIC create external volume if not exists main.default.braun_dynamodb_credentials
# MAGIC location 's3://alucius-sandbox-group-b/ddb/braun'

# COMMAND ----------

import os

os.environ['AWS_SHARED_CREDENTIALS_FILE'] = '/Volumes/main/default/braun_dynamodb_credentials/dynamodb_credentials.ini'

# COMMAND ----------

import boto3

braun_dynamo = boto3.client(
    "dynamodb",
    region_name="us-east-1",
)

# COMMAND ----------

response=braun_dynamo.scan(TableName='lucius')
print(response)
