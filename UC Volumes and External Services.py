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

df=spark.read.format('csv')\
.option('inferSchema', 'true')\
.option('header', 'true')\
.option('sep', ',')\
.load('/databricks-datasets/Rdatasets/data-001/csv/ggplot2/diamonds.csv')

df.write\
.mode('overwrite')\
.saveAsTable('main.default.diamonds')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from main.default.diamonds

# COMMAND ----------

# DBTITLE 1,Create external volume pointing to .ini file with AWS Keys
# MAGIC %sql
# MAGIC create external volume if not exists main.default.lucius_dynamodb_credentials
# MAGIC location 's3://alucius-sandbox-group-b/ddb/lucius'

# COMMAND ----------

# DBTITLE 1,List files to verify
# MAGIC %python
# MAGIC dbutils.fs.ls("/Volumes/main/default/lucius_dynamodb_credentials")

# COMMAND ----------

# DBTITLE 1,Point AWS Shared Credential File to volume
import os

os.environ['AWS_SHARED_CREDENTIALS_FILE'] = '/Volumes/main/default/lucius_dynamodb_credentials/dynamodb_credentials.ini'

# COMMAND ----------

# DBTITLE 1,Create boto3 client for user
import boto3

lucius_dynamo = boto3.client(
    "dynamodb",
    region_name="us-east-1",
)

# COMMAND ----------

# DBTITLE 1,Call AWS service with client
response = lucius_dynamo.list_tables()
print(response)

# COMMAND ----------

table = lucius_dynamo.create_table(
    TableName='lucius',
    KeySchema=[
        {
            'AttributeName': 'id',
            'KeyType': 'HASH'
        }
    ],
    AttributeDefinitions=[
        {
            'AttributeName': 'id',
            'AttributeType': 'S'
        }
    ],
    ProvisionedThroughput={
        'ReadCapacityUnits': 5,
        'WriteCapacityUnits': 5
    }
)

# COMMAND ----------

table = lucius_dynamo.put_item(
    TableName="lucius", 
    Item={
        "id": {"S": "1234567890"}, 
        "name": {"S": "John Doe"},
        "age": {"N": "30"}
    }
)

# COMMAND ----------

response=lucius_dynamo.scan(TableName='lucius')
print(response)
