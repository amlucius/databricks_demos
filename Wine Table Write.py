# Databricks notebook source
from pyspark.sql import functions as F

df=spark.read.format('csv')\
.option('inferSchema', 'true')\
.option('header', 'true')\
.option('sep', ';')\
.load('/databricks-datasets/wine-quality')

df=df\
.select([F.col(col).alias(col.replace(' ', '_')) for col in df.columns])\
.filter("pH is NOT NULL")

display(df)

# COMMAND ----------

df.write\
.mode('overwrite')\
.saveAsTable('main.default.wine')

# COMMAND ----------

df.write\
.mode('overwrite')\
.saveAsTable('main.default.wine_ext', path='s3://alucius-sandbox-group-b/wine_ext')
