-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Securing data at the row level using Databricks Unity Catalog
-- MAGIC 
-- MAGIC As seen in the previous notebook, Unity Catalog let you grant table ACL using standard SQL GRANT on all the objects (CATALOG, SCHEMA, TABLE)
-- MAGIC 
-- MAGIC But this alone isn't enough. UC let you create more advanced access pattern to dynamically filter your data based on who query it.
-- MAGIC 
-- MAGIC This is usefull to mask sensitive PII information, or restrict access to a subset of data without having to create and maintain multiple tables.
-- MAGIC 
-- MAGIC *Note that Unity Catalog will provide more advanced data masking capabilities in the future, this demo covers what can be done now.*
-- MAGIC 
-- MAGIC See the [documentation](https://docs.databricks.com/security/access-control/table-acls/object-privileges.html#dynamic-view-functions) for more details.
-- MAGIC 
-- MAGIC <!-- tracking, please do not remove -->
-- MAGIC <img width="1px" src="https://www.google-analytics.com/collect?v=1&gtm=GTM-NKQ8TT7&tid=UA-163989034-1&cid=555&aip=1&t=event&ec=field_demos&ea=display&dp=%2F42_field_demos%2Ffeatures%2Fuc%2Ftable_acl%2Fdynamic_view&dt=FEATURE_UC_TABLE_ACL">

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC ## Cluster setup for UC
-- MAGIC 
-- MAGIC <img src="https://github.com/QuentinAmbard/databricks-demo/raw/main/product_demos/uc/uc-cluster-setup-single-user.png" style="float: right"/>
-- MAGIC 
-- MAGIC 
-- MAGIC To be able to run this demo, make sure you create a cluster with the security mode enabled.
-- MAGIC 
-- MAGIC Go in the compute page, create a new cluster.
-- MAGIC 
-- MAGIC Select "Single User" and your UC-user (the user needs to exist at the workspace and the account level)

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ## Dynamic Views: Data Masking
-- MAGIC 
-- MAGIC We'll be using the databricks-datasets diamonds table.
-- MAGIC 
-- MAGIC Suppose we want to mask what users can see based on security groups. 
-- MAGIC 
-- MAGIC ### Using groups
-- MAGIC You can do this with refernce to group in Unity Catalog. In this environment, we have:
-- MAGIC * `Group A`
-- MAGIC * `Group B` 
-- MAGIC 
-- MAGIC You can then add a view with `CASE ... WHEN` statement based on your groups to mask data based on group membership.
-- MAGIC 
-- MAGIC See the [documentation](https://docs.databricks.com/security/access-control/table-acls/object-privileges.html#dynamic-view-functions) for more details on that.

-- COMMAND ----------

SELECT * FROM main.default.diamonds

-- COMMAND ----------

-- DBTITLE 1,Mask price information if not a Group A member
CREATE
OR REPLACE VIEW main.default.diamonds_redacted AS
SELECT
  CASE
    WHEN is_member('Group A') THEN price
    ELSE 'REDACTED'
  END AS price,
  _c0,
  carat,
  cut,
  color,
  clarity,
  depth
FROM
  main.default.diamonds

-- COMMAND ----------

-- DBTITLE 1,Am I a member of Group A?
SELECT is_member('Group A')

-- COMMAND ----------

SELECT * FROM main.default.diamonds_redacted

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ## Dynamic Views: Data Masking with Row-level Permissions
-- MAGIC 
-- MAGIC Dynamic views can also be used to restrict data to a subset based on a field.
-- MAGIC 
-- MAGIC Let's create a table defining our group permissions, including a GDPR permission flag: `analyst_permissions`.
-- MAGIC 
-- MAGIC This table has 3 fields:
-- MAGIC 
-- MAGIC * `group`: to identify the group
-- MAGIC * `country_filter`: we'll filter the dataset based on this value
-- MAGIC * `gdpr_filter`: if true, we'll mask the PII information from the table. If not set the user can see all the information
-- MAGIC 
-- MAGIC Let's create this table and check our current user permissions.

-- COMMAND ----------

-- DBTITLE 1,Group permissions table
CREATE TABLE IF NOT EXISTS main.default.analyst_permissions (
  group STRING,
  country_filter STRING,
  gdpr_filter INT
);
INSERT
  OVERWRITE main.default.analyst_permissions
VALUES
  ('Group A', 'USA', 0),
  ('Group B', 'FRA', 1);
SELECT
  *
from
  main.default.analyst_permissions;

-- COMMAND ----------

-- DBTITLE 1,Which group do I belong to?
SELECT is_member('Group A'), is_member('Group B');

-- COMMAND ----------

-- DBTITLE 1,For this example, I've modified the diamonds table to now include country and gdpr information.
-- MAGIC %python
-- MAGIC df=spark.sql('''
-- MAGIC select
-- MAGIC   *,
-- MAGIC   case
-- MAGIC     when cut = 'Premium' then 'MEX'
-- MAGIC     when cut = 'Ideal' then 'USA'
-- MAGIC     when cut = 'Good' then 'CAN'
-- MAGIC     when cut = 'Fair' then 'GER'
-- MAGIC     when cut = 'Very Good' then 'FRA'
-- MAGIC   end as country,
-- MAGIC   case
-- MAGIC     when cut in ('Fair', 'Very Good') then 1
-- MAGIC     else 0
-- MAGIC   end as gdpr
-- MAGIC from
-- MAGIC   main.default.diamonds
-- MAGIC ''')
-- MAGIC 
-- MAGIC df.write\
-- MAGIC .mode('overwrite')\
-- MAGIC .saveAsTable('main.default.diamonds_masked')

-- COMMAND ----------

SELECT * FROM main.default.diamonds_masked

-- COMMAND ----------

-- DBTITLE 1,Mask cut information for groups with a gdpr flag and limit rows to a group's country
CREATE
OR REPLACE VIEW main.default.diamonds_masked_view AS (
  SELECT
    CASE
      WHEN d.gdpr = 1 THEN sha1(cut)
      ELSE cut
    END AS cut,
    d._c0,
    d.carat,
    d.price,
    d.color,
    clarity,
    d.depth,
    d.country,
    d.gdpr
  from
    main.default.diamonds_masked d
    INNER JOIN main.default.analyst_permissions c ON c.country_filter = d.country
  WHERE
    is_member(c.group)
);

-- COMMAND ----------

SELECT * FROM main.default.diamonds_masked_view

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Let's now change my permission. We'll disable the `gdpr_filter` flag and change our `country_filter` to USA.
-- MAGIC 
-- MAGIC As you can see, requesting the same secured view now returns all the USA customers, and PII information is no longer masked.

-- COMMAND ----------

UPDATE main.default.analyst_permissions SET country_filter='USA', gdpr_filter=0 where group='Group B';

SELECT * FROM main.default.diamonds_masked_view;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Conclusion
-- MAGIC 
-- MAGIC As we've now seen, data masking and filtering can be implemented at a row level using groups, users, and extra tables that you can use to manage more advanced permissions.
-- MAGIC 
-- MAGIC You're now ready to deploy the Lakehouse for your entire organisation, securing data based on your own governance, ensuring proper PII regulation and governance.
