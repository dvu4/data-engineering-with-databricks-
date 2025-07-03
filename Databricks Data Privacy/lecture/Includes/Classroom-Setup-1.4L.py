# Databricks notebook source
# MAGIC %run ./Classroom-Setup-Common

# COMMAND ----------

DA = DBAcademyHelper()
DA.init()

## Create new schema for lab
schema_name = 'cdf_lab'

# spark.sql(f'DROP SCHEMA IF EXISTS {DA.catalog_name}.{schema_name} CASCADE')
spark.sql(f'CREATE SCHEMA IF NOT EXISTS {DA.catalog_name}.{schema_name}')
spark.sql(f'CREATE VOLUME IF NOT EXISTS {DA.catalog_name}.{schema_name}.pii')

## Set default schema to cdf_lab
r = spark.sql(f'USE SCHEMA {schema_name}')



## Drop and recreate the tables for lab
DA.create_users_bin_table(schema_name=schema_name)
DA.create_user_lookup_table(schema_name=schema_name)
DA.create_users_table(schema_name=schema_name)


## Display user values
DA.display_config_values([
        ('Your Course Catalog Name',DA.catalog_name), 
        ('Your Schema',schema_name)
      ]
)