# Databricks notebook source
# MAGIC %run ./Classroom-Setup-Common

# COMMAND ----------

DA = DBAcademyHelper()
DA.init()


spark.sql(f'CREATE SCHEMA IF NOT EXISTS {DA.catalog_name}.pii_data')
spark.sql(f'USE SCHEMA pii_data')

DA.create_customer_bronze()
DA.create_customer_silver()

DA.display_config_values([('Your Course Catalog Name',DA.catalog_name), ('Your Course Schema','pii_data')])