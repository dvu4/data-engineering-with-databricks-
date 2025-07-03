# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning">
# MAGIC </div>
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC # Creating Anonymized User's Age table
# MAGIC
# MAGIC In this lesson we'll create a anonymized key for storing potentially sensitive user data.  
# MAGIC
# MAGIC Our approach in this notebook is fairly straightforward; some industries may require more elaborate de-identification to guarantee privacy.
# MAGIC
# MAGIC We'll examine design patterns for ensuring PII is stored securely and updated accurately. 
# MAGIC
# MAGIC ##### Objectives
# MAGIC - Describe the purpose of "salting" before hashing
# MAGIC - Apply salted hashing to sensitive data(user_id)
# MAGIC - Apply tokenization to sensitive data(user_id)

# COMMAND ----------

# MAGIC %md
# MAGIC ### A. DAG
# MAGIC
# MAGIC ![Anonymization DAG](../Includes/images/piidata_security_anon_dag.png)
# MAGIC

# COMMAND ----------

import dlt
import pyspark.sql.functions as F

# Get the source path for daily user events from Spark configuration
daily_user_events_source = spark.conf.get("daily_user_events_source")

# Get the catalog name for lookup tables from Spark configuration
lookup_catalog = spark.conf.get("lookup_catalog")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## B. Set up Event User Tables
# MAGIC
# MAGIC - The **date_lookup** table is used for the **date** and **week_part** association is used to join with the **users_events_raw** data to identify in what **week_part** does the **Date of Birth(DOB)** belongs. _eg: 2020-07-02 = 2020-27_
# MAGIC - The **user_events_raw** represents the ingested user event data in JSON, which is later unpacked and filtered to retrieve only user information.
# MAGIC - users_bronze: is our focus and will be our source for the ingested user information, where we'll apply **Binning Anonymization** to the **Date of Birth (dob)**.

# COMMAND ----------

@dlt.table
def date_lookup():
    # Read the raw date lookup table from the specified catalog
    return (spark
            .read
            .table(f"{lookup_catalog}.pii_data.date_lookup_raw")
            .select("date", "week_part")
        )


@dlt.table(
    partition_cols=["topic", "week_part"],
    table_properties={"quality": "bronze"}
)
def user_events_raw():
    # Read the streaming user events data from the specified source
    return (
      spark.readStream
        .format("cloudFiles")
        .schema("key BINARY, value BINARY, topic STRING, partition LONG, offset LONG, timestamp LONG")
        .option("cloudFiles.format", "json")
        .load(f"{daily_user_events_source}")
        .join(
          # Join with the date lookup table to get the week part
          F.broadcast(dlt.read("date_lookup")),  # Broadcasts distributes the lookup table to all executors
          F.to_date((F.col("timestamp")/1000).cast("timestamp")) == F.col("date"), "left") 
    )

        
users_schema = "user_id LONG, update_type STRING, timestamp FLOAT, dob STRING, sex STRING, gender STRING, first_name STRING, last_name STRING, address STRUCT<street_address: STRING, city: STRING, state: STRING, zip: INT>"    

@dlt.table(
    table_properties={"quality": "bronze"}
)
def users_bronze():
    # Read the raw user events stream and filter for user info updates
    return (
        dlt.read_stream("user_events_raw") # Reads from user_events_raw
          .filter("topic = 'user_info'") # Filters topic with user_info
          .select(F.from_json(F.col("value").cast("string"), users_schema).alias("v")) # Unpacks the JSON
          .select("v.*") # Select all fields
          .select(
              # Select and transform the necessary columns
              F.col("user_id"),
              F.col("timestamp").cast("timestamp").alias("updated"),
              F.to_date("dob", "MM/dd/yyyy").alias("dob"),
              "sex", 
              "gender", 
              "first_name", 
              "last_name", 
              "address.*", 
              "update_type"
            )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## C. Setup Binning by Age

# COMMAND ----------

# MAGIC %md
# MAGIC ### C.1 Function "age_bins"
# MAGIC
# MAGIC The function `age_bins` takes a date of birth column (**dob_col**) as input.  It calculates the age by finding the difference in months between the current date and the date of birth, then converting it to years.
# MAGIC
# MAGIC It categorizes the age into bins (e.g., "under 18", "18-25", etc.) using a series of conditional statements.
# MAGIC The resulting age category is returned as a new column named "age".

# COMMAND ----------

def age_bins(dob_col):
    age_col = F.floor(F.months_between(F.current_date(), dob_col) / 12).alias("age")
    return (
        F.when((age_col < 18), "under 18")
        .when((age_col >= 18) & (age_col < 25), "18-25")
        .when((age_col >= 25) & (age_col < 35), "25-35")
        .when((age_col >= 35) & (age_col < 45), "35-45")
        .when((age_col >= 45) & (age_col < 55), "45-55")
        .when((age_col >= 55) & (age_col < 65), "55-65")
        .when((age_col >= 65) & (age_col < 75), "65-75")
        .when((age_col >= 75) & (age_col < 85), "75-85")
        .when((age_col >= 85) & (age_col < 95), "85-95")
        .when((age_col >= 95), "95+")
        .otherwise("invalid age")
        .alias("age")
    )

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### C.2 DLT Table "user_age_bins"
# MAGIC
# MAGIC It reads data from a source table named **users_bronze**.
# MAGIC
# MAGIC It selects specific columns: **user_id**, the age category (using the age_bins function on the dob column), gender, city, and state.

# COMMAND ----------

@dlt.table
def user_age_bins():
    return (
        dlt.read("users_bronze")
        .select("user_id", age_bins(F.col("dob")), "gender", "city", "state")
    )

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC &copy; 2025 Databricks, Inc. All rights reserved. Apache, Apache Spark, Spark, the Spark Logo, Apache Iceberg, Iceberg, and the Apache Iceberg logo are trademarks of the <a href="https://www.apache.org/" target="blank">Apache Software Foundation</a>.<br/>
# MAGIC <br/><a href="https://databricks.com/privacy-policy" target="blank">Privacy Policy</a> | 
# MAGIC <a href="https://databricks.com/terms-of-use" target="blank">Terms of Use</a> | 
# MAGIC <a href="https://help.databricks.com/" target="blank">Support</a>
# MAGIC