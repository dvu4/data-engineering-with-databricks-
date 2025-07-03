# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning">
# MAGIC </div>
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC # Creating a Pseudonymized PII Lookup Tables
# MAGIC
# MAGIC In this lesson we'll create a pseudonymized key for storing potentially sensitive user data.  
# MAGIC Our approach in this notebook is fairly straightforward; some industries may require more elaborate de-identification to guarantee privacy.
# MAGIC
# MAGIC We'll examine design patterns for ensuring PII is stored securely and updated accurately. 
# MAGIC
# MAGIC ##### Objectives
# MAGIC - Describe the purpose of "salting" before hashing
# MAGIC - Apply salted hashing to sensitive data(user_id)
# MAGIC - Apply tokenization to sensitive data(user_id)
# MAGIC
# MAGIC ##### Creates the Following
# MAGIC   1. **registered_users** table from the source JSON files with PII.
# MAGIC
# MAGIC   1. Hashing: Handled in table **user_lookup_hashed**
# MAGIC
# MAGIC   1. Tokenization: Handled in tables **registered_users_tokens** and **user_lookup_tokenized**

# COMMAND ----------

# MAGIC %md
# MAGIC ### A. DAG
# MAGIC
# MAGIC
# MAGIC ![Pseudonymization DAG](../Includes/images/pii_data_security_pseudo_dag.png)

# COMMAND ----------

import dlt
import pyspark.sql.functions as F

user_reg_source = spark.conf.get("user_reg_source")

# COMMAND ----------

# MAGIC %md
# MAGIC ## B. Registered Users
# MAGIC
# MAGIC This is our source data to apply

# COMMAND ----------

# Ingest data into the registered_users table incrementally with Auto Loader
@dlt.table
def registered_users():
    return (
        spark.readStream
            .format("cloudFiles")
            .schema("device_id LONG, mac_address STRING, registration_timestamp DOUBLE, user_id LONG")
            .option("cloudFiles.format", "json")
            .load(f"{user_reg_source}")
        )

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## C. Create pseudonymized user lookup table
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### C1. with salted hashing
# MAGIC
# MAGIC Create a function to register this logic to the current database under the name **`salted_hash`**. This will allow this logic to be called by any user with appropriate permissions on this function. 
# MAGIC
# MAGIC Note that it is theoretically possible to link the original key and pseudo-ID if the hash function and the salt are known. Here, we use this method to add a layer of obfuscation; in production, you may wish to have a much more sophisticated hashing method.

# COMMAND ----------

salt = "BEANS"
     
# Define function to pseudonymize with salted hashing    
def salted_hash(id):
    return F.sha2(F.concat(id, F.lit(salt)), 256)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC The logic below creates the **user_lookup** table. In the next notebook, we'll use this pseudo-ID as the sole link to user PII. By controlling access to the link between our **alt_id** and other natural keys, we'll be able to prevent linking PII to other user data throughout our system.
# MAGIC
# MAGIC Use the function above to create the **alt_id** to the **user_id** from the **registered_users** table. Make sure to include all necessary columns for the target **user_lookup** table.

# COMMAND ----------

# Create pseudonymized user lookup table
# Method: Hashing
@dlt.table
def user_lookup_hashed():
    return (dlt
            .read_stream("registered_users")
            .select(
                  salted_hash(F.col("user_id")).alias("alt_id"),
                  "device_id", "mac_address", "user_id")
           )

# COMMAND ----------

# MAGIC %md
# MAGIC ### C2. with Tokenization
# MAGIC
# MAGIC Lets first create a table that will hold the tokens for our users in **registered_token** table

# COMMAND ----------

@dlt.table
def registered_users_tokens():
    return (dlt
            .readStream("registered_users")
            .select("user_id")
            .distinct()
            .withColumn("token", F.expr("uuid()"))
        )

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Now lets create the table **user_lookup_tokenized** using the **registered_users_tokens** and have a join to include the new tokenized column as **alt_id**.
# MAGIC

# COMMAND ----------

# Create pseudonymized user lookup table
# Method: Tokenization
@dlt.table
def user_lookup_tokenized():
    return (dlt
            .read_stream("registered_users")
            .join(dlt.read("registered_users_tokens"), "user_id", "left")
            .drop("user_id")
            .withColumnRenamed("token", "alt_id")
           )

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC &copy; 2025 Databricks, Inc. All rights reserved. Apache, Apache Spark, Spark, the Spark Logo, Apache Iceberg, Iceberg, and the Apache Iceberg logo are trademarks of the <a href="https://www.apache.org/" target="blank">Apache Software Foundation</a>.<br/>
# MAGIC <br/><a href="https://databricks.com/privacy-policy" target="blank">Privacy Policy</a> | 
# MAGIC <a href="https://databricks.com/terms-of-use" target="blank">Terms of Use</a> | 
# MAGIC <a href="https://help.databricks.com/" target="blank">Support</a>
# MAGIC