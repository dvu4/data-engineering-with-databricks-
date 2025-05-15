# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning">
# MAGIC </div>
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # User-Defined Functions
# MAGIC
# MAGIC Databricks recommends using native functions whenever possible. While UDFs are a great way to extend the functionality of Spark SQL, their use requires transferring data between Python and Spark, which in turn requires serialization. This drastically slows down queries.
# MAGIC
# MAGIC But sometimes UDFs are necessary. They can be an especially powerful tool for ML or NLP use cases, which may not have a native Spark equivalent.
# MAGIC
# MAGIC Run the next cell to set up the lesson.

# COMMAND ----------

# MAGIC %md
# MAGIC ## REQUIRED - SELECT CLASSIC COMPUTE
# MAGIC
# MAGIC Before executing cells in this notebook, please select your classic compute cluster in the lab. Be aware that **Serverless** is enabled by default. If you use Serverless, errors will be returned when setting compute runtime properties.
# MAGIC
# MAGIC Follow these steps to select the classic compute cluster:
# MAGIC
# MAGIC 1. Navigate to the top-right of this notebook and click the drop-down menu to select your cluster. By default, the notebook will use **Serverless**.
# MAGIC
# MAGIC 1. If your cluster is available, select it and continue to the next cell. If the cluster is not shown:
# MAGIC
# MAGIC   - In the drop-down, select **More**.
# MAGIC
# MAGIC   - In the **Attach to an existing compute resource** pop-up, select the first drop-down. You will see a unique cluster name in that drop-down. Please select that cluster.
# MAGIC
# MAGIC **NOTE:** If your cluster has terminated, you might need to restart it in order to select it. To do this:
# MAGIC
# MAGIC 1. Right-click on **Compute** in the left navigation pane and select *Open in new tab*.
# MAGIC
# MAGIC 1. Find the triangle icon to the right of your compute cluster name and click it.
# MAGIC
# MAGIC 1. Wait a few minutes for the cluster to start.
# MAGIC
# MAGIC 1. Once the cluster is running, complete the steps above to select your cluster.

# COMMAND ----------

# MAGIC %md
# MAGIC ## A. Classroom Setup
# MAGIC
# MAGIC Run the following cell to configure your working environment for this course. It will also set your default catalog to your unique **labuser** catalog, and the default schema to **default**. All tables will be read from and written to this location.
# MAGIC <br></br>
# MAGIC
# MAGIC **NOTE:** The `DA` object is only used in Databricks Academy courses and is not available outside of these courses. It will dynamically reference the information needed to run the course.

# COMMAND ----------

# MAGIC %run ./Includes/Classroom-Setup-5

# COMMAND ----------

# MAGIC %md
# MAGIC Let's Check the Current Catalog and Schema

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT current_catalog(), current_schema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## B. Disable Caching
# MAGIC
# MAGIC Run the following cell to set a Spark configuration variable that disables disk caching.
# MAGIC
# MAGIC Turning disk caching off prevents Databricks from storing cloud storage files after the first query. This makes the effect of the optimizations more apparent by ensuring that files are always pulled from cloud storage for each query.
# MAGIC
# MAGIC For more information, see [Optimize performance with caching on Databricks](https://docs.databricks.com/en/optimizations/disk-cache.html#optimize-performance-with-caching-on-databricks).
# MAGIC
# MAGIC **NOTE:** This will not work in Serverless. Please use classic compute to turn off caching. If you're using Serverless, an error will be returned.

# COMMAND ----------

spark.conf.set('spark.databricks.io.cache.enabled', False)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## C. Generate Data
# MAGIC
# MAGIC Let's generate the data we will use in this demo. For this, we'll synthesize telemetry data representing temperature readings. This time, however, we're only going to generate 60 readings and create a table named **device_data**.
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import *

## Drop the table if it exists
spark.sql('DROP TABLE IF EXISTS device_data')


## Create the table
spark.sql('DROP TABLE IF EXISTS device_data')

df = (spark
      .range(0, 60, 1, 1)
      .select(
          'id',
          (col('id') % 1000).alias('device_id'),
          (rand() * 100).alias('temperature_F')
      )
      .write
      .saveAsTable('device_data')
)

## Display the table
display(spark.sql('SELECT * FROM device_data'))

# COMMAND ----------

# MAGIC %md
# MAGIC ## D. Python UDF
# MAGIC Create and use a Python UDF in two ways.
# MAGIC - Computationally Expensive Python UDF
# MAGIC - Parallelization a Python UDF through Repartitioning

# COMMAND ----------

# MAGIC %md
# MAGIC ### D1. Computationally Expensive Python UDF
# MAGIC
# MAGIC For the sake of experimentation, let's implement a function that converts Fahrenheit to Celsius. Notice that we're inserting a one-second sleep to simulate a computationally expensive operation within our UDF. Let's try it.

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *
import time

## Create the Python UDF
@udf("double")
def F_to_Celsius(f):
    # Let's pretend some fancy math takes one second per row
    time.sleep(1)
    return (f - 32) * (5/9)

spark.sql('DROP TABLE IF EXISTS celsius')

## Prep the data
celsius_df = (spark
              .table('device_data')
              .withColumn("celsius", F_to_Celsius(col('temperature_F')))
            )

## Create the table
(celsius_df
 .write
 .mode('overwrite')
 .saveAsTable('celsius')
)

# COMMAND ----------

# MAGIC %md
# MAGIC Run the code to view how many partitions were used for the query. Notice that only **1 partition** was used because the UDF does not utilize the parallel processing capabilities of Spark, which slows down the query.
# MAGIC

# COMMAND ----------

print(f'Total number of cores across all executors in the cluster: {spark.sparkContext.defaultParallelism}')
print(f'The number of partitions in the underlying RDD of a dataframe: {celsius_df.rdd.getNumPartitions()}')

# COMMAND ----------

# MAGIC %md
# MAGIC Explain the Spark execution plan. Notice that the **BatchEvalPython** stage indicates that a Python UDF is being used.
# MAGIC

# COMMAND ----------

celsius_df.explain()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Summary
# MAGIC That took approximately one minute, which is kind of surprising since we have about 60 seconds worth of computation, spread across multiple cores. Shouldn't it take significantly less time? 
# MAGIC
# MAGIC The answer to this question is yes, it should take less time. The problem here is that Spark doesn't know that the computations are expensive, so it hasn't divided the work up into tasks that can be done in parallel. We can see that by watching the one task chug away as the cell is running, and by visiting the Spark UI.

# COMMAND ----------

# MAGIC %md
# MAGIC ### D2. Parallelization a Python UDF through Repartitioning
# MAGIC
# MAGIC Repartitioning is the answer in this case. *We* know that this computation is expensive and should span across all 4 cores, so we can explicitly repartition the DataFrame:
# MAGIC

# COMMAND ----------

# Repartition across the number of cores in your cluster
num_cores = 4

@udf("double")
def F_to_Celsius(f):
    # Let's pretend some fancy math take one second per row
    time.sleep(1)
    return (f - 32) * (5/9)

spark.sql('DROP TABLE IF EXISTS celsius')

celsius_df_cores = (spark.table('device_data')
                    .repartition(num_cores) # <-- HERE
                    .withColumn("celsius", F_to_Celsius(col('temperature_F')))
             )

(celsius_df_cores
 .write
 .mode('overwrite')
 .saveAsTable('celsius')
)

# COMMAND ----------

# MAGIC %md
# MAGIC Run the code to view how many partitions are being used for the query. Notice that 4 partitions (tasks) are being used to execute the code in parallel.
# MAGIC

# COMMAND ----------

print(f'Total number of cores across all executors in the cluster: {spark.sparkContext.defaultParallelism}')
print(f'The number of partitions in the underlying RDD of a dataframe: {celsius_df_cores.rdd.getNumPartitions()}')

# COMMAND ----------

# MAGIC %md
# MAGIC #### Summary
# MAGIC Repartition command general recommend best practice that your UDF runs in parallel distributed manner.

# COMMAND ----------

# MAGIC %md
# MAGIC ##E. SQL UDFs
# MAGIC
# MAGIC The ability to create user-defined functions in Python and Scala is convenient since it allows you to extend functionality in the language of your choice. As far as optimization is concerned, however, it's important to know that SQL is generally the best choice, for a couple of reasons:
# MAGIC - SQL UDFs require less data serialization
# MAGIC - Catalyst optimizer can operate within SQL UDFs
# MAGIC
# MAGIC Let's see this in action now by comparing the performance of a SQL UDF to its Python counterpart.
# MAGIC
# MAGIC First let's redefine the Python UDF from before, this time without the delay, so we can compare raw performance.

# COMMAND ----------

# MAGIC %md
# MAGIC Now let's perform the equivalent operation through a SQL UDF.

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- Create the same function
# MAGIC DROP FUNCTION IF EXISTS farh_to_cels;
# MAGIC
# MAGIC CREATE FUNCTION farh_to_cels (farh DOUBLE)
# MAGIC   RETURNS DOUBLE RETURN ((farh - 32) * 5/9);
# MAGIC
# MAGIC
# MAGIC -- Use the function to create the table
# MAGIC DROP TABLE IF EXISTS celsius_sql;
# MAGIC
# MAGIC CREATE OR REPLACE TABLE celsius_sql AS
# MAGIC SELECT farh_to_cels(temperature_F) as Farh_to_cels_convert 
# MAGIC FROM device_data;
# MAGIC
# MAGIC
# MAGIC -- View the data
# MAGIC SELECT * 
# MAGIC FROM celsius_sql;

# COMMAND ----------

# MAGIC %md
# MAGIC Explain the query plan with the SQL UDF. Notice that the SQL UDF is fully supported by Photon is more performant.

# COMMAND ----------

# MAGIC %sql
# MAGIC EXPLAIN 
# MAGIC SELECT farh_to_cels(temperature_F) as Farh_to_cels_convert 
# MAGIC FROM device_data

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC Actual times depend on a number of factors, however, on average, the SQL UDF will perform better than its Python equivalent — often, significantly better. The reason for this is that SQL UDFs use Spark's built-in APIs and functions, rather than relying on external dependencies or Python UDFs.
# MAGIC
# MAGIC If you are using a UDF in your Spark job, refactoring your code to use native Spark APIs or functions whenever possible will lead to the best performance and efficiency gains.
# MAGIC
# MAGIC If you must use UDFs due to strong dependencies on external libraries, you should parallelize your code and repartition your DataFrame to match the number of CPU cores in your cluster to achieve the best level of parallelization.
# MAGIC
# MAGIC When using Python UDFs, consider using **Apache Arrow**-optimized Python UDFs instead, as they improve the efficiency of data exchange between the Spark runtime and the UDF process. [Learn more about Arrow-optimized Python UDFs](https://www.databricks.com/blog/arrow-optimized-python-udfs-apache-sparktm-35).

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC &copy; 2025 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the 
# MAGIC <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/><a href="https://databricks.com/privacy-policy">Privacy Policy</a> | 
# MAGIC <a href="https://databricks.com/terms-of-use">Terms of Use</a> | 
# MAGIC <a href="https://help.databricks.com/">Support</a>