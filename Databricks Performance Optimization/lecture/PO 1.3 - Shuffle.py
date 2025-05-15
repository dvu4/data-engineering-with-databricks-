# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning">
# MAGIC </div>
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC # Shuffle
# MAGIC
# MAGIC Shuffle is a Spark mechanism that redistributes data so that it's grouped differently across partitions. This typically involves copying data across executors and machines and, while it's sometimes necessary, it can be a complex and costly operation.
# MAGIC
# MAGIC In this demo we will see shuffle in action. Run the next cell to set up the lesson.

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

# MAGIC %run ./Includes/Classroom-Setup-3

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
# MAGIC ## C. Creating Tables
# MAGIC Create the tables the following tables for the demonstration:
# MAGIC - **transactions**
# MAGIC - **stores**
# MAGIC - **countries**

# COMMAND ----------

# MAGIC %md
# MAGIC ### C1. Create the transactions table
# MAGIC Let's generate the data we will use in this demo. First, we'll synthesize data representing a set of sales transactions and write the data to a table named **transactions**.
# MAGIC
# MAGIC The table will contain *150,000,000* rows.
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import *

## Drop the table if it exists
spark.sql('DROP TABLE IF EXISTS transactions')

## Create the Spark DataFrame and save as a table
(spark
 .range(0, 150000000, 1, 32)
 .select(
    'id',
    round(rand() * 10000, 2).alias('amount'),
    (col('id') % 10).alias('country_id'),
    (col('id') % 100).alias('store_id')
 )
 .write
 .mode('overwrite')
 .saveAsTable('transactions')
)

## Preview the table
display(spark.sql('SELECT * FROM transactions LIMIT 10'))

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### C2. Create the stores table
# MAGIC Now let's synthesize data and write it to a table describing points of sale information named **stores**.
# MAGIC
# MAGIC The table will contain 99 rows.

# COMMAND ----------

## Drop the table if it exists
spark.sql('DROP TABLE IF EXISTS stores')

## Create the Spark DataFrame
(spark
 .range(0, 99)
 .select(
    'id',
    round(rand() * 100, 0).alias('employees'),
    (col('id') % 10).alias('country_id'),
    expr('uuid()').alias('name')
    )
 .write
 .mode('overwrite')
 .saveAsTable('stores')
)

## Display the table
display(spark.sql('SELECT * FROM stores LIMIT 10'))

# COMMAND ----------

# MAGIC %md
# MAGIC ### C3. Create the countries lookup table 
# MAGIC Now, let's create a lookup table that maps **country_id** from the data tables to the actual country name.
# MAGIC
# MAGIC The **countries** table will contain 12 rows.
# MAGIC

# COMMAND ----------

## Drop the table if it exists
spark.sql('DROP TABLE IF EXISTS countries')

## Create the data
countries = [(0, "Italy"),
             (1, "Canada"),
             (2, "Mexico"),
             (3, "China"),
             (4, "Germany"),
             (5, "UK"),
             (6, "Japan"),
             (7, "Korea"),
             (8, "Australia"),
             (9, "France"),
             (10, "Spain"),
             (11, "USA")
            ]

columns = ["id", "name"]

## Create the Spark DataFrame and countries table
countries_df = (spark
                .createDataFrame(data = countries, schema = columns)
                .write
                .mode('overwrite')
                .saveAsTable("countries")
            )


## Display the table
display(spark.sql('SELECT * FROM countries'))

# COMMAND ----------

# MAGIC %md
# MAGIC ##D. Joins
# MAGIC
# MAGIC Now we'll perform a query that induces shuffling by joining three tables together, writing the results to a separate table.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### D1. Disabling Broadcast Joins to Demonstrate Shuffle
# MAGIC
# MAGIC These options automatically determine when a broadcast join should be used based on the size of the smaller DataFrame (or table) in the join.
# MAGIC
# MAGIC - [spark.sql.autoBroadcastJoinThreshold](https://spark.apache.org/docs/3.5.3/sql-performance-tuning.html#other-configuration-options) documentation
# MAGIC
# MAGIC - [spark.databricks.adaptive.autoBroadcastJoinThreshold](https://spark.apache.org/docs/3.5.3/sql-performance-tuning.html#converting-sort-merge-join-to-broadcast-join) documentation

# COMMAND ----------

# MAGIC %md
# MAGIC Run the cell below to view the default values of the broadcast join and the Adaptive Query Execution(AQE) configurations. Notice that:
# MAGIC - The default value for a table with a broadcast join is **10,485,760 bytes**.
# MAGIC - If AQE is enabled, the value for the broadcast join is **31,457,280 bytes**.
# MAGIC - Note that AQE is enabled by default.

# COMMAND ----------

print(f'Default value of autoBroadcastJoinThreshold: {spark.conf.get("spark.sql.autoBroadcastJoinThreshold")}')
print(f'Default value of adaptive.autoBroadcastJoinThreshold: {spark.conf.get("spark.databricks.adaptive.autoBroadcastJoinThreshold")}')
print(f'View if AQE is enabled: {spark.conf.get("spark.sql.adaptive.enabled")}')

# COMMAND ----------

# MAGIC %md
# MAGIC In this cell, we're explicitly turning off broadcast joins in order to demonstrate a shuffle and show you how to investigate and improve the query performance.

# COMMAND ----------

## Disabling the automatic broadcast join entirely. That is, Spark will never broadcast any dataset for joins, regardless of its size.
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)

# Disabling the broadcast join feature under AQE, meaning that even when using adaptive query execution, Spark will not attempt to broadcast any smaller side of a join.
spark.conf.set("spark.databricks.adaptive.autoBroadcastJoinThreshold", -1)


## View the new values
print(f'Current value of autoBroadcastJoinThreshold: {spark.conf.get("spark.sql.autoBroadcastJoinThreshold")}')
print(f'Current value of adaptive.autoBroadcastJoinThreshold: {spark.conf.get("spark.databricks.adaptive.autoBroadcastJoinThreshold")}')

# COMMAND ----------

# MAGIC %md
# MAGIC Perform the join of **transactions**, **stores**, and **countries** to create a table named **transact_countries** without using broadcast joins. Notice that this query takes about ~1 minute to execute.
# MAGIC

# COMMAND ----------

joined_df_no_broadcast = spark.sql("""
    SELECT 
        transactions.id,
        amount,
        countries.name as country_name,
        employees,
        stores.name as store_name
    FROM
        transactions
    LEFT JOIN
        stores
        ON
            transactions.store_id = stores.id
    LEFT JOIN
        countries
        ON
            transactions.country_id = countries.id
""")

## Create a table with the joined data
(joined_df_no_broadcast
 .write
 .mode('overwrite')
 .saveAsTable('transact_countries')
)

# COMMAND ----------

# MAGIC %md
# MAGIC Open the Spark UI to the Stages page and notice that there were two large **1.4GB** shuffles of data. 
# MAGIC
# MAGIC A shuffle refers to the process of redistributing data across different nodes/partitions. This is necessary for operations like joins, where data from two or more datasets needs to be combined based on a key, but the relevant rows might not be on the same partition.
# MAGIC
# MAGIC
# MAGIC To view how the query DAG, complete the following steps:
# MAGIC
# MAGIC 1. In the cell above, expand **Spark Jobs**.
# MAGIC
# MAGIC 2. In the second job, right-click on **View** and select *Open in a New Tab*.
# MAGIC
# MAGIC     **NOTE:** In the Vocareum lab environment, if you click **View** without opening it in a new tab, the pop-up window will display an error.
# MAGIC
# MAGIC 3. Click on the **Stages** link in the top navigation bar. Notice that in the stages of this specific query, there were very large **Shuffle Read** and **Shuffle Write** operations for the query.
# MAGIC
# MAGIC     ![1.3-non_broadcast_join_stages.png](./Includes/images/1.3-non_broadcast_join_stages.png)
# MAGIC
# MAGIC 4. Next, in the **Completed Stages** table select the link (**mapPartitionsInternal at...**) in the **Description** column for the row that has the large **Shuffle Read** and **Shuffle Write** the **Jobs** link in the top navigation bar. For example, in the image above it would be row **151**.
# MAGIC
# MAGIC 5. Then, select number link in the table at the top in **Associated Jobs Ids** -> then the query number in **Associated SQL Query**. Here, you should see the entire query plan (DAG).
# MAGIC
# MAGIC     ![1.3-non_broadcastjoin_dag.png](./Includes/images/1.3-non_broadcastjoin_dag.png)
# MAGIC
# MAGIC <br></br>
# MAGIC 6. In the query plan, complete the following steps:
# MAGIC <br></br>
# MAGIC ##### 6a. Shuffle Join with transactions and stores
# MAGIC
# MAGIC ![1.3-shuffle_join_transactions_stores.png](./Includes/images/1.3-shuffle_join_transactions_stores.png)
# MAGIC
# MAGIC - Find **PhotonScan parquet dbacademy.your-schema-name.transactions (1)**.
# MAGIC   
# MAGIC - Above that table, expand **PhotonShuffleExchangeSink (2)**.
# MAGIC
# MAGIC - Find the metric **num bytes written total (min, med, max)**. Notice that the shuffle uses around 1.4GB for the first join with the **stores** table.
# MAGIC <br></br>
# MAGIC ##### 6b. Shuffle Join with transactions and stores
# MAGIC
# MAGIC ![1.3-shuffle_join_results_countries.png](./Includes/images/1.3-shuffle_join_results_countries.png)
# MAGIC
# MAGIC - Find **PhotonShuffleExchangeSource** on the left side above the **AQEShuffleRead** for the result of the previous join.
# MAGIC
# MAGIC - Expand **PhotonShuffleExchangeSource**.
# MAGIC
# MAGIC - Find the metric **num bytes written total (min, med, max)**. Notice that another shuffle was performed with around 1.4GB for the join of the results of the first join to the join with the **countries** table.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ###D2. Enabling Broadcast Join
# MAGIC
# MAGIC Broadcast join avoids the shuffle. In the above cells, we explicitly turned off broadcast joins, but now we'll return the configuration to the default so that broadcast join is enabled to compare the queries. 
# MAGIC
# MAGIC This will work in this case because at least one of the tables in each join is relatively small and below the thresholds:
# MAGIC - < 10MB for a broadcast join without AQE
# MAGIC - < 30MB for a broadcast join with AQE
# MAGIC

# COMMAND ----------

# Set the default configurations for broadcast joins
spark.conf.unset("spark.sql.autoBroadcastJoinThreshold")
spark.conf.unset("spark.databricks.adaptive.autoBroadcastJoinThreshold")

## View the default values
print(f'Current value of autoBroadcastJoinThreshold: {spark.conf.get("spark.sql.autoBroadcastJoinThreshold")}')
print(f'Current value of adaptive.autoBroadcastJoinThreshold: {spark.conf.get("spark.databricks.adaptive.autoBroadcastJoinThreshold")}')

# COMMAND ----------

# MAGIC %md
# MAGIC Run the query to perform the same join as before. Notice that this query executes in about half the time of the previous join.

# COMMAND ----------

joined_df = spark.sql("""
    SELECT 
        transactions.id,
        amount,
        countries.name as country_name,
        employees,
        stores.name as store_name
    FROM
        transactions
    LEFT JOIN
        stores
        ON
            transactions.store_id = stores.id
    LEFT JOIN
        countries
        ON
            transactions.country_id = countries.id
""")

(joined_df
 .write
 .mode('overwrite')
 .saveAsTable('transact_countries')
)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC This is an improvement. Referring back to the Spark UI **Stages** tab, note that there are no large shuffle reads anymore. Only the small tables were shuffled, but the large table was not, so we avoided moving 1.4GB twice.
# MAGIC
# MAGIC
# MAGIC View the stages of this query. Notice that the large shuffles have been avoided, improving the query efficiency.
# MAGIC ![1.3-broadcast_join_stages.png](./Includes/images/1.3-broadcast_join_stages.png)
# MAGIC
# MAGIC <br></br>
# MAGIC You can also view the query plan and see that the large tables were not shuffled.
# MAGIC
# MAGIC ![1.3-broadcast_join.png](./Includes/images/1.3-broadcast_join.png)
# MAGIC
# MAGIC <br></br>
# MAGIC Broadcast joins can be significantly faster than shuffle joins if one of the tables is very large and the other is small. Unfortunately, broadcast joins only work if at least one of the tables is less than 100MB in size. In the case of joining larger tables, if we want to avoid the shuffle, we may need to reconsider our schema to avoid having to do the join in the first place.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## E. Aggregations
# MAGIC
# MAGIC Aggregations also use a shuffle, but they're often much less expensive. The following cell executes a query that demonstrates this.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC   country_id, 
# MAGIC   COUNT(*) AS count,
# MAGIC   AVG(amount) AS avg_amount
# MAGIC FROM transactions
# MAGIC GROUP BY country_id
# MAGIC ORDER BY count DESC

# COMMAND ----------

# MAGIC %md
# MAGIC That was fast! There are a lot of things going on here. One of the main points is that we're only shuffling the counts and sums necessary to compute the counts and averages that were requested. This only results in shuffling a few KB. Use the Spark UI once again to verify this.
# MAGIC
# MAGIC ![1.3_aggregations-ui.png](./Includes/images/1.3-aggregations_ui.png)
# MAGIC
# MAGIC So the shuffle is cheap compared to the shuffle joins, which need to shuffle all of the data. It also helps that our output is basically 0 in this case.

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC &copy; 2025 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the 
# MAGIC <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/><a href="https://databricks.com/privacy-policy">Privacy Policy</a> | 
# MAGIC <a href="https://databricks.com/terms-of-use">Terms of Use</a> | 
# MAGIC <a href="https://help.databricks.com/">Support</a>

# COMMAND ----------

