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
# MAGIC # Exploding Join
# MAGIC
# MAGIC In this lab, we will be working on improving query performance on an exploding join between 3 tables:
# MAGIC - **transactions**
# MAGIC - **stores**
# MAGIC - **countries**
# MAGIC
# MAGIC We want to introduce and identify spill introduced by the exploding join and gradually improve the performance of the join.
# MAGIC
# MAGIC #### Lab Notes
# MAGIC To successfully create a exploding join that spills to disk we had to specifically turn off the following optimizations:
# MAGIC - [Predictive optimization](https://docs.databricks.com/aws/en/optimizations/predictive-optimization) for Unity Catalog managed tables in the **lab** schema.
# MAGIC - Broadcast joins to gradually demonstrate the performance improvements of tuning the join strategy.

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
# MAGIC Run the following cell to configure your working environment for this course. It will also set your default catalog to your unique **labuser** catalog, and the default schema to **lab**. All tables will be read from and written to this location.
# MAGIC <br></br>
# MAGIC
# MAGIC **NOTE:** The `DA` object is only used in Databricks Academy courses and is not available outside of these courses. It will dynamically reference the information needed to run the course.

# COMMAND ----------

# MAGIC %run ./Includes/Classroom-Setup-4L

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
# MAGIC ## C. Creating & Storing Data

# COMMAND ----------

# MAGIC %md
# MAGIC ### C1. Create the transactions table
# MAGIC Generate the **transactions** table with the schema below and write it to a Delta table. This is the table with the largest amount of data, containing 2,000,000 rows.
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import *

## Delete the table if it exists
spark.sql('DROP TABLE IF EXISTS transactions')


## Create the table
transactions_df = (spark
                   .range(0, 2000000, 1, 32)
                    .select(
                        'id',
                        round(rand() * 10000, 2).alias('amount'),
                        (col('id') % 10).alias('country_id'),
                        (col('id') % 100).alias('store_id')
                    )
                    .write
                    .mode('overwrite')
                    .option("overwriteSchema", "true")
                    .saveAsTable('transactions')
                )

## Display the table
display(spark.sql('SELECT * FROM transactions LIMIT 10'))

# COMMAND ----------

# MAGIC %md
# MAGIC ### C2. Create the countries table
# MAGIC Generate the **countries** table to join with the **transactions** table later.

# COMMAND ----------

## Drop the table if it exists
spark.sql('DROP TABLE IF EXISTS countries')

## Create the table
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
# MAGIC
# MAGIC ### C3. Create the stores table
# MAGIC - Generate the **stores** table to join with the **transactions** table later on.
# MAGIC - The **stores** table intentionally contains duplicates of the **id** value, which will introduce an exploding join with the **transactions** table.
# MAGIC - When joining the **transactions** table with the **stores** table, a number of rows will explode due to the duplicated **ids**.
# MAGIC

# COMMAND ----------

## Drop the table if it exists
spark.sql('DROP TABLE IF EXISTS stores')


stores_df = (spark
                .range(0, 9999)
                .select(
                    (col('id') % 100).alias('id'), # intentionally duplicating ids to explode the join
                    round(rand() * 100, 0).alias('employees'),
                    (col('id') % 10).alias('country_id'),
                    expr('uuid()').alias('name')
                )
                .write
                .mode('overwrite')
                .saveAsTable('stores')
            )


## Display the table
display(spark.sql('SELECT * FROM stores ORDER BY id'))

# COMMAND ----------

# MAGIC %md
# MAGIC ## D. Perform an Exploding Join

# COMMAND ----------

# MAGIC %md
# MAGIC In this cell, turn off broadcast joins to gradually demonstrate the performance improvements of tuning the join strategy.
# MAGIC
# MAGIC
# MAGIC - [spark.sql.autoBroadcastJoinThreshold](https://spark.apache.org/docs/3.5.3/sql-performance-tuning.html#other-configuration-options) documentation
# MAGIC
# MAGIC - [spark.databricks.adaptive.autoBroadcastJoinThreshold](https://spark.apache.org/docs/3.5.3/sql-performance-tuning.html#converting-sort-merge-join-to-broadcast-join) documentation

# COMMAND ----------

## Disabling the automatic broadcast join entirely. That is, Spark will never broadcast any dataset for joins, regardless of its size.
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)

# Disabling the broadcast join feature under AQE, meaning that even when using adaptive query execution, Spark will not attempt to broadcast any smaller side of a join.
spark.conf.set("spark.databricks.adaptive.autoBroadcastJoinThreshold", -1)

# COMMAND ----------

# MAGIC %md
# MAGIC We will be joining the **transactions** table with the **countries** and **stores** data, and trigger the action by saving the result to a table named **transact_countries**. The query has been written for you below.
# MAGIC
# MAGIC Run the cell, note down the time taken to execute the query, and compare it with each optimization.
# MAGIC
# MAGIC **NOTE:** This should take about ~1 minute.
# MAGIC

# COMMAND ----------

joined_df_nobroadcast = spark.sql("""
    SELECT 
        transactions.id,
        amount,
        countries.name as country_name,
        employees,
        stores.name as store_name
    FROM
        transactions
    JOIN
        stores
        ON
            transactions.store_id = stores.id
    JOIN
        countries
        ON
            transactions.country_id = countries.id
""")

(joined_df_nobroadcast
 .write
 .mode('overwrite')
 .saveAsTable('transact_countries')
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### D1. TODO: See the Exploding Join
# MAGIC Open the Spark UI and navigate to the **Stages** page. Identify the explosion of rows in the DAG of the Spark UI. To view the DAG, complete the following:
# MAGIC
# MAGIC 1. In the cell above, expand **Spark Jobs**.
# MAGIC
# MAGIC 2. In the first job , right-click on **View** and select *Open in a New Tab*. 
# MAGIC     - **If the first job does not display the correct information, try the second one**.
# MAGIC
# MAGIC     **NOTES:** In the Vocareum lab environment, if you click **View** without opening it in a new tab, the pop-up window will display an error.
# MAGIC
# MAGIC 3. In the new window, find the **Associated SQL Query** header at the top and select the number.
# MAGIC
# MAGIC 4. Here, you should see the entire query plan. Read the DAG graph from bottom to top to better understand the details of the execution plan.
# MAGIC
# MAGIC <br></br>
# MAGIC ![1.4-d1-exploding_join_shuffle_dag.png](./Includes/images/1.4-d1-exploding_join_shuffle_dag.png)
# MAGIC
# MAGIC 5. In the query plan, below the number of rows (199,980,000), expand the **PhotonShuffleExchangeSink** box (the arrow in the image above shows you what to expand). Notice that the **metric** for *estimated rows output* has exploded by 100x after joining the **transactions** table with the **stores** table, which is caused by duplicates of the store-id in the stores table.
# MAGIC
# MAGIC 6. In the same location, look at the **metric** **num bytes spilled to disk due to memory pressure total (min, med, max)**. Notice that *927.6 MiB* (value can vary) spilled to disk.
# MAGIC
# MAGIC 7. Leave the Spark UI open.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### D2. TODO: See the Spill
# MAGIC In the Spark UI view the memory spill. 
# MAGIC
# MAGIC 1. In the Spark UI select **Stages** on the top navigation bar. Here you can see all the stages performed on the cluster.
# MAGIC
# MAGIC 2. Find the stage with the largest amount of **Shuffle Writes** (should be around 1335.0 MiB, but it can vary) for the query within the **Description** column that begins with `joined_df_nobroadcast = spark("""SELECT...)`.
# MAGIC
# MAGIC 3. After you find that stage, select the link in the **Description** field.
# MAGIC ![1.4-d2_find_spill.png](./Includes/images/1.4-d2_find_spill.png)
# MAGIC
# MAGIC 4. Look at the field **Spill (Disk)** for the stage. Notice that this query spilled to disk.
# MAGIC
# MAGIC     **NOTE:** In Apache Spark, when there's more data than it can handle in memory, it puts some of the extra data on the hard drive. This is called *spilling to disk* and the 927.9 MiB means it had to move about 927.9 megabytes of data to the hard drive to keep processing.
# MAGIC
# MAGIC <br></br>
# MAGIC ![1.4-d2_memory_spill.png](./Includes/images/1.4-d2_memory_spill.png)
# MAGIC
# MAGIC
# MAGIC
# MAGIC 5. In the same page look at the **Locality Level Summary**. This means that the number of partitions in this stage is 4. Think to yourself, is this a good setting?
# MAGIC
# MAGIC 6. Close the Spark UI browser.

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## E. Improve: Increasing the number of shuffle partitions
# MAGIC Let's try to increase the performance of this query by increasing the number of shuffle partitions. You can do this by modifying the **spark.sql.shuffle.partitions** configuration setting and setting it to **8** partitions. 
# MAGIC
# MAGIC This configures the number of partitions to use when shuffling data for joins or aggregations.
# MAGIC
# MAGIC For more information, view the [spark.sql.shuffle.partitions](https://spark.apache.org/docs/3.5.3/sql-performance-tuning.html#other-configuration-options) documentation.
# MAGIC

# COMMAND ----------

## Rerun the cell to turn off the broadcast join features if not already turned off
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
spark.conf.set("spark.databricks.adaptive.autoBroadcastJoinThreshold", -1)

## Deal with the spill by increasing the number of shuffle partitions to 8
## Not a huge difference in this small example, but as the amount of data spilled increases
## This can make a big difference
spark.conf.set("spark.sql.shuffle.partitions", <FILL_IN>)

# COMMAND ----------

# MAGIC %md
# MAGIC Run the same query as the previous example, but this time with the number of shuffle partitions set to 8. Note the time the query takes to execute.
# MAGIC
# MAGIC **NOTE:** This should take about ~50 seconds.
# MAGIC

# COMMAND ----------

joined_df_8_partitions = spark.sql("""
    SELECT 
        transactions.id,
        amount,
        countries.name as country_name,
        employees,
        stores.name as store_name
    FROM
        transactions
    JOIN
        stores
        ON
            transactions.store_id = stores.id
    JOIN
        countries
        ON
            transactions.country_id = countries.id
""")

(joined_df_8_partitions
 .write
 .mode('overwrite')
 .saveAsTable('transact_countries')
)

## Reset the shuffle.partions to the default setting
spark.conf.unset("spark.sql.shuffle.partitions")

# COMMAND ----------

# MAGIC %md
# MAGIC ### E1. TODO: See the Spill
# MAGIC
# MAGIC In the Spark UI view the memory spill. 
# MAGIC
# MAGIC 1. Expand **Spark Jobs**, on the first job right click and select **Open in a New Tab**.
# MAGIC
# MAGIC 2. In the Spark UI select **Stages** on the top navigation bar.
# MAGIC
# MAGIC 3. Find the stage with the largest amount of shuffle writes (should be around 679.9 MiB, but it can vary) for the query within the **Description** column that begins with `joined_df_8_partitions = spark("""SELECT...)`.
# MAGIC
# MAGIC 4. After you find that stage, select the link in the **Description** field.
# MAGIC
# MAGIC 5. Look at the field **Spill (Disk)**. Notice that this stage spilled to disk.
# MAGIC
# MAGIC     **NOTE:** In Apache Spark, when there's more data than it can handle in memory, it puts some of the extra data on the hard drive. This is called *spilling to disk* and the 273.4 MiB means it had to move about 273.4 MiB of data to the hard drive to keep processing.
# MAGIC
# MAGIC     When modifying the number of partitions the spill has decreased.
# MAGIC
# MAGIC
# MAGIC <br></br>
# MAGIC ![1.4-e_memory_spill.png](./Includes/images/1.4-e_memory_spill.png)
# MAGIC <br></br>
# MAGIC
# MAGIC 6. In the same page look at the **Locality Level Summary**. This means that the number of partitions in this stage is 6 even though we set the number of partitions to 8. This is because Spark automatically decides to combine smaller partitions into bigger ones during a job, which can help Spark finish the job more quickly and use less memory. 
# MAGIC
# MAGIC     You can disable this by setting the following option `spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", False)`.
# MAGIC
# MAGIC
# MAGIC 7. Note down amount of data spilled has been reduced and time taken to execute the query is a bit faster. 
# MAGIC
# MAGIC **NOTE:** The spill metrics of 273MB spilled can be observed in query plan in **PhotonShuffleExchangeSink** that follows the join operations, as well as stage details for stage that has largest amount of shuffle writes.

# COMMAND ----------

# MAGIC %md
# MAGIC ## F. Improve: Change the Order of the Join

# COMMAND ----------

# MAGIC %md
# MAGIC Run the cell below to turn off **autoBroadcastJoinThreshold** if not already turned off.

# COMMAND ----------

## Turn off the broadcast join features if not already turned off
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
spark.conf.set("spark.databricks.adaptive.autoBroadcastJoinThreshold", -1)

# COMMAND ----------

# MAGIC %md
# MAGIC Let's look at the modified query below, which now joins the **transactions** table with the **countries** first, then joins with the **stores** table. 
# MAGIC
# MAGIC Running the smaller join first (avoiding the large exploding join) means we don't have to shuffle as much data as we did when joining with the **stores** table first.
# MAGIC
# MAGIC Run the cell and note the amount of time it takes to complete the query.
# MAGIC
# MAGIC **NOTE:** The query has been modified for you. Simply run the cell.

# COMMAND ----------

small_joined_first_df = spark.sql("""
    SELECT 
        transactions.id,
        amount,
        countries.name as country_name,
        employees,
        stores.name as store_name
    FROM
        transactions
    -- Notice that we are joining with countries first to avoid instead of stores --
    JOIN
        countries
        ON
            transactions.country_id = countries.id
    -- Then we join the results with the stores table, avoiding the shuffle of the large exploding join  --
    JOIN
        stores
        ON
            transactions.store_id = stores.id
""")


(small_joined_first_df
 .write
 .mode('overwrite')
 .saveAsTable('transact_countries')
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### F1. TODO: See the Shuffles
# MAGIC Open the Spark UI and navigate to the query DAG. Identify the explosion of rows in the DAG of the Spark UI. To view how the DAG works, complete the following:
# MAGIC
# MAGIC 1. In the cell above, expand **Spark Jobs**.
# MAGIC
# MAGIC 2. In the first Job, right-click on **View** and select *Open in a New Tab*. 
# MAGIC
# MAGIC     **NOTE:** In the Vocareum lab environment, if you click **View** without opening it in a new tab, the pop-up window will display an error.
# MAGIC
# MAGIC 3. In the new window, find the **Associated SQL Query** header at the top and select the number.
# MAGIC
# MAGIC 4. Here, you should see the entire query plan. Read the DAG graph bottom-up to better understand the details of the execution plan. 
# MAGIC
# MAGIC 5. In the query plan view, notice that the first join is between the **transactions** and **countries** tables, which returns 2 million results. Then, the results join with the **stores** table, creating the exploding join. This avoids having to shuffle the large join between **transactions** and **stores** (~200,000,000 rows) as we did in the first query.
# MAGIC
# MAGIC <br></br>
# MAGIC ![1.4-f_smaller_join_first_dag.png](./Includes/images/1.4-f_smaller_join_first_dag.png)
# MAGIC
# MAGIC <br></br>
# MAGIC 6. Leave the Spark UI open.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### F2. TODO: See the Spill
# MAGIC In the Spark UI view the memory spill. 
# MAGIC
# MAGIC 1. In the Spark UI select **Stages** on the top navigation bar. Here you can see all the stages performed on the cluster.
# MAGIC
# MAGIC 2. Find the stage with the largest amount of **Shuffle Writes** (should be around 19.1 MiB, but it can vary) for the query within the **Description** column that begins with `small_joined_first_df = spark("""SELECT...)`.
# MAGIC
# MAGIC 3. After you find that stage, select the link in the **Description** field.
# MAGIC
# MAGIC 4. Look at the field **Spill (Disk)**. Notice that this query did not spill to disk.
# MAGIC
# MAGIC <br></br>
# MAGIC ![1.4-f2_memory_spill.png](./Includes/images/1.4-f2_memory_spill.png)
# MAGIC <br></br>
# MAGIC
# MAGIC
# MAGIC 5. Close the Spark UI browser.

# COMMAND ----------

# MAGIC %md
# MAGIC Can you still see Spill in Spark UI? Did this query run faster or slower than the previous example?

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## G. Improve: Analyze and Use Default Broadcast Configurations: 
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC Reset the **spark.sql.autoBroadcastJoinThreshold** and **spark.databricks.adaptive.autoBroadcastJoinThreshold** configurations using the `spark.conf.unset()` method.
# MAGIC
# MAGIC [spark.sql.autoBroadcastJoinThreshold](https://spark.apache.org/docs/3.5.3/sql-performance-tuning.html#other-configuration-options) documentation
# MAGIC
# MAGIC [spark.databricks.adaptive.autoBroadcastJoinThreshold](https://spark.apache.org/docs/3.5.3/sql-performance-tuning.html#converting-sort-merge-join-to-broadcast-join) documentation
# MAGIC
# MAGIC Run the cell and view the results. Confirm the following default values for the configurations:
# MAGIC - *Default value of autoBroadcastJoinThreshold: 10,485,760 bytes*
# MAGIC - *Default value of adaptive.autoBroadcastJoinThreshold: 31,457,280 bytes*
# MAGIC

# COMMAND ----------

# Reset defaults here
spark.conf.unset(<FILL_IN>)
spark.conf.unset(<FILL_IN>)

# Display the values
print(f'Default value of autoBroadcastJoinThreshold: {spark.conf.get("spark.sql.autoBroadcastJoinThreshold")}')
print(f'Default value of adaptive.autoBroadcastJoinThreshold: {spark.conf.get("spark.databricks.adaptive.autoBroadcastJoinThreshold")}')

# COMMAND ----------

# MAGIC %md
# MAGIC Cost-based optimizers rely on statistics information to generate the most efficient physical query plan with the lowest cost. This includes decisions on join strategy and the order of joins.
# MAGIC
# MAGIC Running `ANALYZE` on the joining columns across the three tables allows the optimizer to make better decisions, and everything will work as if by magic. 
# MAGIC
# MAGIC Complete the cell below by writing the required `ANALYZE` statements. 
# MAGIC
# MAGIC **HINT:** View the [ANALYZE TABLE](https://docs.databricks.com/en/sql/language-manual/sql-ref-syntax-aux-analyze-table.html) documentation for more information.
# MAGIC

# COMMAND ----------


# Analyze the transactions table and compute statistics for columns country_id and store_id


# Analyze the stores table and compute statistics for the column id


# Analyze the countries table and compute statistics for the column id

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC In the example below, a developer writes joins without considering the optimal order of joins.
# MAGIC
# MAGIC Run the query again using the initial query we used in this demonstration that joins **transactions** with **stores** first and explodes the data for the large shuffle. 
# MAGIC
# MAGIC Will letting Spark figure out how to join the data efficiently work? 
# MAGIC
# MAGIC Note down time taken to do the join.

# COMMAND ----------

joined_df_analyze = spark.sql("""
    SELECT 
        transactions.id,
        amount,
        countries.name as country_name,
        employees,
        stores.name as store_name
    FROM
        transactions
    JOIN
        stores
        ON
            transactions.store_id = stores.id
    JOIN
        countries
        ON
            transactions.country_id = countries.id
""")

(joined_df_analyze
 .write
 .mode('overwrite')
 .saveAsTable('transact_countries')
)

# COMMAND ----------

# MAGIC %md
# MAGIC **NOTES:**
# MAGIC - We put the shuffle settings back to the defaults.  Usually best to stick with the defaults which tend to improve over time.  If you hard-code configurations, you may be opting out of future performance improvements unwittingly.  Always worth checking old configurations to make sure they're still needed.  You could get big performance improvements by cleaning out old configurations.

# COMMAND ----------

# MAGIC %md
# MAGIC View the Spark UI. Think about the following:
# MAGIC - What can you see in DAG of query plan? 
# MAGIC - Any spill? 
# MAGIC - Rows exploded? 
# MAGIC - Order of Join? 
# MAGIC - Is the DAG similar to the previous joins? 
# MAGIC - Did the query execute faster? What were the size of the shuffle writes? 
# MAGIC - Larger or smaller than the previous queries?
# MAGIC
# MAGIC <br></br>
# MAGIC **Stages**
# MAGIC - Notice the small amount of shuffle writes.
# MAGIC ![1.4-g_analyze_stages.png](./Includes/images/1.4-g_analyze_stages.png)
# MAGIC
# MAGIC <br></br>
# MAGIC **DAG**
# MAGIC - Look at the differences in the DAG.
# MAGIC ![1.4_g_dag.png](./Includes/images/1.4_g_dag.png)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary of the 4 Joins
# MAGIC
# MAGIC | Join Strategy | Execution Time | Memory Spill | Largest Shuffle Write | Notes |
# MAGIC |---------------|----------------|----------------|----------------|----------------|
# MAGIC |D. Exploding Join| ~60 seconds| ~928 MiB |	1334.8 MiB |Joining the **transactions** table with the **store** table (exploding join) first |
# MAGIC |E. Increase the number of shuffles| ~50 seconds | ~273.7 MiB | 680.5 MiB |Same join as before, specify 8 partitions |
# MAGIC |F. Change the join order | ~40 seconds| 0 | 19.1 MiB |Change the join order to join **transactions** with **countries** first |
# MAGIC |G. Analyze and broadcast join| ~20 seconds| 0 | 383.6 KiB |Let Databricks analyze and use default broadcast configurations. While the query took about the same time, |
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC &copy; 2025 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the 
# MAGIC <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/><a href="https://databricks.com/privacy-policy">Privacy Policy</a> | 
# MAGIC <a href="https://databricks.com/terms-of-use">Terms of Use</a> | 
# MAGIC <a href="https://help.databricks.com/">Support</a>