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
# MAGIC ## Databricks Performance Optimization
# MAGIC In this course, you’ll learn how to optimize workloads and physical layout with Spark and Delta Lake and and analyze the Spark UI to
# MAGIC assess performance and debug applications. We’ll cover topics like streaming, liquid clustering, data skipping, caching, photons, and
# MAGIC more.
# MAGIC ---
# MAGIC #### Prerequisites
# MAGIC You should meet the following prerequisites before starting this course:
# MAGIC
# MAGIC - Ability to perform basic code development tasks using Databricks (`create clusters`, `run code in notebooks`, `use basic notebook operations`, `import repos from git`, etc.)
# MAGIC - Intermediate programming experience with PySpark, such as extract data from a variety of file format data sources, apply a number
# MAGIC of common transformations to clean data, and reshape and manipulate complex data using advanced built-in functions
# MAGIC - Intermediate programming experience with Delta Lake (`create tables`, `perform complete and incremental updates`, `compact files`,
# MAGIC `restore previous versions`, etc.)
# MAGIC ---
# MAGIC #### Course Agenda
# MAGIC The following modules are part of the **Data Engineer Learning Path** by Databricks Academy.
# MAGIC
# MAGIC | # | Module Name |
# MAGIC | --- | --- |
# MAGIC | 1 | [PO 1.1 - Demo - File Explosion]($./PO 1.1 - File Explosion) |
# MAGIC | 2 | [PO 1.2L - Lab - Data Skipping and Liquid Clustering]($./PO 1.2L - Data Skipping and Liquid Clustering) |
# MAGIC | 3 | [PO 1.3 - Demo - Shuffle]($./PO 1.3 - Shuffle) |
# MAGIC | 4 | [PO 1.4L - Lab - Exploding Join]($./PO 1.4L - Exploding Join) |
# MAGIC | 5 | [PO 1.5 - Demo - User-Defined Functions]($./PO 1.5 - User-Defined Functions) |
# MAGIC
# MAGIC ---
# MAGIC ### Requirements
# MAGIC
# MAGIC Please review the following requirements before starting the lesson:
# MAGIC
# MAGIC - Use Databricks Runtime version: **`15.4.x-scala2.12`** for running all demo and lab notebooks.
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