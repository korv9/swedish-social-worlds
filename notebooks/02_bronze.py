# Databricks notebook source
# MAGIC %md
# MAGIC # 02 Bronze
# MAGIC Delta table `bronze_texts`: one row per original text with its Volume path, source metadata and licence.
# MAGIC
# MAGIC Run via `run_pipeline` (or `%run ./00_setup` first): uses config and state from earlier stages.

# COMMAND ----------

if rows:
    (spark.createDataFrame(rows, "corpus string, text_id string, attrs map<string,string>, raw_path string, "
                                 "source_file_url string, doi string, license string, ingested_at timestamp")
     .write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("bronze_texts"))

display(spark.sql("SELECT corpus, count(*) texts, min(ingested_at) ingested_at FROM bronze_texts GROUP BY corpus"))
