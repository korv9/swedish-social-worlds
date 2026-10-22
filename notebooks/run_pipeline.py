# Databricks notebook source
# MAGIC %md
# MAGIC # Swedish Social Worlds: end-to-end pipeline
# MAGIC
# MAGIC **Ingestion → Bronze → Silver → embeddings → clustering → Gold → interactive UMAP.** Use *Run all*.
# MAGIC
# MAGIC Clusters are exploratory themes that need human interpretation, not historical conclusions.
# MAGIC
# MAGIC Requires Databricks Runtime 15.4 LTS ML or newer and Unity Catalog. A GPU speeds up embeddings but is optional.

# COMMAND ----------

# MAGIC %pip install -q umap-learn==0.5.7 sentence-transformers==3.3.1

# COMMAND ----------

# MAGIC %run ./00_setup

# COMMAND ----------

# MAGIC %run ./01_ingestion

# COMMAND ----------

# MAGIC %run ./02_bronze

# COMMAND ----------

# MAGIC %run ./03_silver

# COMMAND ----------

# MAGIC %run ./04_embeddings

# COMMAND ----------

# MAGIC %run ./05_clustering

# COMMAND ----------

# MAGIC %run ./06_gold

# COMMAND ----------

# MAGIC %run ./07_umap_exports
