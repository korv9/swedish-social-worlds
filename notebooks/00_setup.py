# Databricks notebook source
# MAGIC %md
# MAGIC # 00 Setup
# MAGIC Widgets, Unity Catalog schema and volumes, source definitions.

# COMMAND ----------

dbutils.widgets.text("catalog", "")  # empty = workspace default (e.g. `workspace` on Free Edition)
dbutils.widgets.text("schema", "swedish_social_worlds")
dbutils.widgets.text("folke_max_texts", "5000")
dbutils.widgets.text("lb_max_texts", "60")
dbutils.widgets.text("max_passages_per_corpus", "2000")
dbutils.widgets.text("embedding_model", "intfloat/multilingual-e5-base")
dbutils.widgets.text("min_cluster_size", "15")
dbutils.widgets.dropdown("refresh_bronze", "false", ["true", "false"])

W = {k: dbutils.widgets.get(k) for k in [
    "catalog", "schema", "folke_max_texts", "lb_max_texts", "max_passages_per_corpus",
    "embedding_model", "min_cluster_size", "refresh_bronze"]}

# COMMAND ----------

import os, sys, json, textwrap
from datetime import datetime, timezone

sys.path.append(os.path.abspath("../src"))
from pyspark.sql import functions as F
import sparv

C, S = W["catalog"] or spark.sql("SELECT current_catalog()").first()[0], W["schema"]
print("Unity Catalog location:", f"{C}.{S}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {C}.{S}")
for vol in ("raw", "exports"):
    spark.sql(f"CREATE VOLUME IF NOT EXISTS {C}.{S}.{vol}")
spark.sql(f"USE {C}.{S}")
RAW, EXPORTS = f"/Volumes/{C}/{S}/raw", f"/Volumes/{C}/{S}/exports"

SOURCES = {
    "folke": dict(
        url="https://spraakbanken.gu.se/resurser/meningsmangder/sbs-folke.xml.bz2",
        doi="https://doi.org/10.23695/pawz-fh93",
        license="CC-BY-4.0",
        attribution="Folke, Institute for Language and Folklore (Isof), via Språkbanken Text",
        keep=lambda a: True,
        max_texts=int(W["folke_max_texts"]),
    ),
    "litteraturbanken": dict(
        url="https://spraakbanken.gu.se/resurser/meningsmangder/lb-open.xml.bz2",
        doi="https://doi.org/10.23695/evf4-hq97",
        license="CC-BY-4.0",
        attribution="Litteraturbanken: Free Works, via Språkbanken Text",
        keep=lambda a: a.get("mediatype") == "etext",  # skip OCR facsimiles
        max_texts=int(W["lb_max_texts"]),
    ),
}
ATTRIBUTION = "; ".join(f"{s['attribution']} ({s['license']}, {s['doi']})" for s in SOURCES.values())
