# Databricks notebook source
# MAGIC %md
# MAGIC # 06 Gold
# MAGIC Delta tables `gold_passage_points` and `gold_clusters`.
# MAGIC
# MAGIC Run via `run_pipeline` (or `%run ./00_setup` first): uses config and state from earlier stages.

# COMMAND ----------

run_at = datetime.now(timezone.utc)
gold_points = pd.concat(points)[[
    "passage_id", "corpus", "doc_id", "cluster_id", "umap_x", "umap_y", "text", "theme_hits",
    "title", "people", "year", "source_url"]].assign(
    theme_hits=lambda d: d.theme_hits.map(list), model=MODEL, run_at=run_at)
(spark.createDataFrame(gold_points, "passage_id string, corpus string, doc_id string, cluster_id int, umap_x double, "
                       "umap_y double, text string, theme_hits array<string>, title string, people string, year int, "
                       "source_url string, model string, run_at timestamp")
 .write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("gold_passage_points"))
(spark.createDataFrame(pd.DataFrame(clusters).assign(model=MODEL, run_at=run_at),
                       "corpus string, cluster_id int, size int, keywords array<string>, "
                       "representative_passage_ids array<string>, label string, model string, run_at timestamp")
 .write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("gold_clusters"))
display(spark.table("gold_clusters"))
