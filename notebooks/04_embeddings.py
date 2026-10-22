# Databricks notebook source
# MAGIC %md
# MAGIC # 04 Embeddings
# MAGIC Multilingual embeddings, cached in `embeddings` by passage, text hash and model.
# MAGIC
# MAGIC Run via `run_pipeline` (or `%run ./00_setup` first): uses config and state from earlier stages.

# COMMAND ----------

from sentence_transformers import SentenceTransformer

MODEL = W["embedding_model"]
spark.sql("CREATE TABLE IF NOT EXISTS embeddings "
          "(passage_id string, text_hash string, model string, embedding array<float>, created_at timestamp)")
cached = spark.table("embeddings").where(F.col("model") == MODEL)
todo = (spark.table("silver_theme_passages")
        .join(cached, ["passage_id", "text_hash"], "left_anti")
        .select("passage_id", "text_hash", "text").toPandas())
print(len(todo), "passages to embed")

if len(todo):
    prefix = "passage: " if "e5" in MODEL else ""  # E5 models expect this prefix
    vecs = SentenceTransformer(MODEL).encode(
        [prefix + t for t in todo.text], batch_size=32, normalize_embeddings=True, show_progress_bar=True)
    todo = todo.drop(columns="text").assign(
        model=MODEL, embedding=[v.astype("float32").tolist() for v in vecs], created_at=datetime.now(timezone.utc))
    spark.createDataFrame(todo, spark.table("embeddings").schema).write.mode("append").saveAsTable("embeddings")
