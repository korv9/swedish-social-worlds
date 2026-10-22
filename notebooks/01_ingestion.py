# Databricks notebook source
# MAGIC %md
# MAGIC # 01 Ingestion
# MAGIC Stream each corpus from Språkbanken and land the original XML per text in the `raw` Volume.
# MAGIC
# MAGIC Run via `run_pipeline` (or `%run ./00_setup` first): uses config and state from earlier stages.

# COMMAND ----------

INGEST = W["refresh_bronze"] == "true" or not spark.catalog.tableExists("bronze_texts")
rows = []
if INGEST:
    now = datetime.now(timezone.utc)
    for corpus, src in SOURCES.items():
        os.makedirs(f"{RAW}/{corpus}", exist_ok=True)
        for attrs, raw in sparv.stream_texts(src["url"], src["max_texts"], src["keep"]):
            tid = sparv.text_id(attrs)
            path = f"{RAW}/{corpus}/{tid}.xml"
            with open(path, "w", encoding="utf-8") as f:
                f.write(raw)
            rows.append((corpus, tid, attrs, path, src["url"], src["doi"], src["license"], now))
        print(corpus, sum(r[0] == corpus for r in rows), "texts landed in", f"{RAW}/{corpus}")
else:
    print("bronze_texts exists: skipping download (set refresh_bronze=true to re-ingest)")
