# Databricks notebook source
# MAGIC %md
# MAGIC # 03 Silver
# MAGIC Clean documents and passages with stable IDs and source links, then the theme-filtered sample.
# MAGIC
# MAGIC Run via `run_pipeline` (or `%run ./00_setup` first): uses config and state from earlier stages.

# COMMAND ----------

# MAGIC %md ## Documents (Spark SQL)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE silver_documents AS
# MAGIC SELECT
# MAGIC   corpus,
# MAGIC   text_id AS doc_id,
# MAGIC   attrs['title'] AS title,
# MAGIC   coalesce(attrs['author'], array_join(filter(split(attrs['persons'], '[|]'), x -> x != ''), '; ')) AS people,
# MAGIC   array_join(filter(split(attrs['places'], '[|]'), x -> x != ''), '; ') AS places,
# MAGIC   int(substr(attrs['datefrom'], 1, 4)) AS year,
# MAGIC   -- TODO: verify Folke record URL pattern with Isof; Litteraturbanken links come from page URLs per passage
# MAGIC   CASE WHEN corpus = 'folke' THEN concat('https://sok.folke.isof.se/records/', split(text_id, '_')[1]) END AS record_url,
# MAGIC   doi,
# MAGIC   license,
# MAGIC   raw_path
# MAGIC FROM bronze_texts

# COMMAND ----------

# MAGIC %md ## Passages (PySpark UDF over the raw XML)

# COMMAND ----------

from pyspark import cloudpickle
from pyspark.sql import functions as F
import xml.etree.ElementTree as ET

cloudpickle.register_pickle_by_value(sparv)  # ship src/sparv.py functions to executors with the UDF
# Import functions, not the module: pickling the module object drags in the Databricks import loader (dbruntime),
# which executors cannot load.
from sparv import passages, text_id


@F.udf("array<struct<doc_id:string, idx:int, text:string, embed_text:string, lemmas:array<string>, page_url:string>>")
def parse_passages(raw):
    root = ET.fromstring(raw)
    tid = text_id(root.attrib)
    return [(tid, i, t, m, l, u) for i, (t, m, l, u) in enumerate(passages(root))]


paths = [r.raw_path for r in spark.table("bronze_texts").select("raw_path").collect()]
parsed = spark.read.text(paths, wholetext=True).select(F.explode(parse_passages("value")).alias("p")).select("p.*")
docs = spark.table("silver_documents").select("doc_id", "corpus", "record_url")

(parsed.join(docs, "doc_id")
 .where(F.length("text") > 50)
 .select(
     F.concat_ws(":", "corpus", "doc_id", F.lpad(F.col("idx").cast("string"), 4, "0")).alias("passage_id"),
     "corpus", "doc_id", "idx", "text", "embed_text", "lemmas",
     F.coalesce("page_url", "record_url").alias("source_url"),
     F.sha2("embed_text", 256).alias("text_hash"))  # hash what gets embedded
 .write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("silver_passages"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Theme filter: passages about work, class, poverty and belonging
# MAGIC Matches on Sparv lemmas (at least `min_theme_hits` distinct theme words). At most `max_passages_per_doc` per document,
# MAGIC then `max_passages_per_corpus` per corpus. Sampling is deterministic (hash order), so reruns pick the same passages.


# COMMAND ----------

THEME_LEMMAS = [
    "arbete", "arbeta", "arbetare", "arbetsfolk", "dagsverke", "dagakarl", "lön", "fabrik",
    "dräng", "piga", "tjänare", "tjänstefolk", "husbonde", "matmor", "legohjon",
    "torpare", "torp", "statare", "backstugusittare", "inhyses", "herrskap", "herrgård",
    "fattig", "fattigdom", "fattighus", "fattigstuga", "tiggare", "tigga", "nöd", "svält", "hunger", "skuld",
    "hemlös", "främling", "utvandra",
]  # dropped as too broad: rik, tjänst, socken, flytta; amerika is a proper name, now excluded from lemmas
# ponytail: lemma match misses old spellings Sparv cannot lemmatise (e.g. 1800s "arbete" variants); add surface-form regex if recall matters.
theme_sql = "array(" + ", ".join(f"'{w}'" for w in THEME_LEMMAS) + ")"
spark.sql(f"""
CREATE OR REPLACE TABLE silver_theme_passages AS
WITH hits AS (
  SELECT *, array_intersect(lemmas, {theme_sql}) AS theme_hits FROM silver_passages
), per_doc AS (
  SELECT *, row_number() OVER (PARTITION BY doc_id ORDER BY xxhash64(passage_id)) AS doc_rn
  FROM hits WHERE size(theme_hits) >= {int(W['min_theme_hits'])}
), per_corpus AS (
  SELECT *, row_number() OVER (PARTITION BY corpus ORDER BY xxhash64(passage_id)) AS rn
  FROM per_doc WHERE doc_rn <= {int(W['max_passages_per_doc'])}
)
SELECT * EXCEPT (doc_rn, rn) FROM per_corpus WHERE rn <= {int(W['max_passages_per_corpus'])}
""")
display(spark.sql("SELECT corpus, count(*) passages, count(DISTINCT doc_id) docs FROM silver_theme_passages GROUP BY corpus"))
