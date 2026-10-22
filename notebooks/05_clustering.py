# Databricks notebook source
# MAGIC %md
# MAGIC # 05 Clustering
# MAGIC Per corpus: UMAP (5d) + HDBSCAN, separate 2d UMAP for the map, class TF-IDF keywords, representative passages.
# MAGIC
# MAGIC Run via `run_pipeline` (or `%run ./00_setup` first): uses config and state from earlier stages.

# COMMAND ----------

import numpy as np
import pandas as pd
import umap
from sklearn.cluster import HDBSCAN
from sklearn.feature_extraction.text import TfidfVectorizer

MIN_CS = int(W["min_cluster_size"])
df = (spark.table("silver_theme_passages")
      .join(spark.table("embeddings").where(F.col("model") == MODEL)  # re-read: includes rows just appended
            .select("passage_id", "text_hash", "embedding"), ["passage_id", "text_hash"])
      .join(spark.table("silver_documents").select("doc_id", "title", "people", "year"), "doc_id")
      .toPandas())

points, clusters = [], []
for corpus, g in df.groupby("corpus"):
    if len(g) < 3 * MIN_CS:
        print(f"skip {corpus}: only {len(g)} passages")
        continue
    X = np.stack(g.embedding.values)
    reduced = umap.UMAP(n_components=5, n_neighbors=15, min_dist=0.0, metric="cosine", random_state=42).fit_transform(X)
    labels = HDBSCAN(min_cluster_size=MIN_CS, cluster_selection_method=W["cluster_selection"]).fit_predict(reduced)
    xy = umap.UMAP(n_components=2, n_neighbors=15, min_dist=0.1, metric="cosine", random_state=42).fit_transform(X)
    g = g.assign(cluster_id=labels, umap_x=xy[:, 0], umap_y=xy[:, 1])
    points.append(g)

    ids = sorted(set(labels) - {-1})
    if not ids:
        continue
    # Class-based TF-IDF: one "document" per cluster, built from its content lemmas.
    tfidf = TfidfVectorizer(token_pattern=r"\S+", sublinear_tf=True)
    M = tfidf.fit_transform([" ".join(l for ls in g.lemmas[labels == c] for l in ls) for c in ids])
    vocab = tfidf.get_feature_names_out()
    for row, c in enumerate(ids):
        mask = labels == c
        centroid = X[mask].mean(axis=0)
        top = np.argsort(-(X[mask] @ centroid))[:3]
        clusters.append(dict(
            corpus=corpus, cluster_id=int(c), size=int(mask.sum()),
            keywords=[vocab[i] for i in np.argsort(-M[row].toarray()[0])[:8]],
            representative_passage_ids=g.passage_id[mask].iloc[top].tolist(),
            label=None))  # filled in by a human curator

print(pd.DataFrame(clusters)[["corpus", "cluster_id", "size", "keywords"]].to_string())
