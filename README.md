# Swedish Social Worlds

A computational humanities project on how **work, class, poverty and belonging** are portrayed in Swedish
historical narratives and literature. Output: an interactive text atlas (thematic clusters on a UMAP map)
with readable excerpts linked back to the original sources.

Source texts stay in Swedish. Code, docs and UI are in English.

Clusters are **exploratory themes that need interpretation**, not finished historical conclusions.

## Pipeline

`notebooks/run_pipeline.py` runs the whole chain with *Run all*. It calls one notebook per stage, using `%run`:

```
00_setup → 01_ingestion → 02_bronze → 03_silver → 04_embeddings → 05_clustering → 06_gold → 07_umap_exports
```

| Layer | Where | Contents |
|---|---|---|
| Bronze | Volume `raw/<corpus>/<text_id>.xml`, table `bronze_texts` | Original Sparv XML per text, source file URL, DOI, licence, ingest time |
| Silver | `silver_documents`, `silver_passages`, `silver_theme_passages` | Clean documents, ~150-word passages with stable IDs (`corpus:doc_id:0000`) and source links, theme-filtered sample |
| ML | `embeddings` | Multilingual embeddings (`intfloat/multilingual-e5-base`), cached by passage, text hash and model |
| Gold | `gold_passage_points`, `gold_clusters` | Cluster membership, UMAP coordinates, metadata; cluster keywords (class TF-IDF on lemmas), representative passages, empty `label` for curators |
| Exports | Volume `exports/` | `umap_<corpus>.html` (click a point to open the source), `points.parquet`, `clusters.json` |

Clustering: 5-d UMAP, then HDBSCAN per corpus. A separate 2-d UMAP is used for the map. The corpora are clustered separately for now.

## Data

Both sources come from Språkbanken Text as Sparv-annotated XML. Each corpus is streamed, and only the first N texts are read.

| Corpus | File | Licence | Cite |
|---|---|---|---|
| Folke (Isof transcribed folklore records) | `sbs-folke.xml.bz2` (63 MB) | CC BY 4.0 | https://doi.org/10.23695/pawz-fh93 |
| Litteraturbanken free works | `lb-open.xml.bz2` (5.75 GB) | CC BY 4.0 | https://doi.org/10.23695/evf4-hq97 |

Every export carries the attribution string, as CC BY requires.

## Run

1. Add this repo as a Databricks Git folder.
2. Open `notebooks/run_pipeline.py` on a cluster with **DBR 15.4 LTS ML+** and Unity Catalog. A GPU is optional.
3. Optionally set the `catalog`/`schema` widgets (empty catalog = workspace default), then click *Run all*.

The parser self-check runs locally without dependencies: `python src/sparv.py`.

## Known limits (MVP)

- The sample is the first N texts in archive order, not a random sample.
- The theme filter matches modern Sparv lemmas. It misses old spellings that Sparv cannot lemmatise.
- The Folke record link pattern (`sok.folke.isof.se/records/<id>`) has not been verified with Isof yet.

## Team plan

| Owner | Next |
|---|---|
| Data engineering | Run on Databricks; verify Folke record URLs; move from first-N to stratified sampling (by decade, genre) |
| AI/ML | Compare e5-base with a Swedish model (KBLab) on a labelled sample; tune `min_cluster_size`; try a joint map of both corpora |
| Analysts | Label clusters in `gold_clusters.label`; extend `THEME_LEMMAS` with historical spellings; write the interpretation notes |
| Engineering | Upload `exports/` to R2; embed the HTML map in the portfolio |
