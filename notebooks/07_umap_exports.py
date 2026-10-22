# Databricks notebook source
# MAGIC %md
# MAGIC # 07 Interactive UMAP and exports
# MAGIC Plotly map per corpus and HTML/Parquet/JSON exports in the `exports` Volume for the portfolio (R2).
# MAGIC
# MAGIC Run via `run_pipeline` (or `%run ./00_setup` first): uses config and state from earlier stages.

# COMMAND ----------

import plotly.express as px

kw = {(c["corpus"], c["cluster_id"]): ", ".join(c["keywords"][:3]) for c in clusters}
export = gold_points.assign(
    cluster=lambda d: [f"{c}: {kw[(k, c)]}" if c >= 0 else "noise" for k, c in zip(d.corpus, d.cluster_id)],
    excerpt=lambda d: d.text.str.slice(0, 300))

for corpus, g in export.groupby("corpus"):
    g = g.assign(passage=g.excerpt.map(lambda t: "<br>".join(textwrap.wrap(t, 70))))  # hover-only line breaks
    fig = px.scatter(
        g, x="umap_x", y="umap_y", color="cluster", custom_data=["source_url"],
        hover_data={"title": True, "year": True, "passage": True,
                    "umap_x": False, "umap_y": False, "cluster": False},
        color_discrete_map={"noise": "lightgrey"},
        title=f"{corpus}: thematic map ({len(g)} passages)<br><sup>{ATTRIBUTION}</sup>",
        height=750)
    fig.update_traces(marker_size=5)
    fig.write_html(f"{EXPORTS}/umap_{corpus}.html", include_plotlyjs="cdn",
                   post_script="document.getElementById('{plot_id}').on('plotly_click', "
                               "e => window.open(e.points[0].customdata[0], '_blank'));")
    fig.show()

export.drop(columns=["text", "cluster"]).to_parquet(f"{EXPORTS}/points.parquet", index=False)
with open(f"{EXPORTS}/clusters.json", "w", encoding="utf-8") as f:
    json.dump({"attribution": ATTRIBUTION, "model": MODEL, "run_at": run_at.isoformat(), "clusters": clusters},
              f, ensure_ascii=False, indent=2)
print("exports:", os.listdir(EXPORTS))
