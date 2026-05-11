# 🤘 Metal Analytics Pipeline 🤘

An end-to-end data engineering project exploring the global 
metal music scene from band origins to audio DNA lml!.

> Data is pulled automatically via the Last.fm API.
> No manual datasets required. 

The pipeline discovers:
> top artists per genre dynamically based on real
> community listening data.

<br>

## 🏗️ Architecture

Metal-Archives + Last.fm API → Bronze → Silver → Gold → Tableau

<br>

## 🛠️ Tech Stack

| Layer | Tool |
|---|---|
| Ingestion & Processing | Databricks + PySpark |
| Transformation | dbt Core |
| Storage | Delta Lake (Unity Catalog) |
| Visualization | Tableau Public |
| Version Control | GitHub |
| Data Sources | Metal-Archives + Last.fm API |

<br>

## 🎸 Business Questions Answered

- Which metal subgenres have the most global listeners?
- What is the loyalty score of each subgenre? (playcount/listeners)
- Which bands are most similar to each other across subgenres?
- Which subgenres produce the most cross-genre artists?
- Which metal albums have the highest playcount globally?
- How does popularity differ between underground and mainstream metal?
- Which subgenres are growing vs declining in global listenership?

<br>

## 🥉🥈🥇 Medallion Architecture

- **Bronze** — Raw data from Metal-Archives & Last.fm API
- **Silver** — Cleaned, typed, enriched Delta Tables
- **Gold** — Business-ready aggregations powering the dashboard

<br>

## 📁 Project Structure

    notebooks/     # PySpark ingestion and transformation notebooks
    dbt/           # dbt models and data quality tests
    dashboard/     # Tableau screenshots and public link

<br>

## 🔗 Live Dashboard

[View on Tableau Public](#)

<br>

## 📊 Dataset Scale

| Dataset | Records |
|---|---|
| Tracks | 12,834 |
| Artists | 1,286 |
| Similar Artist Relationships | 6,414 |
| Albums | 6,387 |
| Metal Subgenres | 30 |

<br>

## 📦 Data Sources

- [Encyclopaedia Metallum](https://www.metal-archives.com)
- [Last.fm API](https://www.last.fm/api)
