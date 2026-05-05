# 🤘 Metal Analytics Pipeline

An end-to-end data engineering project exploring the global 
metal music scene — from band origins to audio DNA.

## 🏗️ Architecture

Metal-Archives + Spotify API → Bronze → Silver → Gold → Tableau

<br>

## 🛠️ Tech Stack

| Layer | Tool |
|---|---|
| Ingestion & Processing | Databricks + PySpark |
| Transformation | dbt Core |
| Storage | Delta Lake (Unity Catalog) |
| Visualization | Tableau Public |
| Version Control | GitHub |
| Data Sources | Metal-Archives + Spotify API |

## 🎸 Business Questions Answered

- Which countries dominate each metal subgenre?
- What was the golden decade for Death, Black and Thrash Metal?
- What do audio features reveal about subgenre differences?
- Which labels shaped the metal scene the most?
- Is the global metal scene growing or shrinking?

## 🥉🥈🥇 Medallion Architecture

- **Bronze** — Raw data from Metal-Archives & Spotify API
- **Silver** — Cleaned, typed, enriched Delta Tables
- **Gold** — Business-ready aggregations powering the dashboard

## 📁 Project Structure

    notebooks/     # PySpark ingestion and transformation notebooks
    dbt/           # dbt models and data quality tests
    dashboard/     # Tableau screenshots and public link

## 🔗 Live Dashboard

[View on Tableau Public](#)

## 📦 Data Sources

- [Encyclopaedia Metallum](https://www.metal-archives.com)
- [Spotify Web API](https://developer.spotify.com)
