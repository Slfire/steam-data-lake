# Steam Data Lake

End-to-end data pipeline that collects Steam gaming data daily and stores it in a structured data lake for analysis.

## Overview

This project implements a **medallion architecture** using open-source tools:

- **Bronze layer**: Raw JSON data ingested from the Steam API, stored in MinIO (S3-compatible object storage)
- **Silver layer**: Cleaned and normalized data loaded into PostgreSQL for querying

The pipeline runs automatically every day via Apache Airflow.

## Architecture

```
Steam API
    │
    ▼
Airflow (bronze_steam_ingestion)
    │
    ▼
MinIO ─── raw JSON files (bronze/)
    │
    ▼
Airflow (silver_steam_load)
    │
    ▼
PostgreSQL ─── structured tables (silver)
```

## Stack

| Tool | Role |
|---|---|
| Apache Airflow | Pipeline orchestration & scheduling |
| MinIO | Raw data lake storage (S3-compatible) |
| PostgreSQL | Data warehouse |
| Docker | Containerization |

## Data collected

About **1000 Steam games** with the following tables:

- `steam_games` — game details (price, metacritic score, platforms, description...)
- `steam_genres` — genre reference table
- `steam_game_genres` — game ↔ genre relationship
- `steam_developers` — developers and publishers
- `steam_game_devs` — game ↔ developer relationship
- `steam_player_counts` — daily snapshot of concurrent players per game

## Getting started

### Requirements

- Docker & Docker Compose
- A Steam API key (free at https://steamcommunity.com/dev/apikey)
- A running PostgreSQL instance

### Setup

1. Clone the repo

```bash
git clone https://github.com/Slfire/steam-data-lake.git
cd steam-data-lake
```

2. Create your `.env` file from the example

```bash
cp .env.example .env
# Fill in your Steam API key and PostgreSQL credentials
```

3. Initialize the PostgreSQL tables

```bash
psql -h your_host -U your_user -d your_db -f sql/init_tables.sql
```

4. Start the stack

```bash
docker compose up -d
```

5. Open Airflow at http://localhost:8080 (admin / admin), enable and trigger `bronze_steam_ingestion`

The silver DAG (`silver_steam_load`) will be triggered automatically once the bronze pipeline completes.

## Example queries

```sql
-- Top 10 games by Metacritic score
SELECT name, metacritic, recommendations
FROM steam_games
WHERE metacritic IS NOT NULL
ORDER BY metacritic DESC
LIMIT 10;

-- Number of games per genre
SELECT g.genre_name, COUNT(*) as total
FROM steam_game_genres gg
JOIN steam_genres g USING (genre_id)
GROUP BY g.genre_name
ORDER BY total DESC;

-- Free games with the most recommendations
SELECT name, recommendations
FROM steam_games
WHERE is_free = true
ORDER BY recommendations DESC
LIMIT 20;

-- Games available on all platforms
SELECT name, price_final
FROM steam_games
WHERE platforms_win = true
  AND platforms_mac = true
  AND platforms_linux = true
ORDER BY recommendations DESC NULLS LAST;
```
