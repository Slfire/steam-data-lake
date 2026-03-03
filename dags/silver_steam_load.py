from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import json
import os
import psycopg2
from psycopg2.extras import execute_values
from minio import Minio


MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "minio:9000")
MINIO_ACCESS = os.environ.get("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET = os.environ.get("MINIO_SECRET_KEY", "minioadmin123")
BUCKET = "bronze"

PG_HOST = os.environ.get("POSTGRES_HOST", "localhost")
PG_PORT = int(os.environ.get("POSTGRES_PORT", 5432))
PG_DB = os.environ.get("POSTGRES_DB", "postgres")
PG_USER = os.environ.get("POSTGRES_USER", "postgres")
PG_PASS = os.environ.get("POSTGRES_PASSWORD", "")


def get_minio():
    return Minio(MINIO_ENDPOINT, access_key=MINIO_ACCESS, secret_key=MINIO_SECRET, secure=False)


def get_db():
    return psycopg2.connect(
        host=PG_HOST, port=PG_PORT, dbname=PG_DB, user=PG_USER, password=PG_PASS
    )


def read_json(client, path):
    try:
        return json.loads(client.get_object(BUCKET, path).read())
    except Exception as e:
        print(f"Could not read {path}: {e}")
        return None


def create_tables(**context):
    conn = get_db()
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS steam_games (
            appid            INTEGER PRIMARY KEY,
            name             TEXT NOT NULL,
            type             TEXT,
            is_free          BOOLEAN,
            short_desc       TEXT,
            price_initial    NUMERIC(10,2),
            price_final      NUMERIC(10,2),
            currency         TEXT,
            metacritic       INTEGER,
            recommendations  INTEGER,
            release_date     TEXT,
            platforms_win    BOOLEAN DEFAULT FALSE,
            platforms_mac    BOOLEAN DEFAULT FALSE,
            platforms_linux  BOOLEAN DEFAULT FALSE,
            positive_reviews INTEGER,
            negative_reviews INTEGER,
            loaded_at        TIMESTAMP DEFAULT NOW()
        );

        CREATE TABLE IF NOT EXISTS steam_genres (
            genre_id   SERIAL PRIMARY KEY,
            genre_name TEXT UNIQUE NOT NULL
        );

        CREATE TABLE IF NOT EXISTS steam_game_genres (
            appid      INTEGER REFERENCES steam_games(appid) ON DELETE CASCADE,
            genre_id   INTEGER REFERENCES steam_genres(genre_id),
            PRIMARY KEY (appid, genre_id)
        );

        CREATE TABLE IF NOT EXISTS steam_developers (
            dev_id   SERIAL PRIMARY KEY,
            dev_name TEXT UNIQUE NOT NULL
        );

        CREATE TABLE IF NOT EXISTS steam_game_devs (
            appid    INTEGER REFERENCES steam_games(appid) ON DELETE CASCADE,
            dev_id   INTEGER REFERENCES steam_developers(dev_id),
            role     TEXT DEFAULT 'developer',
            PRIMARY KEY (appid, dev_id, role)
        );

        CREATE TABLE IF NOT EXISTS steam_player_counts (
            appid         INTEGER,
            snapshot_date DATE,
            player_count  INTEGER,
            loaded_at     TIMESTAMP DEFAULT NOW(),
            PRIMARY KEY (appid, snapshot_date)
        );
    """)

    conn.commit()
    cur.close()
    conn.close()
    print("Tables ready")


def load_games(**context):
    client = get_minio()
    date = context["ds"]

    prefix = f"steam/game_details/{date}/"
    try:
        objects = [o.object_name for o in client.list_objects(BUCKET, prefix=prefix)]
    except Exception:
        objects = []

    print(f"Found {len(objects)} game files in MinIO")

    conn = get_db()
    cur = conn.cursor()

    games = []
    genres_cache = {}
    devs_cache = {}
    game_genres = []
    game_devs = []

    for path in objects:
        raw = read_json(client, path)
        if not raw:
            continue

        for appid_str, result in raw.items():
            if not result.get("success"):
                continue

            d = result.get("data", {})
            if not d.get("name"):
                continue

            appid = d.get("steam_appid") or int(appid_str)
            price = d.get("price_overview", {})
            meta = d.get("metacritic", {})
            reco = d.get("recommendations", {})

            games.append((
                appid,
                d.get("name", "")[:255],
                d.get("type", "game"),
                d.get("is_free", False),
                (d.get("short_description") or "")[:500],
                round(price.get("initial", 0) / 100, 2) if price else None,
                round(price.get("final", 0) / 100, 2) if price else None,
                price.get("currency") if price else None,
                meta.get("score") if meta else None,
                reco.get("total") if reco else None,
                d.get("release_date", {}).get("date"),
                d.get("platforms", {}).get("windows", False),
                d.get("platforms", {}).get("mac", False),
                d.get("platforms", {}).get("linux", False),
                None,
                None,
            ))

            for g in d.get("genres", []):
                name = g.get("description", "").strip()
                if not name:
                    continue
                if name not in genres_cache:
                    cur.execute(
                        "INSERT INTO steam_genres (genre_name) VALUES (%s) ON CONFLICT (genre_name) DO UPDATE SET genre_name=EXCLUDED.genre_name RETURNING genre_id",
                        (name,)
                    )
                    genres_cache[name] = cur.fetchone()[0]
                game_genres.append((appid, genres_cache[name]))

            for role, key in [("developer", "developers"), ("publisher", "publishers")]:
                for name in d.get(key, []):
                    name = name.strip()[:255]
                    if not name:
                        continue
                    if name not in devs_cache:
                        cur.execute(
                            "INSERT INTO steam_developers (dev_name) VALUES (%s) ON CONFLICT (dev_name) DO UPDATE SET dev_name=EXCLUDED.dev_name RETURNING dev_id",
                            (name,)
                        )
                        devs_cache[name] = cur.fetchone()[0]
                    game_devs.append((appid, devs_cache[name], role))

    conn.commit()

    if games:
        execute_values(cur, """
            INSERT INTO steam_games (
                appid, name, type, is_free, short_desc,
                price_initial, price_final, currency,
                metacritic, recommendations, release_date,
                platforms_win, platforms_mac, platforms_linux,
                positive_reviews, negative_reviews
            ) VALUES %s
            ON CONFLICT (appid) DO UPDATE SET
                name = EXCLUDED.name,
                price_final = EXCLUDED.price_final,
                metacritic = EXCLUDED.metacritic,
                recommendations = EXCLUDED.recommendations,
                loaded_at = NOW()
        """, games)
        conn.commit()

    if game_genres:
        execute_values(cur, "INSERT INTO steam_game_genres (appid, genre_id) VALUES %s ON CONFLICT DO NOTHING", list(set(game_genres)))
        conn.commit()

    if game_devs:
        execute_values(cur, "INSERT INTO steam_game_devs (appid, dev_id, role) VALUES %s ON CONFLICT DO NOTHING", list(set(game_devs)))
        conn.commit()

    cur.close()
    conn.close()

    print(f"Loaded {len(games)} games, {len(set(game_genres))} genre links, {len(set(game_devs))} dev links")
    return len(games)


def load_player_counts(**context):
    client = get_minio()
    date = context["ds"]

    data = read_json(client, f"steam/player_counts/{date}/player_counts.json")
    if not data:
        return 0

    rows = [
        (int(appid), date, result.get("response", {}).get("player_count"))
        for appid, result in data.items()
        if result.get("response", {}).get("player_count") is not None
    ]

    if not rows:
        return 0

    conn = get_db()
    cur = conn.cursor()
    execute_values(cur, """
        INSERT INTO steam_player_counts (appid, snapshot_date, player_count)
        VALUES %s
        ON CONFLICT (appid, snapshot_date) DO UPDATE SET
            player_count = EXCLUDED.player_count,
            loaded_at = NOW()
    """, rows)
    conn.commit()
    cur.close()
    conn.close()

    print(f"Loaded {len(rows)} player count snapshots")
    return len(rows)


default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}

with DAG(
    dag_id="silver_steam_load",
    default_args=default_args,
    description="Load cleaned Steam data from MinIO into PostgreSQL",
    schedule_interval=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["silver", "steam"],
) as dag:

    t1 = PythonOperator(task_id="create_tables", python_callable=create_tables)
    t2 = PythonOperator(task_id="load_games", python_callable=load_games)
    t3 = PythonOperator(task_id="load_player_counts", python_callable=load_player_counts)

    t1 >> [t2, t3]
