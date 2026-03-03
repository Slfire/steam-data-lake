from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime, timedelta
import requests
import json
import os
import io
import time
from minio import Minio


STEAM_API_KEY = os.environ.get("STEAM_API_KEY", "")
MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "minio:9000")
MINIO_ACCESS = os.environ.get("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET = os.environ.get("MINIO_SECRET_KEY", "minioadmin123")
BUCKET = "bronze"
TARGET_GAMES = 1000

session = requests.Session()
session.headers.update({"User-Agent": "Mozilla/5.0"})


def get_minio_client():
    return Minio(MINIO_ENDPOINT, access_key=MINIO_ACCESS, secret_key=MINIO_SECRET, secure=False)


def upload_json(client, path, data):
    if not client.bucket_exists(BUCKET):
        client.make_bucket(BUCKET)
    content = json.dumps(data, ensure_ascii=False).encode("utf-8")
    client.put_object(BUCKET, path, io.BytesIO(content), length=len(content), content_type="application/json")


def safe_request(url, params=None, retries=3):
    for attempt in range(retries):
        try:
            r = session.get(url, params=params, timeout=15)
            if r.status_code == 429:
                time.sleep(30)
                continue
            return r
        except Exception:
            if attempt < retries - 1:
                time.sleep(2 * (attempt + 1))
    return None


def fetch_top_games(**context):
    client = get_minio_client()
    date = context["ds"]

    r = safe_request(
        "https://api.steampowered.com/ISteamChartsService/GetMostPlayedGames/v1/",
        params={"key": STEAM_API_KEY}
    )

    data = r.json() if r else {}
    upload_json(client, f"steam/top_games/{date}/most_played.json", data)

    appids = [g["appid"] for g in data.get("response", {}).get("ranks", [])]
    upload_json(client, f"steam/top_games/{date}/appids.json", {"appids": appids})

    print(f"Fetched {len(appids)} top games")
    return appids


def fetch_app_list(**context):
    client = get_minio_client()
    date = context["ds"]

    r = safe_request(
        "https://api.steampowered.com/IStoreService/GetAppList/v1/",
        params={
            "key": STEAM_API_KEY,
            "max_results": 50000,
            "include_games": 1,
            "include_dlc": 0,
            "include_software": 0,
        }
    )

    data = r.json() if r else {}
    upload_json(client, f"steam/app_list/{date}/app_list.json", data)

    total = len(data.get("response", {}).get("apps", []))
    print(f"App list fetched: {total} entries")
    return total


def fetch_game_details(**context):
    client = get_minio_client()
    date = context["ds"]

    # top games from previous task
    try:
        resp = client.get_object(BUCKET, f"steam/top_games/{date}/appids.json")
        top_appids = json.loads(resp.read()).get("appids", [])
    except Exception:
        top_appids = []

    # curated list covering all major genres
    curated = [
        730, 440, 1172470, 578080, 252490, 1506830, 377160, 359550, 346110,
        570, 39210, 48240, 286160, 8930, 289070, 410900,
        1086940, 1245620, 814380, 1091500, 1174180, 1145360, 230230, 374320,
        292030, 306130, 582010, 1240440, 1658290, 2050650, 1716740,
        108600, 413150, 4000, 322330, 304930, 381210, 1167630,
        1551360, 255710, 206190, 244210, 222900, 1644840, 294100,
        218620, 304050, 1085660, 435150,
        239030, 311690, 418370, 601430, 753640, 976730, 1560030,
        367520, 106000, 219890, 387290, 648800, 728880, 774171,
        1313140, 1675200, 381730, 1260320, 1817190,
        945360, 1238080, 1517290, 1599340, 221100,
        1623730, 2246340, 1966720, 2075620, 1382330, 1818750,
        220, 240, 300, 320, 340, 380, 420, 460, 500, 550,
        10, 20, 30, 40, 50, 60, 70, 80, 130,
        646570, 1203220, 457140, 620, 400,
        1649240, 1282730, 1341820, 1237320, 1659420,
        1422810, 1493610, 1274900, 1538940, 1781750,
    ]

    # pull some appids from catalog for variety
    try:
        resp = client.get_object(BUCKET, f"steam/app_list/{date}/app_list.json")
        catalog = json.loads(resp.read())
        all_catalog = [a["appid"] for a in catalog.get("response", {}).get("apps", [])]
        step = max(1, len(all_catalog) // 2000)
        catalog_sample = all_catalog[::step][:2000]
    except Exception:
        catalog_sample = []

    candidates = list(dict.fromkeys(top_appids + curated + catalog_sample))
    print(f"{len(candidates)} candidate appids, targeting {TARGET_GAMES} valid games")

    stored = 0
    skipped = 0
    failed = 0

    for i, appid in enumerate(candidates):
        if stored >= TARGET_GAMES:
            break

        # skip if already cached
        try:
            client.stat_object(BUCKET, f"steam/game_details/{date}/game_{appid}.json")
            stored += 1
            skipped += 1
            continue
        except Exception:
            pass

        r = safe_request(
            "https://store.steampowered.com/api/appdetails",
            params={"appids": appid, "key": STEAM_API_KEY, "l": "english", "cc": "us"}
        )

        if r and r.status_code == 200:
            try:
                data = r.json()
                game = data.get(str(appid), {})
                if game.get("success") and game.get("data", {}).get("type") == "game":
                    upload_json(client, f"steam/game_details/{date}/game_{appid}.json", data)
                    stored += 1
                else:
                    failed += 1
            except Exception:
                failed += 1
        else:
            failed += 1

        if (i + 1) % 20 == 0:
            print(f"Progress: {i+1} checked, {stored} stored ({skipped} cached), {failed} skipped")
            time.sleep(2)
        else:
            time.sleep(0.5)

    print(f"Done: {stored} games stored ({skipped} from cache), {failed} non-games/errors")
    return stored


def fetch_player_counts(**context):
    client = get_minio_client()
    date = context["ds"]

    try:
        resp = client.get_object(BUCKET, f"steam/top_games/{date}/appids.json")
        appids = json.loads(resp.read()).get("appids", [])
    except Exception:
        appids = [730, 570, 440, 1172470, 1086940, 252490]

    results = {}
    for appid in appids[:150]:
        r = safe_request(
            "https://api.steampowered.com/ISteamUserStats/GetNumberOfCurrentPlayers/v1/",
            params={"appid": appid, "key": STEAM_API_KEY}
        )
        if r:
            results[str(appid)] = r.json()
        time.sleep(0.3)

    upload_json(client, f"steam/player_counts/{date}/player_counts.json", results)
    print(f"Player counts stored for {len(results)} games")
    return len(results)


default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
    "email_on_failure": False,
}

with DAG(
    dag_id="bronze_steam_ingestion",
    default_args=default_args,
    description="Fetch raw Steam data and store as JSON in MinIO",
    schedule_interval="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["bronze", "steam"],
) as dag:

    t1 = PythonOperator(task_id="fetch_top_games", python_callable=fetch_top_games)
    t2 = PythonOperator(task_id="fetch_app_list", python_callable=fetch_app_list)
    t3 = PythonOperator(task_id="fetch_game_details", python_callable=fetch_game_details)
    t4 = PythonOperator(task_id="fetch_player_counts", python_callable=fetch_player_counts)
    t5 = TriggerDagRunOperator(
        task_id="trigger_silver",
        trigger_dag_id="silver_steam_load",
        wait_for_completion=False,
    )

    t1 >> [t2, t3, t4] >> t5
