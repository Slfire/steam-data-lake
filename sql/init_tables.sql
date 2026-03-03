-- Steam Data Lake - Silver layer tables
-- Run this once before starting the pipeline

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
