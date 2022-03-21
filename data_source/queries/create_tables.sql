CREATE TABLE IF NOT EXISTS million_df (
    id SERIAL PRIMARY KEY,
    region VARCHAR(10) NULL,
    origin_coord VARCHAR(100) NULL,
    destination_coord VARCHAR(100) NULL,
    `datetime` TIMESTAMP NULL,
    datasource VARCHAR(100) NULL
);

CREATE TABLE IF NOT EXISTS trips (
    id SERIAL PRIMARY KEY,
    region VARCHAR(10) NULL,
    origin_coord VARCHAR(100) NULL,
    destination_coord VARCHAR(100) NULL,
    `datetime` TIMESTAMP NULL,
    datasource VARCHAR(100) NULL
);

CREATE TABLE IF NOT EXISTS trips_by_area (
    id SERIAL PRIMARY KEY,
    region VARCHAR(10) NULL,
    weekofyear INTEGER NULL,
    `count` INTEGER NULL
);
