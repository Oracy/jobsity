DROP TABLE IF EXISTS trips_stg;
CREATE TABLE IF NOT EXISTS trips_stg (
    id SERIAL PRIMARY KEY,
    region VARCHAR(10) NULL,
    "datetime" TIMESTAMP NULL,
    datasource VARCHAR(100) NULL,
    lat_origin VARCHAR(100) NULL,
    long_origin VARCHAR(100) NULL,
    lat_destination VARCHAR(100) NULL,
    long_destination VARCHAR(100) NULL
);
