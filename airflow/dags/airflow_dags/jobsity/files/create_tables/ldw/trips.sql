CREATE TABLE IF NOT EXISTS trips_ldw (
    id SERIAL PRIMARY KEY,
    region VARCHAR(10) NULL,
    "datetime" TIMESTAMP NULL,
    datasource VARCHAR(100) NULL,
    lat_origin VARCHAR(100) NULL,
    long_origin VARCHAR(100) NULL,
    lat_destination VARCHAR(100) NULL,
    long_destination VARCHAR(100) NULL
);
