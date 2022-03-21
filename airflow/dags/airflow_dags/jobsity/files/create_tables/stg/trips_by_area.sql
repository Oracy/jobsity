DROP TABLE IF EXISTS trips_by_area_stg;
CREATE TABLE IF NOT EXISTS trips_by_area_stg (
    id SERIAL PRIMARY KEY,
    region VARCHAR(10) NULL,
    weekofyear INTEGER NULL,
    count INTEGER NULL
);
