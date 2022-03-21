CREATE TABLE IF NOT EXISTS trips_by_area_ldw (
    id SERIAL PRIMARY KEY,
    region VARCHAR(10) NULL,
    weekofyear INTEGER NULL,
    count INTEGER NULL
);
