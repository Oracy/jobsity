insert into
    trips_ldw (
        region,
        datetime,
        datasource,
        lat_origin,
        long_origin,
        lat_destination,
        long_destination
    )
select
    trips_stg.region,
    trips_stg.datetime,
    trips_stg.datasource,
    trips_stg.lat_origin,
    trips_stg.long_origin,
    trips_stg.lat_destination,
    trips_stg.long_destination
FROM
    public.trips_stg
    left join public.trips_ldw on trips_stg.datetime = trips_ldw.datetime
    and trips_stg.lat_origin = trips_ldw.lat_origin
    and trips_stg.long_origin = trips_ldw.long_origin
    and trips_stg.lat_destination = trips_ldw.lat_destination
    and trips_stg.long_destination = trips_ldw.long_destination
where
    trips_ldw.datetime is NULL
    and trips_ldw.lat_origin is NULL
    and trips_ldw.long_origin is NULL
    and trips_ldw.lat_destination is NULL
    and trips_ldw.long_destination is NULL
group by
    trips_stg.region,
    trips_stg.datetime,
	trips_stg.datasource,
	trips_stg.lat_origin,
	trips_stg.long_origin,
	trips_stg.lat_destination,
	trips_stg.long_destination;
