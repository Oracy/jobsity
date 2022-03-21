insert into
    trips_by_area_ldw (region, weekofyear, count)
select
    trips_by_area_stg.region,
    trips_by_area_stg.weekofyear,
    trips_by_area_stg."count"
from
    trips_by_area_stg
    left join trips_by_area_ldw on trips_by_area_stg.region = trips_by_area_ldw.region
    and trips_by_area_stg.weekofyear = trips_by_area_ldw.weekofyear
    and trips_by_area_stg.count = trips_by_area_ldw.count
where
    trips_by_area_ldw.region is null
    and trips_by_area_ldw.weekofyear is null
    and trips_by_area_ldw.count is null
group by
    trips_by_area_stg.region,
    trips_by_area_stg.weekofyear,
    trips_by_area_stg."count";
