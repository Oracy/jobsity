select
    distinct region
from
    trips_ldw
where
    datasource = 'cheap_mobile';
