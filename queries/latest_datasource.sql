with two_most_common as (
    select
        region,
        count(region) as qty
    from
        trips_ldw
    group by
        region
    order by
        qty desc
),
latest_datasource as (
    select
        trips_ldw.region,
        trips_ldw.datasource,
        max(trips_ldw.datetime) as latest,
        row_number() over (
            partition by trips_ldw.region
            order by
                max(trips_ldw.datetime) desc
        ) as rnk,
        two_most_common.qty
    from
        trips_ldw
        inner join two_most_common on trips_ldw.region = two_most_common.region
    group by
        trips_ldw.region,
        trips_ldw.datasource,
        two_most_common.qty
    order by
        latest desc
)
select
    region,
    datasource,
    latest,
    qty
from
    latest_datasource
where
    rnk = 1
limit
    1;