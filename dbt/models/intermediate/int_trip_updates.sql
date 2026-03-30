with deduplicated as (
    {{ dbt_utils.deduplicate(
        relation=ref('stg_trip_updates'),
        partition_by='trip_id, stop_id, stop_sequence, date(feed_at)',
        order_by='feed_at desc'
    ) }}
)

select
    entity_id,
    trip_id,
    route_id,
    stop_sequence,
    stop_id,
    arrival_delay,
    arrived_at,
    departure_delay,
    departed_at,
    trip_delay,
    feed_at,
    date(feed_at) as dt
from deduplicated
where arrival_delay is not null
