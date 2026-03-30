with source as (
    select * from {{ source('amtab', 'trip_updates') }}
),

route_mapping as (
    select * from {{ ref('route_id_mapping') }}
),

cleaned as (
    select
        s.entity_id,
        s.trip_id,
        coalesce(m.route_id_static, s.route_id) as route_id,
        s.stop_sequence,
        s.stop_id,
        s.arrival_delay,
        timestamp_seconds(s.arrival_time) as arrived_at,
        s.departure_delay,
        timestamp_seconds(s.departure_time) as departed_at,
        s.trip_delay,
        timestamp_seconds(s.feed_timestamp) as feed_at
    from source s
    left join route_mapping m
        on s.route_id = m.route_id_rt
)

select * from cleaned
