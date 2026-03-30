with source as (
    select * from {{ source('amtab', 'vehicle_position') }}
),

route_mapping as (
    select * from {{ ref('route_id_mapping') }}
),

cleaned as (
    select
        s.entity_id,
        s.trip_id,
        coalesce(m.route_id_static, s.route_id) as route_id,
        s.vehicle_id,
        s.vehicle_label,
        s.latitude,
        s.longitude,
        s.speed,
        s.current_stop_sequence,
        s.stop_id,
        timestamp_seconds(s.vehicle_timestamp) as recorded_at,
        timestamp_seconds(s.feed_timestamp) as feed_at
    from source s
    left join route_mapping m
        on s.route_id = m.route_id_rt
)

select * from cleaned
