with source as (
    select * from {{ source('amtab', 'stop_times') }}
)

select
    trip_id,
    stop_id,
    arrival_time as scheduled_arrival,
    departure_time as scheduled_departure,
    stop_sequence
from source
