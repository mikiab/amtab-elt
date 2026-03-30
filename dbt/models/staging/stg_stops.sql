with source as (
    select * from {{ source('amtab', 'stops') }}
)

select
    stop_id,
    stop_name,
    cast(stop_lat as float64) as stop_lat,
    cast(stop_lon as float64) as stop_lon
from source
