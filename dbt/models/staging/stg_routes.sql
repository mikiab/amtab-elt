with source as (
    select * from {{ source('amtab', 'routes') }}
)

select
    route_id,
    route_short_name,
    route_long_name,
    route_type
from source
