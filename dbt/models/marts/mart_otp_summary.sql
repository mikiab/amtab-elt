{{
    config(
        materialized='incremental',
        partition_by={
            'field': 'dt',
            'data_type': 'date',
            'granularity': 'day'
        },
        cluster_by=['route_id'],
        incremental_strategy='insert_overwrite'
    )
}}

with trip_updates as (
    select *
    from {{ ref('int_trip_updates') }}
    {% if is_incremental() %}
        where dt > (select max(dt) from {{ this }})
    {% endif %}
),

stop_times as (
    select
        trip_id,
        stop_id,
        stop_sequence
    from {{ ref('stg_stop_times') }}
),

matched as (
    select
        tu.dt,
        tu.route_id,
        tu.arrival_delay,
        tu.arrival_delay between -1 and 5 as is_on_time
    from trip_updates tu
    inner join stop_times st
        on tu.trip_id = st.trip_id
        and tu.stop_id = st.stop_id
        and tu.stop_sequence = st.stop_sequence
)

select
    dt,
    route_id,
    count(*) as total_observations,
    countif(is_on_time) as on_time_count,
    round(countif(is_on_time) / count(*) * 100, 2) as otp_pct
from matched
group by dt, route_id
