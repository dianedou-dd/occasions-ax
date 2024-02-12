SET snapshot_date = '2024-02-08'::DATE;

create or replace table DIANEDOU.superbowl_2024_mau_coverage as

with mau as (
    select distinct creator_id
    from dimension_deliveries dd
    where dd.is_filtered_core = true and dd.active_date between $snapshot_date -29 and $snapshot_date - 1 and country_id = 1
),

store_base as (
    select distinct
        ds.store_id,
        ds.business_name
    from dimension_store ds
    JOIN PRODDB.DIANEDOU.SUPERBOWL_2024_BUSINESS_ID id
    on ds.STORE_ID = id.STORE_ID

)
, availability as (
    select
          m.creator_id
        , count(distinct s.business_name) as offers
        , count(distinct s.store_id) as stores
    from fact_store_availability fsa
    join mau m
        on fsa.consumer_id::varchar = m.creator_id::varchar
    join store_base s
        on fsa.store_id::varchar = s.store_id::varchar
    group by 1
)

    select
        offers
      , count(distinct creator_id) as MAUs
    from availability
    group by 1
    order by 1
;



create or replace table DIANEDOU.superbowl_2024_mau_coverage_perc  as
with ttl_mau as (
    select distinct creator_id
    from dimension_deliveries dd
    where dd.is_filtered_core = true
      and dd.active_date between $snapshot_date -29 and $snapshot_date - 1
      and country_id = 1
)

select base.*
, base.MAUs / (select count (distinct CREATOR_ID) from ttl_mau) as coverage_perc

from DIANEDOU.superbowl_2024_mau_coverage base
order by 1;

select * from DIANEDOU.superbowl_2024_mau_coverage_perc;

GRANT SELECT ON TABLE dianedou.superbowl_2024_mau_coverage_perc TO read_only_users;