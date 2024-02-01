SET snapshot_date = '2023-12-01'::DATE;

create or replace table DIANEDOU.dtd_2023_mau_coverage as

with mau as (
    select distinct creator_id
    from dimension_deliveries dd
    where dd.is_filtered_core = true and dd.active_date between $snapshot_date -29 and $snapshot_date - 1 and country_id = 1
),

store_base as (
    select distinct
        store_id,
        business_name
    from dimension_store
    where business_id in ('11626333','331358','11396919','799015','412816','11205715','11630312','434024','8176','627594','7376','1162','105','4477','5253','839695','421977','493867','839694','586343','351380','10171','564','1857',
'434628','4815','12284','479193','43285','364841','479186','292028')
    union all
    select distinct
        s.store_id,
        cpg.business_name
    from xiwenzhang.dtd_cpg cpg
    left join dimension_store s on cpg.business_id = s.business_id
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

GRANT SELECT ON TABLE dianedou.dtd_2023_mau_coverage TO read_only_users;

create or replace table static.dtd_2023_mau_coverage_perc as
with ttl_mau as (
    select distinct creator_id
    from dimension_deliveries dd
    where dd.is_filtered_core = true
      and dd.active_date between $snapshot_date -29 and $snapshot_date - 1
      and country_id = 1
)

select base.*
, base.MAUs / (select count (distinct CREATOR_ID) from ttl_mau) as coverage_perc

from DIANEDOU.dtd_2023_mau_coverage base
order by 1;

