with flower_table as (
        select
          ds.store_id,
          ds.name,
          ds.business_id,
          ds.business_name,
          ds.submarket_id,
          ds.submarket_name,
          ds.country_id,
          ds.is_active
from proddb.tableau.new_verticals_stores nvs
join dimension_store as ds on ds.store_id = nvs.store_id
where 1=1
and nvs.business_vertical_id in (141)
and ds.country_id = 1

 union
  select distinct
   ds.store_id,
   ds.name,
   ds.business_id,
   ds.business_name,
   ds.submarket_id,
    ds.submarket_name,
    ds.country_id,
    ds.is_active
   from
    dimension_store as ds
   where
    1 = 1
    and ds.country_id = 1
    and ds.business_id in (749869)
    and ds.is_active = true)

, ly_cx as (select
  distinct creator_id
from
  dimension_deliveries a
  join flower_table b on a.store_id = b.store_id
where 1=1
    and is_filtered_core = true
  and active_date in ('02/12/2023', '02/13/2023', '02/14/2023')
  group by 1)


  , cx_reordered as (select
  distinct creator_id
from
  dimension_deliveries a
  join flower_table b on a.store_id = b.store_id
where 1=1
  and a.is_filtered_core = true
  and a.active_date in ('02/12/2024', '02/13/2024', '02/14/2024')
  and creator_id in (select creator_id from ly_cx))


  select
  count (distinct a.creator_id) as ly,
  count (distinct b.creator_id) as ty,
  ty / ly *100
  from ly_cx a
  join dimension_users du on  du.consumer_id = a.creator_id
  left join cx_reordered b on a.creator_id = b.creator_id
  where 1=1;


with flower_table as (
        select
          ds.store_id,
          ds.name,
          ds.business_id,
          ds.business_name,
          ds.submarket_id,
          ds.submarket_name,
          ds.country_id,
          ds.is_active
from proddb.tableau.new_verticals_stores nvs
join dimension_store as ds on ds.store_id = nvs.store_id
where 1=1
and nvs.business_vertical_id in (141)
and ds.country_id = 1

 union
  select distinct
   ds.store_id,
   ds.name,
   ds.business_id,
   ds.business_name,
   ds.submarket_id,
    ds.submarket_name,
    ds.country_id,
    ds.is_active
   from
    dimension_store as ds
   where
    1 = 1
    and ds.country_id = 1
    and ds.business_id in (749869)
    and ds.is_active = true)

, ly_cx as (select
  distinct creator_id
from
  dimension_deliveries a
  join flower_table b on a.store_id = b.store_id
where 1=1
    and is_filtered_core = true
  and active_date in ('02/12/2023', '02/13/2023', '02/14/2023')
  group by 1)

  , cx_reordered as (select
  distinct creator_id
from
  dimension_deliveries a
  join flower_table b on a.store_id = b.store_id
where 1=1
  and a.is_filtered_core = true
  and a.active_date in ('02/12/2024', '02/13/2024', '02/14/2024')
  and creator_id in (select creator_id from ly_cx))


SELECT
       CASE
           WHEN COALESCE(mh.days_since_last_purchase, -1) BETWEEN 0 AND 28 THEN 'Active'
           WHEN COALESCE(mh.days_since_last_purchase, -1) BETWEEN 29 AND 90 THEN 'Dormant'
           WHEN COALESCE(mh.days_since_last_purchase, -1) > 90 THEN 'Churned'
           ELSE 'New'
           END                                                                       AS mp_segment,
      count(distinct sub.CREATOR_ID)
FROM ly_cx sub
     LEFT JOIN mattheitz.mh_customer_authority mh
        ON mh.creator_id = sub.creator_id
        AND mh.dte = '2024-02-11'


GROUP BY 1;


with flower_table as (
        select
          ds.store_id,
          ds.name,
          ds.business_id,
          ds.business_name,
          ds.submarket_id,
          ds.submarket_name,
          ds.country_id,
          ds.is_active
from proddb.tableau.new_verticals_stores nvs
join dimension_store as ds on ds.store_id = nvs.store_id
where 1=1
and nvs.business_vertical_id in (141)
and ds.country_id = 1

 union
  select distinct
   ds.store_id,
   ds.name,
   ds.business_id,
   ds.business_name,
   ds.submarket_id,
    ds.submarket_name,
    ds.country_id,
    ds.is_active
   from
    dimension_store as ds
   where
    1 = 1
    and ds.country_id = 1
    and ds.business_id in (749869)
    and ds.is_active = true)

, ly_cx as (select
  distinct creator_id, a.IS_SUBSCRIBED_CONSUMER
from
  dimension_deliveries a
  join flower_table b on a.store_id = b.store_id
where 1=1
    and is_filtered_core = true
  and active_date  BETWEEN '2023-02-05' AND '2023-02-15' -- in ('02/12/2023', '02/13/2023', '02/14/2023')
  group by all)

, ty_cx as (select
  distinct creator_id, a.IS_SUBSCRIBED_CONSUMER
from
  dimension_deliveries a
  join flower_table b on a.store_id = b.store_id
where 1=1
    and is_filtered_core = true
  and active_date  BETWEEN '2024-02-05' AND '2024-02-15' -- in ('02/12/2023', '02/13/2023', '02/14/2023')
  group by all)

, ly_MDAY_cx as (select
  distinct creator_id, a.IS_SUBSCRIBED_CONSUMER
from
  dimension_deliveries a
  join flower_table b on a.store_id = b.store_id
where 1=1
    and is_filtered_core = true
  and active_date  BETWEEN '2023-05-08' AND '2023-05-15' -- in ('02/12/2023', '02/13/2023', '02/14/2023')
  group by all)


-- SELECT a.LIFESTAGE
-- , count(DISTINCT a.CREATOR_ID) as redeemers
-- , count(distinct case when ly_cx.CREATOR_ID is not null then a.CREATOR_ID end) as ly_orderers
-- , ly_orderers / redeemers
-- FROM dianedou.vday_2024_cx_level_performance a
-- left join ly_cx on a.CREATOR_ID = ly_cx.CREATOR_ID
-- GROUP BY  1
-- ;

SELECT a.LIFESTAGE
, count(DISTINCT a.CREATOR_ID) as redeemers
, count(distinct case when ly_cx.CREATOR_ID is not null then a.CREATOR_ID end) as ly_orderers
, ly_orderers / redeemers
FROM dianedou.vday_2024_cx_level_performance a
left join ly_mday_cx ly_cx on a.CREATOR_ID = ly_cx.CREATOR_ID
GROUP BY  1
;


-- SELECT 'TY' as period
-- , IS_SUBSCRIBED_CONSUMER
-- , COUNT(DISTINCT  CREATOR_ID)
--
-- FROM ty_cx
-- GROUP BY 1,2
--
-- union all
--
-- SELECT 'LY' as period
-- , IS_SUBSCRIBED_CONSUMER
-- , COUNT(DISTINCT  CREATOR_ID)
--
-- FROM ly_cx
-- GROUP BY 1,2

;

-- SELECT case when a.DP_CX_SAVINGS is not null then 1 else 0 end as DP_TY
-- , count(DISTINCT a.CREATOR_ID) as redeemers
-- , count(distinct case when ly_cx.CREATOR_ID is not null then a.CREATOR_ID end) as ly_orderers
-- , ly_orderers / redeemers
-- FROM dianedou.vday_2024_cx_level_performance a
-- left join ly_cx on a.CREATOR_ID = ly_cx.CREATOR_ID
-- GROUP BY  1;

select count(distinct CONSUMER_ID)
from edw.consumer.fact_consumer_subscription__daily__extended
where dte = '2024-02-05'--(select max(dte) from edw.consumer.fact_consumer_subscription__daily__extended)
    and ('2024-02-05'::DATE)  between start_time and end_time
and DYNAMIC_SUBSCRIPTION_STATUS in ('active_free_subscription','active_paid', 'active_trial','trial_waiting_for_payment','dtp_waiting_for_payment','paid_waiting_for_payment')
;


select  'LY' as period
, dd.IS_SUBSCRIBED_CONSUMER
, count(distinct CREATOR_ID)

from dimension_deliveries dd
where is_filtered_core = true
  and active_date  BETWEEN '2023-02-05' AND '2023-02-15'
group by 1,2

union  all

select  'TY' as period
, dd.IS_SUBSCRIBED_CONSUMER
, count(distinct CREATOR_ID)

from dimension_deliveries dd
where is_filtered_core = true
  and active_date  BETWEEN '2024-02-05' AND '2024-02-15'
group by 1,2
;


SELECT dd.IS_SUBSCRIBED_CONSUMER
, count(distinct a.DELIVERY_ID)
FROM dianedou.vday_2024_cx_level_performance a
left join dimension_deliveries dd on a.DELIVERY_ID = dd.DELIVERY_ID
GROUP BY  1;