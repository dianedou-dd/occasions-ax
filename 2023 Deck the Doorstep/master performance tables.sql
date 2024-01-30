-- create or replace table yvonneliu.dtd_2023_master_campaign_list as
CREATE OR REPLACE TABLE DIANEDOU.dtd_2023_master_campaign_list AS
WITH campaigns AS (
---- evergreen reskinned
---- campaign list: https://docs.google.com/spreadsheets/d/18qRkS5F3fhaaYcrM_AQSW9IFMq3Lnh7lbJWrALBYgHA/edit#gid=191777592
---- snowflake: https://app.snowflake.com/doordash/doordash/w2woj9p1yXSg#query

    SELECT DISTINCT a.store_id
                  , a.campaign_id
-- , 'NV' as vertical
    FROM yvonneliu.evergreen_reskin_campaign_id_list_dtd_2023 a
    UNION
    ----- adops
----- https://app.snowflake.com/doordash/doordash/w4jxdTLMoWZs#query
----- Campaign list: https://docs.google.com/spreadsheets/d/1TKl_DW3bNE3wl6dKAZk7PXRiKvANbLvAzXivRIiYtNs/edit#gid=1326696068

    SELECT DISTINCT a.store_id
                  , a.campaign_id
    -- , 'Rx' as vertical
-- , 'Adops' as source
    FROM yvonneliu.adops_campaign_id_list_dtd_2023 a


    UNION
    ----- non adops
-- https://app.snowflake.com/doordash/doordash/w1rrdHs0OKAs#query
-- store ids for 7bbc657c-a4ab-412a-b8e6-3176e8288cbf campaign id
-- campaign list: https://docs.google.com/spreadsheets/d/1TKl_DW3bNE3wl6dKAZk7PXRiKvANbLvAzXivRIiYtNs/edit#gid=1414080216

    SELECT DISTINCT a.store_id
                  , a.campaign_id
    -- , 'Rx' as vertical
-- , 'Non-Adops' as source
    FROM yvonneliu.nonadops_campaign_id_list_dtd_2023 a

    UNION

    ---- nv ad ops: provided by Kelly, Kristena
---- https://app.snowflake.com/doordash/doordash/w3QZNaetVBox#query
---- campaign list: https://docs.google.com/spreadsheets/d/1stYFgFtcfKK4IozvAKX5LcwMZoP1H2ZT/edit#gid=2018481971 tab: Deck the Doorstep (Kristena)
    SELECT DISTINCT a.store_id
                  , a.campaign_id
    -- , 'NV' as vertical
-- , 'Kristena' as source
    FROM yvonneliu.kristena_campaign_id_list_dtd_2023 a

    UNION
    ---- nv ad ops: provided by Kelly, Claire, Fei, and Ailia
---- https://app.snowflake.com/doordash/doordash/w2TwPhkgS5RJ#query
---- campaign list: https://docs.google.com/spreadsheets/d/1stYFgFtcfKK4IozvAKX5LcwMZoP1H2ZT/edit#gid=1960438875
    SELECT DISTINCT a.store_id
                  , a.campaign_id
    -- , 'NV' as vertical
-- , 'AlcBBY' as source
    FROM yvonneliu.alcbby_campaign_id_list_dtd_2023 a

    UNION

    SELECT DISTINCT a.store_id
                  , a.campaign_id
-- , 'NV' as vertical
    FROM static.dtd_wes_campaign_list a
    WHERE last_updated::DATE = '2023-12-12'::DATE
    --(select max(last_updated) from static.dtd_wes_campaign_list)


-- -- ///// Featured Offers (All Verticals) ///// --
-- -- https://app.snowflake.com/doordash/doordash/w323ygDfDhto#query
-- union

-- select distinct
--       a.store_id
--     , a.campaign_id
-- , 'Rx' as vertical
-- -- , 'Wes -- Featured Offers' as source
--   from static.dtd_featured_offers_campaign_list a
-- union
-- -- ///// Wes: Local (Rx) ///// --
-- -- https://app.snowflake.com/doordash/doordash/w1iKVjkqvdW9#query
-- select distinct
--       a.store_id
--     , a.campaign_id
-- , 'Rx' as vertical
-- -- , 'Wes -- Local Rx' as source
--   from static.dtd_local_rx_campaign_list a
-- union
-- -- ///// Wes: National Rx ///// --
-- -- https://app.snowflake.com/doordash/doordash/w4NF80snk0TS#query row 204
-- select distinct
--       a.store_id
--     , a.campaign_id
-- , 'Rx' as vertical
-- -- , 'Wes -- National Rx' as source
--   from static.dtd_national_rx_campaign_list a
    UNION
--- ad ops 91d63b91-8bb9-4560-8894-5fb356caab9d taco bell campaign
    SELECT DISTINCT a.store_id
                  , a.campaign_id
    FROM yvonneliu.coke_tacobell_20231211 a

    UNION
    --- ad ops 033e577a-9740-4d15-ad5e-152ab50f7af2 coke campaign
-- https://app.snowflake.com/doordash/doordash/w3lQip2yMRWV#query
    SELECT DISTINCT a.store_id
                  , a.campaign_id
    FROM yvonneliu.coke_7af2_campaign_id a

    UNION
    SELECT DISTINCT a.store_id
                  , a.campaign_id
    FROM yvonneliu.dtd_additional_store_ids_20231218 a)

SELECT *

FROM campaigns

UNION ALL

SELECT NULL AS store_id, mixed_list.VALUE AS campaign_id
FROM TABLE (FLATTEN(INPUT => ARRAY_CONSTRUCT(
    ---- milestones offer $400K budget
        '3f22e607-4065-43a3-bd24-3ee9cb3b87a9',
        '207074d9-0bde-4609-a994-c51d88b7fc21',
        'aa3df3d2-6143-46f1-86e2-ab9e7e24856b',
        'f5814fd1-a49f-4c55-8460-bd55f3cbdee8',
    ---- annual plan

    ---- sign up sale
    ---- tab: https://app.mode.com/editor/doordash/reports/3d1a26326b24/queries/8712183de9b0 (Daily Level: Agg OR & Spend)
        '1b6bb333-cc5d-496a-8d3e-502c161dcda2',
        '4ee36518-9b97-4e21-861b-ff99a3d3f4ca',
        '891c6547-03e4-4ca1-a812-ee86ed8d97e8'))) AS mixed_list
;


CREATE OR REPLACE TABLE DIANEDOU.dtd_2023_master_campaign_list_nv AS
SELECT DISTINCT c.*
              , CASE
                    WHEN nv.is_filtered_mp_vertical = 1 AND nv.org_id = 1 THEN 'CG&A'
                    WHEN nv.vertical_name = '1P Convenience' THEN 'DashMart'
                    WHEN nv.business_vertical_id IN (265, 266, 267, 331, 332, 333, 334, 364, 430, 10000794)
                        THEN 'Retail'
                    WHEN nv.business_vertical_id IN (139, 169) THEN 'Pet' ---- 20231218 addition
                    WHEN (nv.business_vertical_id IN (141) OR nv.business_id = 749869)
                        THEN 'Flowers' ---- 20231218 addition
                    ELSE 'Other' END  AS vertical
              , nv.vertical_name
              , nv.business_line
              , nv.org
              -- when cp.store_id is not null then 'CPG' else 'Other' end as vertical
              , CASE
                    WHEN c.campaign_id IN (
                        ---- milestones offer $400K budget
                                           '3f22e607-4065-43a3-bd24-3ee9cb3b87a9',
                                           '207074d9-0bde-4609-a994-c51d88b7fc21',
                                           'aa3df3d2-6143-46f1-86e2-ab9e7e24856b',
                                           'f5814fd1-a49f-4c55-8460-bd55f3cbdee8',
                        ---- annual plan

                        ---- sign up sale
                        ---- tab: https://app.mode.com/editor/doordash/reports/3d1a26326b24/queries/8712183de9b0 (Daily Level: Agg OR & Spend)
                                           '1b6bb333-cc5d-496a-8d3e-502c161dcda2',
                                           '4ee36518-9b97-4e21-861b-ff99a3d3f4ca',
                                           '891c6547-03e4-4ca1-a812-ee86ed8d97e8') THEN 'Mixed ENT/SMB'
                    WHEN sg.cohort = 'SMB' THEN 'SMB'
                    WHEN cohort = 'Ent/MM' AND vertical NOT IN ('Other') THEN 'NV'
                    ELSE 'ENT Rx' END AS adjusted_cohort
              , ds.name               AS store_name
              , ds.business_id
              , ds.business_name
FROM DIANEDOU.dtd_2023_master_campaign_list c
     LEFT JOIN (SELECT DISTINCT store_id, cohort FROM public.fact_ads_promo_store_categorization) sg
               ON sg.store_id = c.store_id
     LEFT JOIN edw.cng.dimension_new_vertical_store_tags nv
               ON nv.is_filtered_mp_vertical = 1 AND c.store_id = nv.store_id
     LEFT JOIN dimension_store ds ON c.store_id = ds.store_id
;
;
CREATE OR REPLACE TABLE DIANEDOU.dtd_2023_cx_segment AS
WITH dtd_redeemers AS (SELECT DISTINCT creator_id,
                                       MIN(active_date) AS first_redemption_date,
                                       count(distinct delivery_id) as redemptions
                       FROM fact_order_discounts_and_promotions_extended p
                            INNER JOIN DIANEDOU.dtd_2023_master_campaign_list_nv s
                                       ON p.campaign_id = s.campaign_id --AND p.store_id = s.store_id
                       WHERE active_date BETWEEN '2023-12-01'::DATE AND '2023-12-12'::DATE
                       GROUP BY 1)

   , prior_deliveries AS (SELECT dtd.creator_id
                               , redemptions
                               , dtd.first_redemption_date
                               , MAX(dd.active_date)                                           AS last_order_date_prior
                               , MIN(dd.active_date)                                           AS first_order_date_prior
                               , DATEDIFF('day', last_order_date_prior, first_redemption_date) AS date_difference
                          FROM dimension_deliveries dd
                               JOIN dtd_redeemers dtd
                                    ON dd.CREATOR_ID = dtd.CREATOR_ID AND dd.active_date < dtd.first_redemption_date
                                        AND is_filtered_core = 1
                                        AND is_caviar = 0
                          GROUP BY 1, 2, 3)

SELECT distinct pd.creator_id
     , first_order_date_prior
     , last_order_date_prior
     , first_redemption_date
     , redemptions
     , CASE
           WHEN pd.first_order_date_prior BETWEEN DATEADD(DAY, -29, DATEADD(DAY, -1, pd.first_redemption_date)) AND DATEADD(DAY, -1, first_redemption_date)
               THEN 'New'
           WHEN date_difference <= 28 THEN 'Active'
           WHEN date_difference <= 90 THEN 'Dormant'
           WHEN date_difference <= 180 THEN 'Churn'
           ELSE 'Very churn' END AS lifestage

FROM prior_deliveries pd
;


create or replace table dianedou.dtd_2023_cx_level_performance  as
with dp_deliveries as (
  select
    fodp.business_id,
    fodp.business_name,
    campaign_id,
    CAMPAIGN_OR_PROMO_NAME,
    fodp.delivery_id,
    fodp.ACTIVE_DATE,
    DISCOUNTS_PROMOTION_SERVICE_SOURCED_AMOUNT,
    DISCOUNT_GROUP_MERCHANT_PROMOTION_COMPONENT_AMOUNT
  from fact_order_discounts_and_promotions fodp
  inner join PRODDB.PUBLIC.DIMENSION_DELIVERIES dd on dd.delivery_id = fodp.DELIVERY_ID
  where IS_FILTERED_CORE = true
    and IS_SUBSCRIBED_CONSUMER = true
    and dd.active_date >= '2023-11-24'::date
)

-- , promo_data as (
    select distinct
        dd.active_date
      , c.week as campaign_week
      , u.creator_id
      , u.lifestage
      , b.vertical
      , b.campaign_id
      , b.adjusted_cohort
      , dd.delivery_id
      , b.store_id
      , b.vertical_name
      , b.business_line
      , b.org
      , b.store_name
      , b.business_id
      , b.business_name
      , ifnull(sum(ifnull(promotion_fee,0)/100),0) as mx_funded_promo_dollars
      , ifnull(sum(ifnull(consumer_discount,0)/100),0) as all_cx_discount
      , ifnull(sum(case when ifnull(promotion_fee,0) > 0 and ifnull(promotion_fee,0) < ifnull(consumer_discount,0) then ifnull(promotion_fee,0)/100
                when ifnull(promotion_fee,0) > 0 and ifnull(promotion_fee,0) >= ifnull(consumer_discount,0) then ifnull(consumer_discount,0)/100 end),0) as mx_funded_cx_discount
      , ifnull(sum(case when ifnull(promotion_fee,0) > 0 and ifnull(promotion_fee,0) > ifnull(consumer_discount,0) then (ifnull(promotion_fee,0) - ifnull(consumer_discount,0))/100 end),0) as mx_marketing_fee
      , ifnull(sum(ifnull(dd.subtotal,0)/100),0) as dd_subtotals
      , ifnull(sum(case when ifnull(dd.promotion_fee,0) > 0 then ifnull(dd.subtotal,0)/100 end),0) as mx_funded_promo_subtotals
      , count(distinct dd.delivery_id) as num_redemptions
      , count(distinct dd.consumer_id) as num_redeemers
      -- , count(distinct dd.delivery_id) / count(distinct dd.consumer_id) as redemp_per_cx
      , ifnull(sum(ifnull(DISCOUNTS_PROMOTION_SERVICE_SOURCED_AMOUNT,0) / 100),0) as dp_cx_savings
    from DIANEDOU.dtd_2023_cx_segment u
    left join edw.invoice.fact_promotion_deliveries dd on u.creator_id = dd.consumer_id
    join DIANEDOU.dtd_2023_master_campaign_list_nv b
      on true
--         and b.store_id = dd.store_id
        and b.campaign_id::varchar = dd.campaign_id::varchar
    left join static.dtd_2023_calendar c
      on dd.active_date = c.active_date
    left join dp_deliveries d
      on dd.delivery_id = d.delivery_id
    where dd.active_date >= '2023-11-24'::date
      and consumer_discount > 0
    group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15
    ;