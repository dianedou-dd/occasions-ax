SET campaign_start = '2023-12-01'::DATE;
SET campaign_end = '2023-12-12'::DATE;

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

select *
from dianedou.dtd_2023_mixed_campaign_list
;

CREATE OR REPLACE TABLE dianedou.dtd_2023_mixed_campaign_list AS
SELECT DISTINCT store_id, CAMPAIGN_ID
FROM edw.invoice.fact_promotion_deliveries dd
     JOIN TABLE (FLATTEN(INPUT => ARRAY_CONSTRUCT(
    ---- milestones offer $400K budget
        '3f22e607-4065-43a3-bd24-3ee9cb3b87a9',
        '207074d9-0bde-4609-a994-c51d88b7fc21',
        'aa3df3d2-6143-46f1-86e2-ab9e7e24856b',
        'f5814fd1-a49f-4c55-8460-bd55f3cbdee8',
    ---- annual plan
        '7488bf83-d93e-4bc3-a366-30dc7ff3a1e7',
    ---- sign up sale
    ---- tab: https://app.mode.com/editor/doordash/reports/3d1a26326b24/queries/8712183de9b0 (Daily Level: Agg OR & Spend)
        '1b6bb333-cc5d-496a-8d3e-502c161dcda2',
        '4ee36518-9b97-4e21-861b-ff99a3d3f4ca',
        '891c6547-03e4-4ca1-a812-ee86ed8d97e8'))) AS mixed_list
        ON dd.active_date BETWEEN $campaign_start AND $campaign_end
        AND dd.campaign_id::VARCHAR = mixed_list.VALUE::VARCHAR
;



CREATE OR REPLACE TABLE DIANEDOU.dtd_2023_cx_segment AS
WITH dtd_redeemers AS (SELECT DISTINCT creator_id,
                                       MIN(active_date)            AS first_redemption_date,
                                       COUNT(DISTINCT delivery_id) AS redemptions
                       FROM fact_order_discounts_and_promotions_extended p
                            INNER JOIN (SELECT DISTINCT campaign_id FROM DIANEDOU.dtd_2023_master_campaign_list) s
                               ON p.campaign_id = s.campaign_id --AND p.store_id = s.store_id
                       WHERE active_date BETWEEN $campaign_start AND $campaign_end
                       GROUP BY 1)

   , prior_deliveries AS (SELECT dtd.creator_id
                               , redemptions
                               , dtd.first_redemption_date
                               , MIN(CASE WHEN IS_FIRST_ORDERCART = TRUE THEN dd.active_date END) AS first_order_date_prior
                               , MAX(dd.active_date)                                              AS last_order_date_prior
--                                , DATEDIFF('day', last_order_date_prior, first_redemption_date) AS date_difference
                               , DATEDIFF('day', last_order_date_prior, $campaign_start)          AS date_difference
                          FROM dimension_deliveries dd
                               JOIN dtd_redeemers dtd
                                  ON dd.CREATOR_ID = dtd.CREATOR_ID AND
                                     dd.active_date < $campaign_start --dtd.first_redemption_date
                              AND is_filtered_core = 1
                              AND is_caviar = 0
                          GROUP BY 1, 2, 3)

SELECT DISTINCT pd.creator_id
              , first_order_date_prior
              , last_order_date_prior
              , first_redemption_date
              , redemptions
              , CASE
    --                     WHEN pd.first_order_date_prior BETWEEN DATEADD(DAY, -29, DATEADD(DAY, -1, pd.first_redemption_date))
--                         AND DATEADD(DAY, -1, first_redemption_date)
--                         THEN 'New'
                    WHEN pd.first_order_date_prior BETWEEN DATEADD(DAY, -29, DATEADD(DAY, -1, $campaign_start))
                        AND DATEADD(DAY, -1, $campaign_start)
                        THEN 'New'
                    WHEN (first_order_date_prior IS NULL) OR (last_order_date_prior IS NULL)
                        THEN 'activated during campaign' --also considered 'New'
                    WHEN date_difference <= 28 THEN 'Active'
                    WHEN date_difference <= 90 THEN 'Dormant'
                    WHEN date_difference <= 180 THEN 'Churn'
                    ELSE 'Very churn' END AS lifestage

FROM prior_deliveries pd
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
                                           '7488bf83-d93e-4bc3-a366-30dc7ff3a1e7',
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
     LEFT JOIN dimension_store ds
        ON c.store_id = ds.store_id
;

CREATE OR REPLACE TABLE dianedou.dtd_2023_cx_level_performance AS
WITH dp_deliveries AS (SELECT fodp.business_id,
                              fodp.business_name,
                              campaign_id,
                              CAMPAIGN_OR_PROMO_NAME,
                              fodp.delivery_id,
                              fodp.ACTIVE_DATE,
                              DISCOUNTS_PROMOTION_SERVICE_SOURCED_AMOUNT,
                              DISCOUNT_GROUP_MERCHANT_PROMOTION_COMPONENT_AMOUNT
                       FROM fact_order_discounts_and_promotions fodp
                            INNER JOIN PRODDB.PUBLIC.DIMENSION_DELIVERIES dd
                               ON dd.delivery_id = fodp.DELIVERY_ID
                           AND IS_FILTERED_CORE = TRUE
                           AND IS_SUBSCRIBED_CONSUMER = TRUE
                           AND dd.active_date BETWEEN $campaign_start AND $campaign_end)

   , dd_deliveries AS (SELECT dd.*, b.ADJUSTED_COHORT
                       FROM edw.invoice.fact_promotion_deliveries dd
                            JOIN DIANEDOU.dtd_2023_cx_segment u
                               ON u.creator_id = dd.consumer_id
                           AND dd.active_date BETWEEN $campaign_start AND $campaign_end
                           AND consumer_discount > 0
                            JOIN (SELECT DISTINCT ADJUSTED_COHORT, campaign_id, store_id
                                  FROM DIANEDOU.dtd_2023_master_campaign_list_nv) b
                               ON b.campaign_id::VARCHAR = dd.campaign_id::VARCHAR
                           AND b.store_id = dd.store_id)

SELECT DISTINCT dd.active_date
              , c.week                                             AS campaign_week
              , u.creator_id
              , u.lifestage
              , dd.campaign_id
              , dd.delivery_id
              , dd.store_id
--               , dd.vertical
--               , dd.vertical_name
--               , dd.business_line
--               , dd.org
--               , dd.store_name
              , dd.adjusted_cohort
              , dd.business_id
              , dd.business_name
              , IFNULL(SUM(IFNULL(promotion_fee, 0) / 100), 0)     AS mx_funded_promo_dollars
              , IFNULL(SUM(IFNULL(consumer_discount, 0) / 100), 0) AS all_cx_discount
              , IFNULL(SUM(CASE
                               WHEN IFNULL(dd.promotion_fee, 0) > 0 AND
                                    IFNULL(dd.promotion_fee, 0) < IFNULL(dd.consumer_discount, 0)
                                   THEN IFNULL(dd.promotion_fee, 0) / 100
                               WHEN IFNULL(dd.promotion_fee, 0) > 0 AND
                                    IFNULL(dd.promotion_fee, 0) >= IFNULL(dd.consumer_discount, 0)
                                   THEN IFNULL(dd.consumer_discount, 0) / 100 END),
                       0)                                          AS mx_funded_cx_discount
              , IFNULL(SUM(CASE
                               WHEN IFNULL(dd.promotion_fee, 0) > 0 AND
                                    IFNULL(dd.promotion_fee, 0) > IFNULL(dd.consumer_discount, 0)
                                   THEN (IFNULL(dd.promotion_fee, 0) - IFNULL(dd.consumer_discount, 0)) / 100 END),
                       0)                                          AS mx_marketing_fee
              , IFNULL(SUM(IFNULL(dd.subtotal, 0) / 100), 0)       AS dd_subtotals
              , IFNULL(SUM(CASE WHEN IFNULL(dd.promotion_fee, 0) > 0 THEN IFNULL(dd.subtotal, 0) / 100 END),
                       0)                                          AS mx_funded_promo_subtotals
--               , COUNT(DISTINCT dd.delivery_id)                     AS num_redemptions
--               , COUNT(DISTINCT dd.consumer_id)                     AS num_redeemers
--               , count(distinct dd.delivery_id) / count(distinct dd.consumer_id) as redemp_per_cx
              , IFNULL(SUM(IFNULL(d.DISCOUNTS_PROMOTION_SERVICE_SOURCED_AMOUNT, 0) / 100),
                       0)                                          AS dp_cx_savings
FROM DIANEDOU.dtd_2023_cx_segment u
     LEFT JOIN dd_deliveries dd
        ON u.creator_id = dd.consumer_id
     LEFT JOIN DIANEDOU.dtd_2023_master_campaign_list_nv b
        ON b.campaign_id::VARCHAR = dd.campaign_id::VARCHAR
    AND b.store_id = dd.store_id
     JOIN static.dtd_2023_calendar c
        ON dd.active_date = c.active_date
     LEFT JOIN dp_deliveries d
        ON dd.delivery_id = d.delivery_id

GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 --, 11, 12, 13, 14, 15
;

SELECT COUNT(DISTINCT creator_id), COUNT(DISTINCT delivery_id) --4.25M, 5.74M
FROM dianedou.dtd_2023_cx_level_performance
-- group by 1
;

SELECT COUNT(DISTINCT creator_id), COUNT(DISTINCT delivery_id) --4.18M, 5.52M
FROM yvonneliu.dtd_2023_master_performance_data
WHERE ACTIVE_DATE BETWEEN $campaign_start AND $campaign_end
LIMIT 10;


GRANT SELECT ON TABLE static.dtd_2023_cx_segment_performance TO read_only_users;


CREATE OR REPLACE TABLE static.dtd_2023_cx_segment_performance AS
SELECT lifestage
     , COUNT(DISTINCT delivery_id)       AS redemptions
     , redemptions / (SELECT COUNT(DISTINCT delivery_id) FROM dianedou.dtd_2023_cx_level_performance) *
       100                               AS redemp_perc
     , COUNT(DISTINCT CASE
                          WHEN adjusted_cohort = 'SMB' THEN delivery_id
                          ELSE NULL END) AS SMB_redemptions
     , COUNT(DISTINCT CASE
                          WHEN adjusted_cohort = 'NV' THEN delivery_id
                          ELSE NULL END) AS NV_redemptions
     , COUNT(DISTINCT CASE
                          WHEN adjusted_cohort = 'Mixed ENT/SMB' THEN delivery_id
                          ELSE NULL END) AS MixedENTSMB_redemptions
     , COUNT(DISTINCT CASE
                          WHEN adjusted_cohort = 'ENT Rx' THEN delivery_id
                          ELSE NULL END) AS ENTRx_redemptions

     , SUM(IFNULL(all_cx_discount, 0))   AS all_cx_discount
     , SUM(CASE
               WHEN adjusted_cohort = 'SMB' THEN IFNULL(all_cx_discount, 0)
               ELSE 0 END)               AS SMB_cx_discount
     , SUM(CASE
               WHEN adjusted_cohort = 'NV' THEN IFNULL(all_cx_discount, 0)
               ELSE 0 END)               AS NV_cx_discount
     , SUM(CASE
               WHEN adjusted_cohort = 'Mixed ENT/SMB' THEN IFNULL(all_cx_discount, 0)
               ELSE 0 END)               AS MixedENTSMB_cx_discount
     , SUM(CASE
               WHEN adjusted_cohort = 'ENT Rx' THEN IFNULL(all_cx_discount, 0)
               ELSE 0 END)               AS ENTRx_cx_discount

FROM dianedou.dtd_2023_cx_level_performance
WHERE active_date BETWEEN '2023-12-01' AND '2023-12-12'
GROUP BY 1
ORDER BY 2;

SELECT lifestage,
       redemptions,
       redemp_perc,
       SMB_redemptions,
       NV_redemptions,
       MixedENTSMB_redemptions,
       ENTRx_redemptions,
       all_cx_discount,
       SMB_cx_discount,
       NV_cx_discount,
       MixedENTSMB_cx_discount,
       ENTRx_cx_discount
FROM static.dtd_2023_cx_segment_performance
LIMIT 10;