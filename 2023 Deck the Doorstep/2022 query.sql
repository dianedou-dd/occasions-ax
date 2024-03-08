CREATE OR REPLACE TABLE dianedou.DTD_2022_delivery_level_perf AS

WITH cm_data AS
    (SELECT
--to_date(r.REDEEMED_AT) active_date,
r.campaign_id,
ROUND(SUM(value), 0)     AS cm_total_discount,
COUNT(DISTINCT order_id) AS cm_num_redemptions
     FROM promotion_service.public.cassandra_redemption_by_id r
          JOIN mayurmahajan.q4_2022_dtd_promo_campaigns pc
             ON r.campaign_id = pc.campaign_id
     WHERE TO_DATE(r.REDEEMED_AT) BETWEEN '2022-12-01' AND '2022-12-12'
     GROUP BY 1)

   , fodp_data AS
    (SELECT
--A.active_date
         A.campaign_id
          , COUNT(A.delivery_id)                                                AS fodp_num_redemptions
// ,count(distinct case when b.business_vertical_id in (139, 141, 169, 265, 266, 267, 331, 332, 333, 334, 364, 430, 10000794) then A.delivery_id end)/fodp_num_redemptions as fodp_pct_retail_redemptions
// ,count(distinct case when b.business_vertical_id in (166,167,100,67,68,562,631,69,232,463,168,199) then A.delivery_id end)/fodp_num_redemptions as fodp_pct_NV_redemptions
// ,round(sum(A.subtotal)/100,0) as subtotal_promo
// ,round(sum(B.subtotal)/100,0) as subtotal_dd
// ,round(sum(B.GOV)/100,0) as GOV_dd
          , MAX(a.PROMO_CODE)                                                   AS promo_code
          , MAX(a.CAMPAIGN_OR_PROMO_NAME)                                       AS CAMPAIGN_OR_PROMO_NAME
          , MAX(a.CAMPAIGN_OR_PROMO_DESCRIPTION)                                AS CAMPAIGN_OR_PROMO_DESCRIPTION
          , MAX(a.CAMPAIGN_OR_PROMO_NOTES)                                      AS CAMPAIGN_OR_PROMO_NOTES
          , MAX(a.CAMPAIGN_DIRECTLY_RESPONSIBLE_INDIVIDUAL)                     AS CAMPAIGN_DIRECTLY_RESPONSIBLE_INDIVIDUAL
          , MAX(a.CAMPAIGN_VERTICAL)                                            AS CAMPAIGN_VERTICAL
          , ROUND(SUM(ae.FDA_OTHER_PROMOTIONS_BASE + ae.FDA_PROMOTION_CATCH_ALL + ae.FDA_CONSUMER_RETENTION) -
                  SUM(ae.FDA_BUNDLES_PRICING_DISCOUNT),
                  0)                                                            AS fodp_finance_promo_spend --Finance provided spend metric
          , ROUND(SUM(A.discounts_total_amount) / 100, 0)                       AS fodp_discounts_total_amount
          , ROUND(SUM(A.discounts_doordash_funded_amount) / 100, 0)             AS fodp_discounts_doordash_funded_amount
          , ROUND(SUM(A.discounts_merchant_funded_amount) / 100, 0)             AS fodp_discounts_merchant_funded_amount
          , ROUND(SUM(A.DISCOUNTS_PROMOTION_SERVICE_SOURCED_AMOUNT) / 100, 0)   AS fodp_DISCOUNTS_PROMOTION_SERVICE_SOURCED_AMOUNT
          , ROUND(SUM(A.DISCOUNTS_PRICING_SERVICE_SOURCED_AMOUNT) / 100, 0)     AS fodp_DISCOUNTS_PRICING_SERVICE_SOURCED_AMOUNT
          , ROUND(SUM(A.DELIVERY_FEE_DISCOUNT_AMOUNT) / 100, 0)                 AS fodp_DELIVERY_FEE_DISCOUNT_AMOUNT
          , ROUND(SUM(A.SERVICE_FEE_DISCOUNT_AMOUNT) / 100, 0)                  AS fodp_SERVICE_FEE_DISCOUNT_AMOUNT
          , ROUND(SUM(A.DASHPASS_SUBSCRIPTION_DISCOUNT_AMOUNT) / 100, 0)        AS fodp_DASHPASS_SUBSCRIPTION_DISCOUNT_AMOUNT
          , ROUND(SUM(A.MERCHANT_PROMOTION_DISCOUNT_AMOUNT) / 100, 0)           AS fodp_MERCHANT_PROMOTION_DISCOUNT_AMOUNT
          , ROUND(SUM(A.DISCOUNT_GROUP_SUBSCRIPTION_COMPONENT_AMOUNT) / 100, 0) AS fodp_DISCOUNT_GROUP_SUBSCRIPTION_COMPONENT_AMOUNT
          , ROUND(SUM(A.DISCOUNT_GROUP_CONSUMER_PROMOTION_COMPONENT_AMOUNT) / 100,
                  0)                                                            AS fodp_DISCOUNT_GROUP_CONSUMER_PROMOTION_COMPONENT_AMOUNT
          , ROUND(SUM(A.DISCOUNT_GROUP_MERCHANT_PROMOTION_COMPONENT_AMOUNT) / 100,
                  0)                                                            AS fodp_DISCOUNT_GROUP_MERCHANT_PROMOTION_COMPONENT_AMOUNT
     FROM fact_order_discounts_and_promotions A
          LEFT JOIN dimension_deliveries B
             ON A.delivery_id = B.delivery_id
          LEFT JOIN fact_order_discounts_and_promotions_extended ae
             ON A.delivery_id = ae.delivery_id
          JOIN mayurmahajan.q4_2022_dtd_promo_campaigns pc
             ON A.campaign_id = pc.campaign_id
     WHERE A.active_date BETWEEN '2022-12-01' AND '2022-12-12'
       AND b.is_filtered_core = 1
     GROUP BY 1
--order by 1 desc
    )
SELECT --distinct
    cd.campaign_id
     , MAX(fd.PROMO_CODE)                                           AS promo_code
     , MAX(fd.CAMPAIGN_OR_PROMO_NAME)                               AS CAMPAIGN_OR_PROMO_NAME
     , MAX(fd.CAMPAIGN_OR_PROMO_DESCRIPTION)                        AS CAMPAIGN_OR_PROMO_DESCRIPTION
     , MAX(fd.CAMPAIGN_OR_PROMO_NOTES)                              AS CAMPAIGN_OR_PROMO_NOTES
     , MAX(fd.CAMPAIGN_DIRECTLY_RESPONSIBLE_INDIVIDUAL)             AS CAMPAIGN_DIRECTLY_RESPONSIBLE_INDIVIDUAL
     , MAX(fd.CAMPAIGN_VERTICAL)                                    AS CAMPAIGN_VERTICAL
     , SUM(cm_num_redemptions)                                      AS cm_num_redemptions
     , SUM(fodp_num_redemptions)                                    AS fodp_num_redemptions
     , SUM(cm_total_discount)                                       AS cm_total_discount
     , SUM(fodp_DISCOUNTS_PROMOTION_SERVICE_SOURCED_AMOUNT)         AS fodp_DISCOUNTS_PROMOTION_SERVICE_SOURCED_AMOUNT
     , SUM(fodp_MERCHANT_PROMOTION_DISCOUNT_AMOUNT)                 AS fodp_MERCHANT_PROMOTION_DISCOUNT_AMOUNT
     , SUM(fodp_DISCOUNT_GROUP_MERCHANT_PROMOTION_COMPONENT_AMOUNT) AS fodp_DISCOUNT_GROUP_MERCHANT_PROMOTION_COMPONENT_AMOUNT
     , SUM(fodp_DISCOUNT_GROUP_CONSUMER_PROMOTION_COMPONENT_AMOUNT) AS fodp_DISCOUNT_GROUP_CONSUMER_PROMOTION_COMPONENT_AMOUNT
     , SUM(fodp_discounts_total_amount)                             AS fodp_discounts_total_amount
     , SUM(fodp_discounts_doordash_funded_amount)                   AS fodp_discounts_doordash_funded_amount
     , SUM(fodp_discounts_merchant_funded_amount)                   AS fodp_discounts_merchant_funded_amount
     , SUM(fodp_DISCOUNTS_PRICING_SERVICE_SOURCED_AMOUNT)           AS fodp_DISCOUNTS_PRICING_SERVICE_SOURCED_AMOUNT
     , SUM(fodp_DELIVERY_FEE_DISCOUNT_AMOUNT)                       AS fodp_DELIVERY_FEE_DISCOUNT_AMOUNT
     , SUM(fodp_SERVICE_FEE_DISCOUNT_AMOUNT)                        AS fodp_SERVICE_FEE_DISCOUNT_AMOUNT
     , SUM(fodp_DASHPASS_SUBSCRIPTION_DISCOUNT_AMOUNT)              AS fodp_DASHPASS_SUBSCRIPTION_DISCOUNT_AMOUNT
     , SUM(fodp_DISCOUNT_GROUP_SUBSCRIPTION_COMPONENT_AMOUNT)       AS fodp_DISCOUNT_GROUP_SUBSCRIPTION_COMPONENT_AMOUNT
     , SUM(fodp_finance_promo_spend)                                AS fodp_finance_promo_spend
FROM cm_data cd
     LEFT JOIN fodp_data fd
        ON cd.campaign_id = fd.campaign_id
GROUP BY 1
ORDER BY cm_num_redemptions DESC
;

SELECT DISTINCT campaign_vertical
FROM DIANEDOU.DTD_2022_DELIVERY_LEVEL_PERF;

SET campaign_start = '2022-12-01'::DATE;
SET campaign_end = '2022-12-12'::DATE;

WITH sub AS (SELECT COUNT(DISTINCT CREATOR_ID) AS all_orderers
             FROM DIMENSION_DELIVERIES
             WHERE ACTIVE_DATE BETWEEN $campaign_start AND $campaign_end)


SELECT sg.COHORT                                                     AS adjusted_cohort
     , COUNT(DISTINCT a.BUSINESS_ID)                                 AS num_mx
     , COUNT(DISTINCT a.CREATOR_ID)                                  AS redemption
     , COUNT(DISTINCT a.CREATOR_ID) / (SELECT all_orderers FROM sub) AS redemption_perc
     , AVG(gov) / 100                                                AS avg_promo_redemption


FROM DIMENSION_DELIVERIES a
     JOIN fact_order_discounts_and_promotions_extended b
        ON a.ACTIVE_DATE BETWEEN $campaign_start AND $campaign_end
    AND a.DELIVERY_ID = b.delivery_id
     JOIN DIANEDOU.DTD_2022_DELIVERY_LEVEL_PERF c
        ON b.campaign_id = c.CAMPAIGN_ID
     LEFT JOIN (SELECT DISTINCT store_id, cohort FROM public.fact_ads_promo_store_categorization) sg
        ON sg.store_id = a.store_id
     LEFT JOIN edw.cng.dimension_new_vertical_store_tags nv
        ON nv.is_filtered_mp_vertical = 1 AND a.store_id = nv.store_id
GROUP BY 1;


WITH sub AS (SELECT COUNT(DISTINCT CREATOR_ID) AS all_orderers
             FROM DIMENSION_DELIVERIES
             WHERE ACTIVE_DATE BETWEEN $campaign_start AND $campaign_end)


SELECT
--     CASE
--            WHEN nv.is_filtered_mp_vertical = 1 AND nv.org_id = 1 THEN 'CG&A'
--            WHEN nv.vertical_name = '1P Convenience' THEN 'DashMart'
--            WHEN nv.business_vertical_id IN (265, 266, 267, 331, 332, 333, 334, 364, 430, 10000794)
--                THEN 'Retail'
--            WHEN nv.business_vertical_id IN (139, 169) THEN 'Pet' ---- 20231218 addition
--            WHEN (nv.business_vertical_id IN (141) OR nv.business_id = 749869)
--                THEN 'Flowers' ---- 20231218 addition
--            ELSE 'Other' END                                          AS vertical
    CASE
        WHEN nv.cng_verticals IS NOT NULL THEN 'NV'
        WHEN sg.cohort = 'SMB' THEN 'SMB'
--            WHEN cohort = 'Ent/MM' AND vertical NOT IN ('Other') THEN 'NV'
        ELSE 'ENT Rx' END                                            AS adjusted_cohort
     , COUNT(DISTINCT a.BUSINESS_ID)                                 AS num_mx
     , COUNT(DISTINCT a.CREATOR_ID)                                  AS redemption
     , COUNT(DISTINCT a.CREATOR_ID) / (SELECT all_orderers FROM sub) AS redemption_perc
     , AVG(gov) / 100                                                AS avg_promo_redemption


FROM DIMENSION_DELIVERIES a
     JOIN fact_order_discounts_and_promotions_extended b
        ON a.ACTIVE_DATE BETWEEN $campaign_start AND $campaign_end
    AND a.DELIVERY_ID = b.delivery_id
     JOIN DIANEDOU.DTD_2022_DELIVERY_LEVEL_PERF c
        ON b.campaign_id = c.CAMPAIGN_ID
     LEFT JOIN tableau.new_verticals_stores nv
        ON a.store_id = nv.store_id AND nv.cng_verticals NOT IN ('Packages', 'Cannabis', 'Other')
     LEFT JOIN (SELECT DISTINCT store_id, cohort FROM public.fact_ads_promo_store_categorization) sg
        ON sg.store_id = a.store_id
--      LEFT JOIN edw.cng.dimension_new_vertical_store_tags nv
--         ON nv.is_filtered_mp_vertical = 1 AND a.store_id = nv.store_id
GROUP BY 1;



SET campaign_start = '2023-12-01'::DATE;
SET campaign_end = '2023-12-12'::DATE;

WITH sub AS (SELECT COUNT(DISTINCT CREATOR_ID) AS all_orderers
             FROM DIMENSION_DELIVERIES
             WHERE ACTIVE_DATE BETWEEN $campaign_start AND $campaign_end)


SELECT CASE
           WHEN nv.cng_verticals IS NOT NULL THEN 'NV'
           WHEN sg.cohort = 'SMB' THEN 'SMB'
--            WHEN cohort = 'Ent/MM' AND vertical NOT IN ('Other') THEN 'NV'
           ELSE 'ENT Rx' END                                         AS adjusted_cohort
     , COUNT(DISTINCT a.BUSINESS_ID)                                 AS num_mx
     , COUNT(DISTINCT a.CREATOR_ID)                                  AS redemption
     , COUNT(DISTINCT a.CREATOR_ID) / (SELECT all_orderers FROM sub) AS redemption_perc
     , AVG(gov) / 100                                                AS avg_promo_redemption


FROM DIMENSION_DELIVERIES a
     JOIN fact_order_discounts_and_promotions_extended b
        ON a.ACTIVE_DATE BETWEEN $campaign_start AND $campaign_end
    AND a.DELIVERY_ID = b.delivery_id
     JOIN (SELECT DISTINCT CAMPAIGN_ID FROM DIANEDOU.dtd_2023_master_campaign_list_nv) c
        ON b.campaign_id = c.CAMPAIGN_ID
     LEFT JOIN tableau.new_verticals_stores nv
        ON a.store_id = nv.store_id AND nv.cng_verticals NOT IN ('Packages', 'Cannabis', 'Other')
     LEFT JOIN (SELECT DISTINCT store_id, cohort FROM public.fact_ads_promo_store_categorization) sg
        ON sg.store_id = a.store_id
GROUP BY 1
;
--dp cx savings
    select IFNULL(SUM(IFNULL(A.DISCOUNTS_PROMOTION_SERVICE_SOURCED_AMOUNT, 0) / 100)
                      , 0) AS dp_cx_savings

     FROM fact_order_discounts_and_promotions A
          LEFT JOIN dimension_deliveries B
             ON A.delivery_id = B.delivery_id
          LEFT JOIN fact_order_discounts_and_promotions_extended ae
             ON A.delivery_id = ae.delivery_id
          JOIN mayurmahajan.q4_2022_dtd_promo_campaigns pc
             ON A.campaign_id = pc.campaign_id
     WHERE A.active_date BETWEEN '2022-12-01' AND '2022-12-12'
       AND b.is_filtered_core = 1
       AND IS_SUBSCRIBED_CONSUMER = TRUE
--      GROUP BY 1
--order by 1 desc
     ;

 select
        date_trunc(week, ddr.active_date_utc) as wbr_week,
        nv.vertical_name,
        sum(1) as trials
    from stefaniemontgomery.dimension_deliveries_ranked ddr
    left join edw.cng.dimension_new_vertical_store_tags nv
        on nv.store_id = ddr.store_id
        and is_filtered_mp_vertical = 1
    where ddr.country_id = 1
    and nv.vertical_name in ('1P Convenience','3P Convenience','Alcohol','Grocery','Pets','Flowers','Emerging Retail')
    and date_trunc(week, ddr.active_date_utc) between current_date - 90 and date_trunc(week, current_date) - 1
    and ddr.order_number_vertical = 1
    group by all
;

select * from stefaniemontgomery.dimension_deliveries_ranked ddr limit 5;