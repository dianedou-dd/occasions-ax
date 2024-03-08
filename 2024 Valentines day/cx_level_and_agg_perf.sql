SET campaign_start = '2024-01-28'::DATE;
SET campaign_end = '2024-02-15'::DATE;

CREATE OR REPLACE TABLE DIANEDOU.vday_2024_cx_segment AS
WITH redeemers AS (SELECT DISTINCT creator_id,
                                       MIN(active_date)            AS first_redemption_date,
                                       COUNT(DISTINCT delivery_id) AS redemptions
                       FROM fact_order_discounts_and_promotions_extended p
                            INNER JOIN (SELECT DISTINCT "Campaign ID" as campaign_id FROM  dianedou.Vday_2024_promo_codes) s
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
                               JOIN redeemers dtd
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
                        THEN 'New' --''activated during campaign' --also considered 'New'
                    WHEN date_difference <= 28 THEN 'Active'
                    WHEN date_difference <= 90 THEN 'Dormant'
                    WHEN date_difference <= 180 THEN 'Churn'
                    ELSE 'Very churn' END AS lifestage

FROM prior_deliveries pd
;



CREATE OR REPLACE TABLE dianedou.vday_2024_cx_level_performance AS
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

   , dd_deliveries AS (SELECT dd.*, b.vertical
                       FROM edw.invoice.fact_promotion_deliveries dd
                            JOIN DIANEDOU.vday_2024_cx_segment u
                               ON u.creator_id = dd.consumer_id
                           AND dd.active_date BETWEEN $campaign_start AND $campaign_end
                           AND consumer_discount > 0
                            JOIN  dianedou.Vday_2024_promo_codes b
                            ON b."Campaign ID"::VARCHAR = dd.CAMPAIGN_ID::VARCHAR
--                             JOIN (SELECT DISTINCT ADJUSTED_COHORT, campaign_id, store_id
--                                   FROM DIANEDOU.dtd_2023_master_campaign_list_nv) b
--                                ON b.campaign_id::VARCHAR = dd.campaign_id::VARCHAR
--                            AND b.store_id = dd.store_id
                           )

SELECT DISTINCT dd.active_date
              , u.creator_id
              , u.lifestage
              , dd.campaign_id
              , dd.delivery_id
              , dd.store_id
              , b.vertical
--               , dd.vertical_name
--               , dd.business_line
--               , dd.org
--               , dd.store_name
              , dd.VERTICAL as dd_vertical
              , dd.business_id
              , dd.business_name
              , dd.GOV
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
FROM DIANEDOU.vday_2024_cx_segment u
     LEFT JOIN dd_deliveries dd
        ON u.creator_id = dd.consumer_id
     LEFT JOIN dianedou.Vday_2024_promo_codes b
        ON b."Campaign ID"::VARCHAR = dd.campaign_id::VARCHAR
--     AND b.store_id = dd.store_id
     LEFT JOIN dp_deliveries d
        ON dd.delivery_id = d.delivery_id

GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11
;

select * from dianedou.vday_2024_cx_level_performance limit 100;

grant select on table dianedou.vday_2024_cx_level_performance to read_only_users;

CREATE OR REPLACE TABLE dianedou.vday_2024_all_campaign_impact AS
WITH calc_aov AS (SELECT AVG(IFNULL(gov / 100, 0)) AS avg_gov, campaign_id
                  FROM edw.invoice.fact_promotion_deliveries
                  WHERE active_date BETWEEN $campaign_start - 31 and $campaign_start - 1 ---- past 30 days' gov
                  GROUP BY 2)

   , agg_performance AS (SELECT ACTIVE_DATE,
                                CAMPAIGN_ID,
                                vertical,
--                                 adjusted_cohort,
                                SUM(MX_FUNDED_PROMO_DOLLARS)         AS MX_FUNDED_PROMO_DOLLARS,
                                SUM(ALL_CX_DISCOUNT)                 AS ALL_CX_DISCOUNT,
                                SUM(MX_FUNDED_CX_DISCOUNT)           AS MX_FUNDED_CX_DISCOUNT,
                                SUM(MX_MARKETING_FEE)                AS MX_MARKETING_FEE,
                                SUM(DD_SUBTOTALS)                    AS DD_SUBTOTALS,
                                SUM(MX_FUNDED_PROMO_SUBTOTALS)       AS MX_FUNDED_PROMO_SUBTOTALS,
                                SUM(DP_CX_SAVINGS)                   AS DP_CX_SAVINGS,
                                COUNT(DISTINCT delivery_id)          AS num_redemptions,
                                COUNT(DISTINCT CREATOR_ID)           AS num_redeemers,
                                num_redemptions / num_redeemers      AS redemp_per_cx,
                                IFNULL(SUM(IFNULL(gov, 0) / 100), 0) AS total_gov,
                                DIV0(total_gov, num_redemptions)     AS avg_gov
                         FROM dianedou.vday_2024_cx_level_performance
                         GROUP BY 1, 2, 3)

   , staging AS (SELECT DISTINCT a.active_date
                               , a.campaign_id
                               , a.vertical
--                                , a.adjusted_cohort

                               , IFF(b.avg_gov > 0, b.avg_gov, CASE
                                                                   WHEN vertical = 'Other' THEN 36.12
                                                                   ELSE 37.48 END) AS adjusted_aov_step1
                               , IFNULL(CASE
                                            WHEN adjusted_aov_step1 > 83 THEN 83
                                            WHEN adjusted_aov_step1 < 30 THEN 30
                                            ELSE adjusted_aov_step1 END, 0)        AS adjusted_aov

                               , CASE
                                     WHEN vertical = 'Other' THEN (
                                         mx_funded_cx_discount --- assumption: $1 promo spend = $1 incr GMV
                                             + (all_cx_discount - mx_funded_cx_discount) /
                                               (all_cx_discount / num_redemptions) * 0.63 * adjusted_aov ---- Rx
                                         )
                                     WHEN vertical NOT IN ('Other') THEN (
                                         mx_funded_cx_discount --- assumption: $1 promo spend = $1 incr GMV
                                             + (all_cx_discount - mx_funded_cx_discount) /
                                               (all_cx_discount / num_redemptions) * 0.46 * adjusted_aov ---- Big G GOV
                                         )
                                     ELSE 0 END                                    AS incr_gmv_raw
                               , incr_gmv_raw / 2                                  AS incr_gmv
                               , DIV0(incr_gmv, adjusted_aov)                      AS incr_orders
                               , b.avg_gov
                               , mx_funded_cx_discount
                               , all_cx_discount
                               , num_redemptions
                 FROM agg_performance a
                      LEFT JOIN calc_aov b
                         ON a.campaign_id = b.campaign_id
                 --                  WHERE campaign_week IS NOT NULL
                 ORDER BY 4 DESC)

SELECT active_date,
       SUM(incr_gmv)    AS incr_gmv,
       SUM(incr_orders) AS incr_orders
FROM staging
GROUP BY 1
;

select SUM(incr_gmv),  sum(incr_orders) from dianedou.vday_2024_all_campaign_impact;