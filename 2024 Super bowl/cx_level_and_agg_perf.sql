SET campaign_start = '2024-02-09'::DATE;
SET campaign_end = '2024-02-11'::DATE;

CREATE OR REPLACE TABLE dianedou.superbowl_2024_cx_level_performance AS

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
                           AND dd.active_date BETWEEN $campaign_start AND $campaign_end
                       INNER JOIN dianedou.Superbowl_2024_Campaign_ID id
                          on fodp.CAMPAIGN_ID = id."Campaign ID"
                      )

   , dd_deliveries AS (SELECT dd.*
                              , b."Cx Savings Category" as cx_savings_category
                              , b."% DP Funded" as dp_cofunding_perc
                       FROM edw.invoice.fact_promotion_deliveries dd
                                -- JOIN DIANEDOU.dtd_2023_cx_segment u
                                --   ON u.creator_id = dd.consumer_id

                            JOIN dianedou.Superbowl_2024_Campaign_ID b
                               ON dd.active_date BETWEEN $campaign_start AND $campaign_end
                           AND dd.consumer_discount > 0
                           AND dd.campaign_id::VARCHAR = b."Campaign ID")


, cx_level_perf as (SELECT DISTINCT dd.active_date
--               , c.week                                             AS campaign_week
                                  , dd.CONSUMER_ID
--               , u.lifestage
                                  , dd.campaign_id
                                  , dd.delivery_id
                                  , dd.store_id
--               , b.vertical
--               , dd.vertical_name
--               , dd.business_line
--               , dd.org
--               , dd.store_name
                                  , dd.cx_savings_category
                                  , dd.dp_cofunding_perc
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
                                                       THEN
                                                       (IFNULL(dd.promotion_fee, 0) - IFNULL(dd.consumer_discount, 0)) /
                                                       100 END),
                                           0)                                          AS mx_marketing_fee
                                  , IFNULL(SUM(IFNULL(dd.subtotal, 0) / 100), 0)       AS dd_subtotals
                                  , IFNULL(
            SUM(CASE WHEN IFNULL(dd.promotion_fee, 0) > 0 THEN IFNULL(dd.subtotal, 0) / 100 END),
            0)                                                                         AS mx_funded_promo_subtotals
--               , COUNT(DISTINCT dd.delivery_id)                     AS num_redemptions
--               , COUNT(DISTINCT dd.consumer_id)                     AS num_redeemers
--               , count(distinct dd.delivery_id) / count(distinct dd.consumer_id) as redemp_per_cx
                                  , IFNULL(SUM(IFNULL(d.DISCOUNTS_PROMOTION_SERVICE_SOURCED_AMOUNT, 0) / 100),
                                           0)                                          AS dp_cx_savings
                                  , dp_cx_savings * dp_cofunding_perc /100 as dd_funded_dp_cx_savings

                    FROM dd_deliveries dd
                         LEFT JOIN dp_deliveries d
                            ON dd.delivery_id = d.delivery_id

                    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 --, 11, 12 --, 13, 14, 15
)

select *
from cx_level_perf

;

GRANT SELECT ON TABLE dianedou.superbowl_2024_cx_level_performance  TO read_only_users;

CREATE OR REPLACE TABLE dianedou.superbowl_2024_agg_performance  AS
WITH calc_aov AS (SELECT AVG(IFNULL(gov / 100, 0)) AS avg_gov, campaign_id
                  FROM edw.invoice.fact_promotion_deliveries
                  WHERE active_date BETWEEN $campaign_start - 31 and $campaign_start - 1 ---- past 30 days' gov
                  GROUP BY 2)

   , agg_performance AS (SELECT ACTIVE_DATE,
                                CAMPAIGN_ID,
                                cx_savings_category as vertical,
--                                 adjusted_cohort,
                                SUM(MX_FUNDED_PROMO_DOLLARS)         AS MX_FUNDED_PROMO_DOLLARS,
                                SUM(ALL_CX_DISCOUNT)                 AS ALL_CX_DISCOUNT,
                                SUM(MX_FUNDED_CX_DISCOUNT)           AS MX_FUNDED_CX_DISCOUNT,
                                SUM(MX_MARKETING_FEE)                AS MX_MARKETING_FEE,
                                SUM(DD_SUBTOTALS)                    AS DD_SUBTOTALS,
                                SUM(MX_FUNDED_PROMO_SUBTOTALS)       AS MX_FUNDED_PROMO_SUBTOTALS,
                                SUM(DP_CX_SAVINGS)                   AS DP_CX_SAVINGS,
                                COUNT(DISTINCT delivery_id)          AS num_redemptions,
                                COUNT(DISTINCT CONSUMER_ID)           AS num_redeemers,
                                num_redemptions / num_redeemers      AS redemp_per_cx,
                                IFNULL(SUM(IFNULL(gov, 0) / 100), 0) AS total_gov,
                                DIV0(total_gov, num_redemptions)     AS avg_gov
                         FROM dianedou.superbowl_2024_cx_level_performance
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
                               , incr_gmv_raw                                  AS incr_gmv
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

GRANT SELECT ON TABLE dianedou.superbowl_2024_agg_performance TO read_only_users;