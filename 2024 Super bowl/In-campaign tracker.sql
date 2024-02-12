SET campaign_start = '2024-02-09'::DATE;
SET campaign_end = '2024-02-11'::DATE;

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
                                  , dp_cx_savings * dp_cofunding_perc as dd_funded_dp_cx_savings

                    FROM dd_deliveries dd
                         LEFT JOIN dp_deliveries d
                            ON dd.delivery_id = d.delivery_id

                    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 --, 11, 12 --, 13, 14, 15
)

select active_date
, cx_savings_category
, sum(all_cx_discount) as total_cx_savings
, sum(dp_cx_savings) as dp_cx_savings
, sum(dd_funded_dp_cx_savings) as dd_funded_dp_cx_savings
from cx_level_perf
group by 1,2
;
