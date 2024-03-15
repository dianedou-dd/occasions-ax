WITH flower_table AS (SELECT ds.store_id,
                             ds.name,
                             ds.business_id,
                             ds.business_name,
                             ds.submarket_id,
                             ds.submarket_name,
                             ds.country_id,
                             ds.is_active
                      FROM proddb.tableau.new_verticals_stores nvs
                           JOIN dimension_store AS ds
                              ON ds.store_id = nvs.store_id
                      WHERE 1 = 1
                        AND nvs.business_vertical_id IN (141)
                        AND ds.country_id = 1

                      UNION
                      SELECT DISTINCT ds.store_id,
                                      ds.name,
                                      ds.business_id,
                                      ds.business_name,
                                      ds.submarket_id,
                                      ds.submarket_name,
                                      ds.country_id,
                                      ds.is_active
                      FROM dimension_store AS ds
                      WHERE 1 = 1
                        AND ds.country_id = 1
                        AND ds.business_id IN (749869)
                        AND ds.is_active = TRUE)

   , ly_cx AS (SELECT DISTINCT creator_id
               FROM dimension_deliveries a
                    JOIN flower_table b
                       ON a.store_id = b.store_id
               WHERE 1 = 1
                 AND is_filtered_core = TRUE
                 AND active_date IN ('02/12/2023', '02/13/2023', '02/14/2023')
               GROUP BY 1)


   , cx_reordered AS (SELECT DISTINCT creator_id
                      FROM dimension_deliveries a
                           JOIN flower_table b
                              ON a.store_id = b.store_id
                      WHERE 1 = 1
                        AND a.is_filtered_core = TRUE
                        AND a.active_date IN ('02/12/2024', '02/13/2024', '02/14/2024')
                        AND creator_id IN (SELECT creator_id FROM ly_cx))


SELECT COUNT(DISTINCT a.creator_id) AS ly,
       COUNT(DISTINCT b.creator_id) AS ty,
       ty / ly * 100
FROM ly_cx a
     JOIN dimension_users du
        ON du.consumer_id = a.creator_id
     LEFT JOIN cx_reordered b
        ON a.creator_id = b.creator_id
WHERE 1 = 1;


WITH flower_table AS (SELECT ds.store_id,
                             ds.name,
                             ds.business_id,
                             ds.business_name,
                             ds.submarket_id,
                             ds.submarket_name,
                             ds.country_id,
                             ds.is_active
                      FROM proddb.tableau.new_verticals_stores nvs
                           JOIN dimension_store AS ds
                              ON ds.store_id = nvs.store_id
                      WHERE 1 = 1
                        AND nvs.business_vertical_id IN (141)
                        AND ds.country_id = 1

                      UNION
                      SELECT DISTINCT ds.store_id,
                                      ds.name,
                                      ds.business_id,
                                      ds.business_name,
                                      ds.submarket_id,
                                      ds.submarket_name,
                                      ds.country_id,
                                      ds.is_active
                      FROM dimension_store AS ds
                      WHERE 1 = 1
                        AND ds.country_id = 1
                        AND ds.business_id IN (749869)
                        AND ds.is_active = TRUE)

   , ly_cx AS (SELECT DISTINCT creator_id
               FROM dimension_deliveries a
                    JOIN flower_table b
                       ON a.store_id = b.store_id
               WHERE 1 = 1
                 AND is_filtered_core = TRUE
                 AND active_date IN ('02/12/2023', '02/13/2023', '02/14/2023')
               GROUP BY 1)

   , cx_reordered AS (SELECT DISTINCT creator_id
                      FROM dimension_deliveries a
                           JOIN flower_table b
                              ON a.store_id = b.store_id
                      WHERE 1 = 1
                        AND a.is_filtered_core = TRUE
                        AND a.active_date IN ('02/12/2024', '02/13/2024', '02/14/2024')
                        AND creator_id IN (SELECT creator_id FROM ly_cx))


SELECT CASE
           WHEN COALESCE(mh.days_since_last_purchase, -1) BETWEEN 0 AND 28 THEN 'Active'
           WHEN COALESCE(mh.days_since_last_purchase, -1) BETWEEN 29 AND 90 THEN 'Dormant'
           WHEN COALESCE(mh.days_since_last_purchase, -1) > 90 THEN 'Churned'
           ELSE 'New'
           END AS mp_segment,
       COUNT(DISTINCT sub.CREATOR_ID)
FROM ly_cx sub
     LEFT JOIN mattheitz.mh_customer_authority mh
        ON mh.creator_id = sub.creator_id
    AND mh.dte = '2024-02-11'


GROUP BY 1;


WITH flower_table AS (SELECT ds.store_id,
                             ds.name,
                             ds.business_id,
                             ds.business_name,
                             ds.submarket_id,
                             ds.submarket_name,
                             ds.country_id,
                             ds.is_active
                      FROM proddb.tableau.new_verticals_stores nvs
                           JOIN dimension_store AS ds
                              ON ds.store_id = nvs.store_id
                      WHERE 1 = 1
                        AND nvs.business_vertical_id IN (141)
                        AND ds.country_id = 1

                      UNION
                      SELECT DISTINCT ds.store_id,
                                      ds.name,
                                      ds.business_id,
                                      ds.business_name,
                                      ds.submarket_id,
                                      ds.submarket_name,
                                      ds.country_id,
                                      ds.is_active
                      FROM dimension_store AS ds
                      WHERE 1 = 1
                        AND ds.country_id = 1
                        AND ds.business_id IN (749869)
                        AND ds.is_active = TRUE)

   , ly_cx AS (SELECT DISTINCT creator_id,
                               a.IS_SUBSCRIBED_CONSUMER
               FROM dimension_deliveries a
                    JOIN flower_table b
                       ON a.store_id = b.store_id
               WHERE 1 = 1
                 AND is_filtered_core = TRUE
                 AND active_date BETWEEN '2023-02-05' AND '2023-02-15' -- in ('02/12/2023', '02/13/2023', '02/14/2023')
               GROUP BY ALL)

   , ty_cx AS (SELECT DISTINCT creator_id,
                               a.IS_SUBSCRIBED_CONSUMER
               FROM dimension_deliveries a
                    JOIN flower_table b
                       ON a.store_id = b.store_id
               WHERE 1 = 1
                 AND is_filtered_core = TRUE
                 AND active_date BETWEEN '2024-02-05' AND '2024-02-15' -- in ('02/12/2023', '02/13/2023', '02/14/2023')
               GROUP BY ALL)

   , ly_MDAY_cx AS (SELECT DISTINCT creator_id,
                                    a.IS_SUBSCRIBED_CONSUMER
                    FROM dimension_deliveries a
                         JOIN flower_table b
                            ON a.store_id = b.store_id
                    WHERE 1 = 1
                      AND is_filtered_core = TRUE
                      AND active_date BETWEEN '2023-05-08' AND '2023-05-15' -- in ('02/12/2023', '02/13/2023', '02/14/2023')
                    GROUP BY ALL)


SELECT a.LIFESTAGE
, count(DISTINCT a.CREATOR_ID) as redeemers
, count(distinct case when ly_cx.CREATOR_ID is not null then a.CREATOR_ID end) as ly_orderers
, ly_orderers / redeemers
FROM dianedou.vday_2024_cx_level_performance a
left join ly_cx on a.CREATOR_ID = ly_cx.CREATOR_ID
GROUP BY  1
;

SELECT a.LIFESTAGE
     , COUNT(DISTINCT a.CREATOR_ID)                                                 AS redeemers
     , COUNT(DISTINCT CASE WHEN ly_cx.CREATOR_ID IS NOT NULL THEN a.CREATOR_ID END) AS ly_orderers
     , ly_orderers / redeemers
FROM dianedou.vday_2024_cx_level_performance a
     LEFT JOIN ly_mday_cx ly_cx
        ON a.CREATOR_ID = ly_cx.CREATOR_ID
GROUP BY 1
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

SELECT COUNT(DISTINCT CONSUMER_ID)
FROM edw.consumer.fact_consumer_subscription__daily__extended
WHERE dte = '2024-02-05'--(select max(dte) from edw.consumer.fact_consumer_subscription__daily__extended)
  AND ('2024-02-05'::DATE) BETWEEN start_time AND end_time
  AND DYNAMIC_SUBSCRIPTION_STATUS IN
      ('active_free_subscription', 'active_paid', 'active_trial', 'trial_waiting_for_payment',
       'dtp_waiting_for_payment', 'paid_waiting_for_payment')
;


SELECT 'LY' AS period
     , dd.IS_SUBSCRIBED_CONSUMER
     , COUNT(DISTINCT CREATOR_ID)

FROM dimension_deliveries dd
WHERE is_filtered_core = TRUE
  AND active_date BETWEEN '2023-02-05' AND '2023-02-15'
GROUP BY 1, 2

UNION ALL

SELECT 'TY' AS period
     , dd.IS_SUBSCRIBED_CONSUMER
     , COUNT(DISTINCT CREATOR_ID)

FROM dimension_deliveries dd
WHERE is_filtered_core = TRUE
  AND active_date BETWEEN '2024-02-05' AND '2024-02-15'
GROUP BY 1, 2
;


SELECT dd.IS_SUBSCRIBED_CONSUMER
     , COUNT(DISTINCT a.DELIVERY_ID)
FROM dianedou.vday_2024_cx_level_performance a
     LEFT JOIN dimension_deliveries dd
        ON a.DELIVERY_ID = dd.DELIVERY_ID
GROUP BY 1;


--28D retention
WITH first_order AS (SELECT DISTINCT a.creator_id                  AS consumer_id
                                   , VERTICAL
                                   , dd.IS_SUBSCRIBED_CONSUMER
                                   , COUNT(DISTINCT a.DELIVERY_ID) AS redemptions_pp
                                   , SUM(a.ALL_CX_DISCOUNT)        AS savings_pp
                                   , AVG(a.GOV / 100)              AS avg_gov
                     FROM dianedou.vday_2024_cx_level_performance a
                          LEFT JOIN dimension_deliveries dd
                             ON a.DELIVERY_ID = dd.DELIVERY_ID
                     WHERE vertical IS NOT NULL
                     GROUP BY 1, 2, 3)


   , staging AS (SELECT VERTICAL
                      , u.IS_SUBSCRIBED_CONSUMER
                      , consumer_id
                      , redemptions_pp
                      , savings_pp
                      , avg_gov
                      , COUNT(DISTINCT
                              CASE WHEN dd.creator_id IS NOT NULL THEN dd.creator_id ELSE NULL END) AS retained_flag
                 FROM first_order u
                      LEFT JOIN dimension_deliveries dd
                         ON dd.creator_id = u.consumer_id
                     AND
                            dd.active_date BETWEEN DATEADD(DAY, 1, '2024-02-15'::DATE) AND DATEADD(DAY, 29, '2024-02-15'::DATE)
                     AND dd.is_filtered_core = TRUE
                     AND is_caviar = 0
                 GROUP BY 1, 2, 3, 4, 5, 6)

SELECT IS_SUBSCRIBED_CONSUMER
     , COUNT(DISTINCT consumer_id)                                       AS redeemers
     , redeemers / (SELECT COUNT(DISTINCT consumer_id) FROM first_order) AS redeemer_perc
     , SUM(retained_flag)                                                AS retained_cx
     , AVG(retained_flag)                                                AS retention_28D
     , AVG(redemptions_pp)                                               AS avg_redemptions_per_cx
     , AVG(savings_pp)                                                   AS avg_savings_per_cx
     , SUM(savings_pp) / SUM(redemptions_pp)                             AS avg_savings_per_order
     , AVG(avg_gov)                                                      AS avg_gov
     , avg_savings_per_order / AVG(avg_gov)


FROM staging
GROUP BY 1