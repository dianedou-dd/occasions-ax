CREATE OR REPLACE TABLE dianedou.flower_daily_volume AS
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
                        AND nvs.business_vertical_id IN (141) --364, 331, 430, 10000794, 332, 266, 333, 265, 334, 267)
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
                        AND ds.is_active = TRUE
                        AND ds.country_id = 1
                        AND ds.business_id IN (749869) --and ds.business_id in (749869, 331358, 979026, 11116009)
)

   , dashmart_delivs AS (SELECT DISTINCT dd.active_date,
                                         dd.store_id,
                                         dd.delivery_id,
                                         dd.is_asap,
                                         dd.is_gift,
                                         dd.is_subscribed_consumer,
                                         dd.subtotal,
                                         dd.gov,
                                         dd.variable_profit
                         FROM public.dimension_deliveries dd
                              JOIN public.dimension_order_item do
                                 ON dd.delivery_id = do.delivery_id
                              LEFT JOIN catalog_service_prod.public.product_item cat
                                 ON cat.DD_BUSINESS_ID = do.BUSINESS_ID AND
                                    cat.MERCHANT_SUPPLIED_ID = do.MERCHANT_SUPPLIED_ID

                         WHERE 1 = 1
                           AND dd.business_id IN (331358, 11662180, 11116009)
                           AND cat.AISLE_NAME_L2 = 'Flowers'
                           AND dd.is_filtered_core = TRUE
                           AND dd.active_date BETWEEN '2024-01-01' AND CURRENT_DATE - 1
                         ORDER BY 1 DESC)


   , flower_delivs AS (SELECT dd.active_date,
                              dd.store_id,
                              dd.delivery_id,
                              dd.is_asap,
                              dd.is_gift,
                              dd.is_subscribed_consumer,
                              dd.subtotal,
                              dd.gov,
                              dd.variable_profit
                       FROM dimension_deliveries AS dd
                            JOIN flower_table AS f
                               ON dd.store_id = f.store_id
                       WHERE 1 = 1
                         AND dd.active_date BETWEEN '2024-01-01' AND CURRENT_DATE - 1
                         AND dd.is_filtered_core = TRUE
--    and f.type <> 'Shipped Flowers'
)
   , full_table AS (WITH agg_data AS (SELECT *
                                      FROM flower_delivs
                                      UNION
                                      SELECT *
                                      FROM dashmart_delivs)

                    SELECT dd.*,
                           CASE
                               WHEN fpcr.delivery_id IS NOT NULL AND fpcr.campaign_id IS NOT NULL THEN 'promo'
                               ELSE 'non_promo' END AS is_promo
                    FROM agg_data AS dd
                         LEFT JOIN fact_order_discounts_and_promotions fpcr
                            ON dd.delivery_id = fpcr.delivery_id)

   , daily_vol AS (SELECT DATE_TRUNC('day', dd.active_date)                                               AS Week_of,
                          COUNT(DISTINCT dd.store_id)                                                     AS Act_Stores,
                          COUNT(DISTINCT dd.delivery_id)                                                  AS Vol,
                          Vol / act_stores                                                                AS Act_OSW,
                          COUNT(DISTINCT CASE WHEN dd.is_asap = 1 THEN dd.delivery_id END)                AS ASAP,
                          COUNT(DISTINCT CASE WHEN dd.is_asap = 0 THEN dd.delivery_id END)                AS Sched,
                          COUNT(DISTINCT CASE WHEN dd.is_gift = 1 THEN dd.delivery_id END)                AS Gift,
                          COUNT(DISTINCT CASE WHEN dd.is_gift = 0 THEN dd.delivery_id END)                AS Non_Gift,
                          COUNT(DISTINCT CASE WHEN dd.is_promo = 'promo' THEN dd.delivery_id END)         AS promo,
                          COUNT(DISTINCT CASE WHEN dd.is_promo = 'non_promo' THEN dd.delivery_id END)     AS non_promo,
                          ROUND(promo / vol, 4)                                                           AS promo_pct,
                          COUNT(DISTINCT CASE WHEN dd.is_subscribed_consumer = 1 THEN dd.delivery_id END) AS DP,
                          COUNT(DISTINCT CASE WHEN dd.is_subscribed_consumer = 0 THEN dd.delivery_id END) AS Non_DP,
                          SUM(dd.subtotal) * .01                                                          AS Subtotal,
                          SUM(dd.subtotal) * .01 / vol                                                    AS avg_subtotal,
                          SUM(dd.gov) * .01                                                               AS GOV,
                          SUM(dd.gov) * .01 / vol                                                         AS AOV,
                          SUM(dd.variable_profit) * 0.01                                                  AS VP

                   FROM full_table AS dd
                   WHERE 1 = 1
                   GROUP BY 1
                   ORDER BY 1 DESC)

SELECT *
FROM daily_vol;

GRANT SELECT ON TABLE dianedou.flower_daily_volume TO READ_ONLY_USERS;

---active DP vs classic users % placed 1+ flower orders
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
                        AND nvs.business_vertical_id IN (141) --364, 331, 430, 10000794, 332, 266, 333, 265, 334, 267)
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
                        AND ds.is_active = TRUE
                        AND ds.country_id = 1
                        AND ds.business_id IN (749869) --and ds.business_id in (749869, 331358, 979026, 11116009)
)

   , dashmart_delivs AS (SELECT DISTINCT dd.active_date,
                                         dd.store_id,
                                         dd.delivery_id,
                                         dd.is_asap,
                                         dd.is_gift,
                                         dd.is_subscribed_consumer,
                                         dd.subtotal,
                                         dd.gov,
                                         dd.variable_profit
                         FROM public.dimension_deliveries dd
                              JOIN public.dimension_order_item do
                                 ON dd.delivery_id = do.delivery_id
                              LEFT JOIN catalog_service_prod.public.product_item cat
                                 ON cat.DD_BUSINESS_ID = do.BUSINESS_ID AND
                                    cat.MERCHANT_SUPPLIED_ID = do.MERCHANT_SUPPLIED_ID

                         WHERE 1 = 1
                           AND dd.business_id IN (331358, 11662180, 11116009)
                           AND cat.AISLE_NAME_L2 = 'Flowers'
                           AND dd.is_filtered_core = TRUE
                           AND dd.active_date BETWEEN '2023-02-09' AND '2023-02-15'
                         ORDER BY 1 DESC)


   , flower_delivs AS (SELECT dd.active_date,
                              dd.store_id,
                              dd.delivery_id,
                              dd.is_asap,
                              dd.is_gift,
                              dd.is_subscribed_consumer,
                              dd.subtotal,
                              dd.gov,
                              dd.variable_profit
                       FROM dimension_deliveries AS dd
                            JOIN flower_table AS f
                               ON dd.store_id = f.store_id
                       WHERE 1 = 1
                         AND dd.active_date BETWEEN '2023-02-09' AND '2023-02-15'
                         AND dd.is_filtered_core = TRUE
--    and f.type <> 'Shipped Flowers'
)
   , full_table AS (WITH agg_data AS (SELECT *
                                      FROM flower_delivs
                                      UNION
                                      SELECT *
                                      FROM dashmart_delivs)

                    SELECT dd.*,
                           CASE
                               WHEN fpcr.delivery_id IS NOT NULL AND fpcr.campaign_id IS NOT NULL THEN 'promo'
                               ELSE 'non_promo' END AS is_promo
                    FROM agg_data AS dd
                         LEFT JOIN fact_order_discounts_and_promotions fpcr
                            ON dd.delivery_id = fpcr.delivery_id)


SELECT dd.is_subscribed_consumer
     , COUNT(DISTINCT CASE WHEN a.DELIVERY_ID IS NOT NULL THEN creator_id END)
     , COUNT(DISTINCT creator_id)
FROM PRODDB.PUBLIC.dimension_deliveries dd
     LEFT JOIN full_table a
        ON dd.DELIVERY_ID = a.DELIVERY_ID

WHERE is_filtered_core = TRUE
  AND is_caviar = 0
  AND dd.active_date BETWEEN '2023-02-09' AND '2023-02-15'
GROUP BY 1;

---% grocery volume

CREATE OR REPLACE TABLE dianedou.Vday_2024_flower_deliveries AS
WITH ENT AS (SELECT DISTINCT l.business_group_id
                           , g.name AS business_group_name
                           --  , count(*)
                           , l.business_id
                           , s.business_name
             FROM doordash_merchant.public.maindb_businesss_group_link l
                  JOIN (SELECT *
                        FROM doordash_merchant.public.maindb_business_group
                        WHERE 1 = 1
                          AND is_test = FALSE
                          AND business_group_type IN ('ENTERPRISE')) g
                     ON g.id = l.business_group_id
                  LEFT JOIN static.enterprise_ownership eo
                     ON eo.business_group_id = l.business_group_id
                  JOIN DOORDASH_MERCHANT.PUBLIC.MAINDB_BUSINESS bv
                     ON l.business_id = bv.id
                  JOIN public.dimension_store s
                     ON l.business_id = s.business_id
             WHERE bv.business_vertical_id = 141)

   , flower_store_table AS (SELECT *
                            FROM (SELECT ds.store_id,
                                         ds.name,
                                         ds.business_id,
                                         ds.business_name,
                                         ds.submarket_id,
                                         ds.submarket_name,
                                         ds.country_id,
                                         'https://doordash.com/store/' || ds.store_id AS hyperlink,
                                         CASE
                                             WHEN ds.store_id IN
                                                  (2863075, 2243949, 23025694, 2195964, 23026199, 2238210, 23025499,
                                                   22983505,
                                                   22978711, 2195964, 2243949, 22953520) THEN 'Shipped Flowers'
                                             WHEN ds.business_id IN (699568) THEN 'FTD Grocery'
                                             --                                              WHEN ds.store_id IN
--                                                   (1480889, 1480887, 1480895, 1480859, 1480871, 1480897, 1480890,
--                                                    1480909,
--                                                    1480898, 1480885, 1480878, 1480905, 1480867, 1480880, 1480876,
--                                                    1480883,
--                                                    1480866, 1480870, 1480875, 1480873) THEN 'Family Flowers'
                                             WHEN t.management_type = 'UNMANAGED' AND ds.business_id <> 852091
                                                 THEN 'SMB Florist' --'Pure Play Florist'
                                             WHEN t.management_type = 'ENTERPRISE' AND
                                                  ds.business_id IN (SELECT business_id
                                                                     FROM ent
                                                                     WHERE BUSINESS_GROUP_NAME = 'FTD Floral'
                                                                       AND business_id <> '699568')
                                                 THEN 'FTD Independent'
                                             WHEN (t.management_type = 'ENTERPRISE' OR ds.business_id = 852091) AND
                                                  (ds.business_id <> 11540341)
                                                 THEN 'Grocery'
                                             WHEN ds.business_id IN (11540341) THEN 'Edible'
                                             END                                      AS TYPE,
                                         ds.is_active
                                  FROM DOORDASH_MERCHANT.PUBLIC.MAINDB_BUSINESS AS b
                                       JOIN dimension_store AS ds
                                          ON ds.business_id = b.id
                                       JOIN dimension_store_ext AS t
                                          ON t.store_id = ds.store_id
                                  WHERE 1 = 1
                                    AND b.business_vertical_id IN (141))
                            UNION
                            SELECT DISTINCT ds.store_id,
                                            ds.name,
                                            ds.business_id,
                                            ds.business_name,
                                            ds.submarket_id,
                                            ds.submarket_name,
                                            ds.country_id,
                                            'https://doordash.com/store/' || ds.store_id AS hyperlink,
                                            '1P'                                         AS type,
                                            ds.is_active
                            FROM dimension_store AS ds
                            WHERE 1 = 1
                              AND ds.is_active = TRUE
                              AND ds.country_id = 1
                              AND ds.business_id IN (749869)) --FBG

   , flower_deliveries AS (SELECT DISTINCT ds.type AS Store_category, dd.*
                           FROM public.dimension_deliveries dd
                                JOIN (SELECT DISTINCT type, BUSINESS_NAME, store_id FROM flower_store_table) ds
                                   ON dd.store_id = ds.store_id
                           WHERE 1 = 1
                             AND dd.is_filtered_core = 1
                             AND dd.active_date BETWEEN '2024-02-05' AND '2024-02-15')


   , dashmart_deliveries AS (SELECT DISTINCT '1P' AS Store_category, dd.*
                             FROM public.dimension_deliveries dd
                                  JOIN public.dimension_order_item do
                                     ON dd.delivery_id = do.delivery_id
                                  LEFT JOIN catalog_service_prod.public.product_item cat
                                     ON cat.DD_BUSINESS_ID = do.BUSINESS_ID AND
                                        cat.MERCHANT_SUPPLIED_ID = do.MERCHANT_SUPPLIED_ID

                             WHERE 1 = 1
                               AND dd.business_id IN (331358, 11662180, 11116009) --Dashmart, neighborhood grocery
                               AND cat.AISLE_NAME_L2 = 'Flowers'
                               AND dd.is_filtered_core = TRUE
                               AND dd.active_date BETWEEN '2024-02-05' AND '2024-02-15'
                             ORDER BY 1 DESC)

   , cng_deliveries AS (SELECT distinct '3P-Convenience' as Store_category, d.*

                    FROM EDW.CNG.FACT_NON_RX_ORDER_ITEM_DETAILS i
                         JOIN PRODDB.TABLEAU.NEW_VERTICALS_STORES nv
                            ON i.STORE_ID = nv.STORE_ID
                        AND nv.CNG_BUSINESS_LINE = '3P Convenience' AND nv.COUNTRY_CODE = 'US'
                         JOIN PRODDB.PUBLIC.DIMENSION_DELIVERIES d
                            ON i.DELIVERY_ID = d.DELIVERY_ID
                    WHERE 1 = 1
                      AND (
                        (i.business_id IN (412816) AND (i.ITEM_MERCHANT_SUPPLIED_ID IN
                                                        ('587968', '233855', '661822', '393210', '607638', '920801',
                                                         '690643', '278693', '255317', '180733', '678138', '319511',
                                                         '461538', '390139', '741312', '200777', '143035', '436909',
                                                         '636009', '508068', '340278', '632592', '494876', '435112',
                                                         '337710', '435101', '948230', '711138', '713029', '652338',
                                                         '583989', '603434', '881414', '124189', '418690', '430448',
                                                         '872841'))) -- CVS
                            OR (i.business_id IN (434024, 586904) AND (i.ITEM_MERCHANT_SUPPLIED_ID IN
                                                                       ('662786', '204620', '250529', '881238',
                                                                        '204621', '250532', '317477', '317483',
                                                                        '250522', '250524', '880551', '250525',
                                                                        '284186', '284187', '284185', '284184',
                                                                        '284183', '284188'))) -- WAG
                            OR (i.business_id IN (715696, 912841) AND
                                (i.ITEM_MERCHANT_SUPPLIED_ID IN ('270415', '2230189', '2203416', '5086287'))) -- RA
                            OR (i.business_id IN (799015, 847462, 847463) AND
                                (i.ITEM_MERCHANT_SUPPLIED_ID IN ('37999001', '37999101'))) -- DG/DGX/pOpshelf
                        )
                      AND d.IS_FILTERED_CORE = TRUE
                      AND d.ACTIVE_DATE BETWEEN '2024-02-01' AND '2024-02-14'
                      AND nv.BUSINESS_VERTICAL_ID = 100
                    )

   , deliveries AS (SELECT *
                    FROM flower_deliveries
                    UNION
                    SELECT *
                    FROM dashmart_deliveries
                    UNION
                    SELECT *
                    FROM cng_deliveries)

SELECT *
FROM deliveries;

CREATE OR REPLACE TABLE dianedou.Vday_2023_flower_deliveries AS
WITH ENT AS (SELECT DISTINCT l.business_group_id
                           , g.name AS business_group_name
                           --  , count(*)
                           , l.business_id
                           , s.business_name
             FROM doordash_merchant.public.maindb_businesss_group_link l
                  JOIN (SELECT *
                        FROM doordash_merchant.public.maindb_business_group
                        WHERE 1 = 1
                          AND is_test = FALSE
                          AND business_group_type IN ('ENTERPRISE')) g
                     ON g.id = l.business_group_id
                  LEFT JOIN static.enterprise_ownership eo
                     ON eo.business_group_id = l.business_group_id
                  JOIN DOORDASH_MERCHANT.PUBLIC.MAINDB_BUSINESS bv
                     ON l.business_id = bv.id
                  JOIN public.dimension_store s
                     ON l.business_id = s.business_id
             WHERE bv.business_vertical_id = 141)

   , flower_store_table AS (SELECT *
                            FROM (SELECT ds.store_id,
                                         ds.name,
                                         ds.business_id,
                                         ds.business_name,
                                         ds.submarket_id,
                                         ds.submarket_name,
                                         ds.country_id,
                                         'https://doordash.com/store/' || ds.store_id AS hyperlink,
                                         CASE
                                             WHEN ds.store_id IN
                                                  (2863075, 2243949, 23025694, 2195964, 23026199, 2238210, 23025499,
                                                   22983505,
                                                   22978711, 2195964, 2243949, 22953520) THEN 'Shipped Flowers'
                                             WHEN ds.business_id IN (699568) THEN 'FTD Grocery'
                                             --                                              WHEN ds.store_id IN
--                                                   (1480889, 1480887, 1480895, 1480859, 1480871, 1480897, 1480890,
--                                                    1480909,
--                                                    1480898, 1480885, 1480878, 1480905, 1480867, 1480880, 1480876,
--                                                    1480883,
--                                                    1480866, 1480870, 1480875, 1480873) THEN 'Family Flowers'
                                             WHEN t.management_type = 'UNMANAGED' AND ds.business_id <> 852091
                                                 THEN 'SMB Florist' --'Pure Play Florist'
                                             WHEN t.management_type = 'ENTERPRISE' AND
                                                  ds.business_id IN (SELECT business_id
                                                                     FROM ent
                                                                     WHERE BUSINESS_GROUP_NAME = 'FTD Floral'
                                                                       AND business_id <> '699568')
                                                 THEN 'FTD Independent'
                                             WHEN (t.management_type = 'ENTERPRISE' OR ds.business_id = 852091) AND
                                                  (ds.business_id <> 11540341)
                                                 THEN 'Grocery'
                                             WHEN ds.business_id IN (11540341) THEN 'Edible'
                                             END                                      AS TYPE,
                                         ds.is_active
                                  FROM DOORDASH_MERCHANT.PUBLIC.MAINDB_BUSINESS AS b
                                       JOIN dimension_store AS ds
                                          ON ds.business_id = b.id
                                       JOIN dimension_store_ext AS t
                                          ON t.store_id = ds.store_id
                                  WHERE 1 = 1
                                    AND b.business_vertical_id IN (141))
                            UNION
                            SELECT DISTINCT ds.store_id,
                                            ds.name,
                                            ds.business_id,
                                            ds.business_name,
                                            ds.submarket_id,
                                            ds.submarket_name,
                                            ds.country_id,
                                            'https://doordash.com/store/' || ds.store_id AS hyperlink,
                                            '1P'                                         AS type,
                                            ds.is_active
                            FROM dimension_store AS ds
                            WHERE 1 = 1
                              AND ds.is_active = TRUE
                              AND ds.country_id = 1
                              AND ds.business_id IN (749869)) --FBG

   , flower_deliveries AS (SELECT DISTINCT ds.type AS Store_category, dd.*
                           FROM public.dimension_deliveries dd
                                JOIN (SELECT DISTINCT type, BUSINESS_NAME, store_id FROM flower_store_table) ds
                                   ON dd.store_id = ds.store_id
                           WHERE 1 = 1
                             AND dd.is_filtered_core = 1
                             AND dd.active_date BETWEEN '2023-02-05' AND '2023-02-15')


   , dashmart_deliveries AS (SELECT DISTINCT '1P' AS Store_category, dd.*
                             FROM public.dimension_deliveries dd
                                  JOIN public.dimension_order_item do
                                     ON dd.delivery_id = do.delivery_id
                                  LEFT JOIN catalog_service_prod.public.product_item cat
                                     ON cat.DD_BUSINESS_ID = do.BUSINESS_ID AND
                                        cat.MERCHANT_SUPPLIED_ID = do.MERCHANT_SUPPLIED_ID

                             WHERE 1 = 1
                               AND dd.business_id IN (331358, 11662180, 11116009) --Dashmart, neighborhood grocery
                               AND cat.AISLE_NAME_L2 = 'Flowers'
                               AND dd.is_filtered_core = TRUE
                               AND dd.active_date BETWEEN '2023-02-05' AND '2023-02-15'
                             ORDER BY 1 DESC)

   , cng_deliveries AS (SELECT distinct '3P-Convenience' as Store_category, d.*

                    FROM EDW.CNG.FACT_NON_RX_ORDER_ITEM_DETAILS i
                         JOIN PRODDB.TABLEAU.NEW_VERTICALS_STORES nv
                            ON i.STORE_ID = nv.STORE_ID
                        AND nv.CNG_BUSINESS_LINE = '3P Convenience' AND nv.COUNTRY_CODE = 'US'
                         JOIN PRODDB.PUBLIC.DIMENSION_DELIVERIES d
                            ON i.DELIVERY_ID = d.DELIVERY_ID
                    WHERE 1 = 1
                      AND (
                        (i.business_id IN (412816) AND (i.ITEM_MERCHANT_SUPPLIED_ID IN
                                                        ('587968', '233855', '661822', '393210', '607638', '920801',
                                                         '690643', '278693', '255317', '180733', '678138', '319511',
                                                         '461538', '390139', '741312', '200777', '143035', '436909',
                                                         '636009', '508068', '340278', '632592', '494876', '435112',
                                                         '337710', '435101', '948230', '711138', '713029', '652338',
                                                         '583989', '603434', '881414', '124189', '418690', '430448',
                                                         '872841'))) -- CVS
                            OR (i.business_id IN (434024, 586904) AND (i.ITEM_MERCHANT_SUPPLIED_ID IN
                                                                       ('662786', '204620', '250529', '881238',
                                                                        '204621', '250532', '317477', '317483',
                                                                        '250522', '250524', '880551', '250525',
                                                                        '284186', '284187', '284185', '284184',
                                                                        '284183', '284188'))) -- WAG
                            OR (i.business_id IN (715696, 912841) AND
                                (i.ITEM_MERCHANT_SUPPLIED_ID IN ('270415', '2230189', '2203416', '5086287'))) -- RA
                            OR (i.business_id IN (799015, 847462, 847463) AND
                                (i.ITEM_MERCHANT_SUPPLIED_ID IN ('37999001', '37999101'))) -- DG/DGX/pOpshelf
                        )
                      AND d.IS_FILTERED_CORE = TRUE
                      AND d.ACTIVE_DATE BETWEEN '2023-02-01' AND '2023-02-14'
                      AND nv.BUSINESS_VERTICAL_ID = 100
                    )

   , deliveries AS (SELECT *
                    FROM flower_deliveries
                    UNION
                    SELECT *
                    FROM dashmart_deliveries
                    UNION
                    SELECT *
                    FROM cng_deliveries)

SELECT *
FROM deliveries;

CREATE OR REPLACE TABLE dianedou.Vday_2024_flower_vol_by_store_category AS

SELECT
--     CAST(DATE_TRUNC('day', dd.active_date) AS DATE)                                 AS Day_of,
       dd.Store_category                                                               AS Store_category,
       case when dd.BUSINESS_NAME ILIKE 'Bloom haus%' then 'Kroger' else dd.BUSINESS_NAME END as Business,
       COUNT(DISTINCT dd.store_id)                                                     AS Act_Stores,
       COUNT(DISTINCT dd.delivery_id)                                                  AS Vol,
       Vol / act_stores                                                                AS Act_OSD,
       COUNT(DISTINCT CASE WHEN dd.is_asap = 1 THEN dd.delivery_id END)                AS ASAP,
       COUNT(DISTINCT CASE WHEN dd.is_asap = 0 THEN dd.delivery_id END)                AS Sched,
       COUNT(DISTINCT CASE WHEN dd.is_gift = 1 THEN dd.delivery_id END)                AS Gift,
       COUNT(DISTINCT CASE WHEN dd.is_gift = 0 THEN dd.delivery_id END)                AS Non_Gift,
       COUNT(DISTINCT CASE WHEN dd.is_subscribed_consumer = 1 THEN dd.delivery_id END) AS DP,
       COUNT(DISTINCT CASE WHEN dd.is_subscribed_consumer = 0 THEN dd.delivery_id END) AS Non_DP,
       COUNT(DISTINCT CASE
                          WHEN fpcr.delivery_id IS NOT NULL
                              THEN dd.delivery_id END)                                 AS promo_vol,
       COUNT(DISTINCT CASE
                          WHEN fpcr.delivery_id IS NULL
                              THEN dd.delivery_id END)                                 AS nonpromo_vol,
       SUM(dd.subtotal) * .01                                                          AS Subtotal,
       SUM(dd.gov) * 0.01                                                              AS GOV,
       SUM(dd.GOV) * .01 / vol                                                         AS AOV,
       SUM(dd.variable_profit) * 0.01                                                  AS VP,
       VP / vol                                                                        AS UE
FROM dianedou.Vday_2024_flower_deliveries dd
     LEFT JOIN fact_promo_code_redemptions fpcr
        ON dd.delivery_id = fpcr.delivery_id AND fpcr.is_fmx = 0 AND
           (fpcr.code IS NOT NULL OR fpcr.campaign_id IS NOT NULL)
WHERE 1 = 1
  AND dd.is_filtered_core = 1
  AND dd.active_date BETWEEN '2024-02-05' AND '2024-02-15'
GROUP BY 1, 2
ORDER BY 1 DESC
;


GRANT SELECT ON TABLE dianedou.Vday_2024_flower_deliveries TO READ_ONLY_USERS;
GRANT SELECT ON TABLE dianedou.Vday_2024_flower_vol_by_store_category TO READ_ONLY_USERS;

SELECT STORE_CATEGORY
     , SUM(Vol)
     , SUM(GOV)
     , SUM(GOV) / SUM(Vol) AS AOV

FROM dianedou.Vday_2024_flower_vol_by_store_category
GROUP BY 1
;


SELECT STORE_CATEGORY
--      , BUSINESS_NAME
     , COUNT(DISTINCT DELIVERY_ID)
     , SUM(GOV)
     , COUNT (DISTINCT CASE WHEN CANCELLED_AT is not null then DELIVERY_ID end) as cancelled_orders_cnt

FROM dianedou.Vday_2024_flower_deliveries
GROUP BY 1
;

