-- WITH ENT AS (SELECT DISTINCT l.business_group_id
--                            , g.name AS business_group_name
--                            --  , count(*)
--                            , l.business_id
--                            , s.business_name
--              FROM doordash_merchant.public.maindb_businesss_group_link l
--                   JOIN (SELECT *
--                         FROM doordash_merchant.public.maindb_business_group
--                         WHERE 1 = 1
--                           AND is_test = FALSE
--                           AND business_group_type IN ('ENTERPRISE')) g
--                      ON g.id = l.business_group_id
--                   LEFT JOIN static.enterprise_ownership eo
--                      ON eo.business_group_id = l.business_group_id
--                   JOIN DOORDASH_MERCHANT.PUBLIC.MAINDB_BUSINESS bv
--                      ON l.business_id = bv.id
--                   JOIN public.dimension_store s
--                      ON l.business_id = s.business_id
--              WHERE bv.business_vertical_id = 141
--                AND s.is_active = 1)
--
--    , flower_table AS (SELECT *
--                       FROM (SELECT ds.store_id,
--                                    ds.name,
--                                    ds.business_id,
--                                    ds.business_name,
--                                    ds.submarket_id,
--                                    ds.submarket_name,
--                                    ds.country_id,
--                                    'https://doordash.com/store/' || ds.store_id AS hyperlink,
--                                    CASE
--                                        WHEN ds.store_id IN
--                                             (2863075, 2243949, 23025694, 2195964, 23026199, 2238210, 23025499, 22983505,
--                                              22978711, 2195964, 2243949, 22953520) THEN 'Shipped Flowers'
--                                        WHEN ds.business_id IN (749869, 331358) THEN '1P'
--                                        WHEN ds.business_id IN (699568) THEN 'FTD Grocery'
--                                        WHEN ds.store_id IN
--                                             (1480889, 1480887, 1480895, 1480859, 1480871, 1480897, 1480890, 1480909,
--                                              1480898, 1480885, 1480878, 1480905, 1480867, 1480880, 1480876, 1480883,
--                                              1480866, 1480870, 1480875, 1480873) THEN 'Family Flowers'
--                                        WHEN t.management_type = 'UNMANAGED' AND ds.business_id <> 852091
--                                            THEN 'Pure Play Florist'
--                                        WHEN t.management_type = 'ENTERPRISE' AND ds.business_id IN (SELECT business_id
--                                                                                                     FROM ent
--                                                                                                     WHERE BUSINESS_GROUP_NAME = 'FTD Floral'
--                                                                                                       AND business_id <> '699568')
--                                            THEN 'FTD Independent'
--                                        WHEN t.management_type = 'ENTERPRISE' OR ds.business_id = 852091 THEN 'Grocery'
--                                        END                                      AS TYPE,
--                                    ds.is_active
--                             FROM DOORDASH_MERCHANT.PUBLIC.MAINDB_BUSINESS AS b
--                                  JOIN dimension_store AS ds
--                                     ON ds.business_id = b.id
--                                  JOIN dimension_store_ext AS t
--                                     ON t.store_id = ds.store_id
--                             WHERE 1 = 1
--                               AND b.business_vertical_id IN (141)
--                               AND ds.is_active = TRUE
--                               AND ds.country_id = 1
--                               AND ds.store_id NOT IN
--                                   (2863075, 2243949, 23025694, 2195964, 23026199, 2238210, 23025499, 22983505, 22978711,
--                                    2195964, 2243949, 22953520)))
---store availability
CREATE OR REPLACE TABLE DIANEDOU.Vday_2024_STORE_AVAILABILITY AS
WITH sub AS (SELECT DISTINCT STORE_CATEGORY
                           , BUSINESS_ID
                           , BUSINESS_NAME
                           , STORE_ID

             FROM dianedou.Vday_2024_flower_deliveries)

   , store_list AS (SELECT f.STORE_CATEGORY,
                           BUSINESS_NAME,
                           c.store_menu_id                                                 AS store_id,
                           c.is_closed,
                           LPAD(FLOOR(c.start_time / 3600000.00, 0)::VARCHAR, 2, '0') || ':' ||
                           LPAD(FLOOR(c.start_time % 3600000 / 60000, 0)::VARCHAR, 2, '0') AS start_time,
                           LPAD(FLOOR(c.end_time / 3600000.00, 0)::VARCHAR, 2, '0') || ':' ||
                           LPAD(FLOOR(c.end_time % 3600000 / 60000, 0)::VARCHAR, 2, '0')   AS end_time
                    FROM menu.public.cassandra_store_level_hour_exception AS c
                         JOIN sub AS f
                            ON f.store_id = c.store_menu_id
                    WHERE 1 = 1
                      AND date = '2024-02-14'
                      AND is_deleted IS NULL
--                       AND f.type NOT IN ('1P')
                      AND is_closed = FALSE
--                       AND store_id NOT IN (SELECT store_id
--                                            FROM dimension_store
--                                            WHERE business_id IN
--                                                  (838007, 11259649, 11259660, 11259664, 11259662, 11259665, 11259654,
--                                                   11259656, 11259651, 11259672, 11259661, 11259658, 11259659, 11259668,
--                                                   11259666, 11259667))
)

   , all_hours AS (SELECT f.STORE_CATEGORY,
                          f.BUSINESS_NAME,
                          oh.MENU_ID,
                          oh.STORE_ID,
                          oh.DAY_INDEX,
                          oh.START_TIME,
                          oh.END_TIME
                   FROM PRODDB.PUBLIC.DIMENSION_MENU_OPEN_HOURS oh
                        JOIN sub AS f
                           ON f.store_id = oh.store_id
                   WHERE 1 = 1
                     AND oh.day_index = '2'
--                      AND store_id NOT IN (SELECT store_id
--                                           FROM dimension_store
--                                           WHERE business_id IN
--                                                 (838007, 11259649, 11259660, 11259664, 11259662, 11259665, 11259654,
--                                                  11259656, 11259651, 11259672, 11259661, 11259658, 11259659, 11259668,
--                                                  11259666, 11259667)
)

   , time_table AS (SELECT DISTINCT a.STORE_CATEGORY,
                                    a.BUSINESS_NAME,
                                    a.store_id,
                                    l.start_time AS special_start_time,
                                    l.end_time   AS special_end_time,
                                    a.menu_id,
                                    a.start_time,
                                    a.end_time

                    FROM all_hours AS a
                         LEFT JOIN store_list AS l
                            ON l.store_id = a.store_id
--                     WHERE 1 = 1
--                       AND l.store_id NOT IN (SELECT store_id
--                                              FROM dimension_store
--                                              WHERE business_id IN
--                                                    (838007, 11259649, 11259660, 11259664, 11259662, 11259665, 11259654,
--                                                     11259656, 11259651,
--                                                     11259672, 11259661, 11259658, 11259659, 11259668, 11259666,
--                                                     11259667))
)


SELECT *
FROM time_table;

GRANT SELECT ON TABLE DIANEDOU.Vday_2024_STORE_AVAILABILITY TO READ_ONLY_USERS;

WITH sub AS (SELECT STORE_CATEGORY
                  , STORE_ID
                  , CASE WHEN BUSINESS_NAME ILIKE 'Bloom Haus%' THEN 'Kroger' ELSE BUSINESS_NAME END AS business
                  , COALESCE(SPECIAL_START_TIME, START_TIME)                                         AS real_start_time
                  , COALESCE(SPECIAL_END_TIME, END_TIME)                                             AS real_end_time
                  , CASE WHEN SPECIAL_START_TIME IS NOT NULL THEN 1 END                              AS updated_hour_flag
                  , DATEDIFF('hour', real_START_TIME::TIME, real_end_time::TIME)                     AS hours_open
             FROM DIANEDOU.Vday_2024_STORE_AVAILABILITY)

SELECT STORE_CATEGORY
     , AVG(CASE WHEN hours_open = 0 THEN 24 ELSE hours_open END)         AS avg_hours_open
     , COUNT(DISTINCT store_id)                                          AS num_stores
     , COUNT(DISTINCT CASE WHEN updated_hour_flag = 1 THEN store_id END) AS num_stores_updated_hours
     , num_stores_updated_hours / num_stores
FROM sub
GROUP BY 1;


--affordability
WITH sub AS (SELECT  dd.STORE_CATEGORY
             , dd.BUSINESS_NAME
             , dd.DELIVERY_ID
             , dd.SUBTOTAL
             , do.ITEM_NAME
             , do.UNIT_PRICE
             , do.SUBTOTAL as item_subtotal
             FROM dianedou.Vday_2024_flower_deliveries dd
                  JOIN public.dimension_order_item do
                     ON dd.delivery_id = do.delivery_id
                  LEFT JOIN catalog_service_prod.public.product_item cat
                     ON cat.DD_BUSINESS_ID = do.BUSINESS_ID AND
                        cat.MERCHANT_SUPPLIED_ID = do.MERCHANT_SUPPLIED_ID

             WHERE 1 = 1
               AND cat.AISLE_NAME_L2 = 'Flowers'
--                AND dd.is_filtered_core = TRUE

             ORDER BY 1 DESC)

select STORE_CATEGORY
, BUSINESS_NAME
, avg(subtotal) as avg_order_subtotal
, avg(UNIT_PRICE)
, avg(item_subtotal)
from sub
group by all
;




SELECT *
FROM PUBLIC.DIMENSION_MENU_ITEM m
     JOIN dimension_store ds
        ON ds.store_id = m.store_id
WHERE ds.business_id IN (
                         838007,
                         11259649,
                         11259660,
                         11259664,
                         11259662,
                         11259665,
                         11259654,
                         11259656,
                         11259651,
                         11259672,
                         11259661,
                         11259658,
                         11259659,
                         11259668,
                         11259666,
                         11259667
    )
  AND ds.is_active = TRUE
  AND m.is_active = TRUE
