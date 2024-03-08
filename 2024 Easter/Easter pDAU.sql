CREATE OR REPLACE TABLE dianedou.vday_dau AS
SELECT CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', ddo.created_at)::DATE          AS created_at_pst,
       dd.creator_id,
       CASE
           WHEN COALESCE(mh.days_since_last_purchase, -1) BETWEEN 0 AND 28 THEN 'Active'
           WHEN COALESCE(mh.days_since_last_purchase, -1) BETWEEN 29 AND 90 THEN 'Dormant'
           WHEN COALESCE(mh.days_since_last_purchase, -1) > 90 THEN 'Churned'
           ELSE 'New'
           END                                                                       AS mp_segment,
       DAYNAME(dd.active_date)                                                       AS dow,
       CASE
           WHEN MIN(CASE WHEN nv.vertical_name IS NOT NULL THEN dd.order_number_nv END) = 1 THEN 'New'
           ELSE 'Existing' END                                                       AS new_cx_nv,
       CASE WHEN MIN(dd.order_number_marketplace) = 1 THEN 'New' ELSE 'Existing' END AS new_cx_dd,
       MAX(CASE WHEN nv.vertical_name IS NOT NULL THEN 1 ELSE 0 END)                 AS has_nv_order,
       MAX(CASE WHEN nv.vertical_name IS NULL THEN 1 ELSE 0 END)                     AS has_rx_order,
       MAX(CASE WHEN v.delivery_id IS NOT NULL THEN 1 ELSE 0 END)                    AS has_vday_order,
       MAX(CASE
               WHEN nv.vertical_name = 'Flowers' OR nv.business_name = 'The Flower & Gift Boutique' THEN 1
               ELSE 0 END)                                                           AS has_flowers_fgb_order,
       MAX(CASE WHEN vl.delivery_id IS NOT NULL THEN 1 ELSE 0 END)                   AS has_vday_l1_order,
       MAX(CASE WHEN ddo.is_asap = FALSE THEN 1 ELSE 0 END)                          AS has_scheduled_order
FROM stefaniemontgomery.dimension_deliveries_ranked dd
     LEFT JOIN edw.cng.dimension_new_vertical_store_tags nv
        ON nv.store_id = dd.store_id
    AND is_filtered_mp_vertical = 1
     LEFT JOIN mattheitz.mh_customer_authority mh
        ON mh.creator_id = dd.creator_id
    AND mh.dte = dd.active_date
     LEFT JOIN stefaniemontgomery._vday24_orders v
        ON v.delivery_id = dd.delivery_id
     LEFT JOIN public.dimension_deliveries ddo
        ON dd.delivery_id = ddo.delivery_id
     LEFT JOIN stefaniemontgomery._vday24_orders_l1 vl
        ON vl.delivery_id = dd.delivery_id
WHERE dd.country_id = 1
  AND (created_at_pst::DATE BETWEEN '2024-02-01'::DATE AND '2024-02-15'::DATE OR
       created_at_pst::DATE BETWEEN '2023-02-01'::DATE AND '2023-02-15'::DATE)
GROUP BY ALL
;

SELECT created_at_pst,
       YEAR(created_at_pst)                        AS dte_year,
       TO_CHAR(created_at_pst, 'MM-DD')            AS dte_mm_dd,
       dow,
       mp_segment,
       has_nv_order,
       has_rx_order,
       has_vday_order                              AS has_flowers_order,
       GREATEST(has_vday_order, has_vday_l1_order) AS has_vday_order,
       has_scheduled_order,
       SUM(1)                                      AS count_cx
FROM stefaniemontgomery.vday_dau
-- where has_nv_order = 1
GROUP BY ALL
;

CREATE OR REPLACE TABLE DIANEDOU.temp_easter_dau_p0 AS
WITH daily_volume AS (SELECT i.DELIVERY_CREATED_AT::DATE                     AS dt,
                             DATE_TRUNC('week', i.DELIVERY_CREATED_AT::DATE) AS week_cohort,
                             i.DELIVERY_ID,
                             i.CREATOR_ID,
                             SUM(i.quantity_requested)                       AS item_quantity,
                             SUM(i.TOTAL_ITEM_PRICE / 100)                   AS item_price
                      FROM edw.cng.fact_non_rx_order_item_details i
                           JOIN dianedou.easter_2024_item_list l
                              ON l.AISLE_NAME_L2 = i.AISLE_NAME_L2
                          AND ((i.delivery_created_at BETWEEN '2022-12-18' AND '2024-02-12') --OR
--                                (i.delivery_created_at BETWEEN '2023-12-18' AND '2024-02-12')
                                     )
                      --                            JOIN PRODDB.PUBLIC.DIMENSION_DELIVERIES dd
--                               ON i.DELIVERY_ID = dd.DELIVERY_ID
--                        WHERE p.product_category_id IN
--                              (SELECT DISTINCT PRODUCT_CATEGORY_ID FROM dianedou.easter_2024_item_list)

                      GROUP BY 1, 2, 3, 4
                      ORDER BY 1
                              DESC, 2)


SELECT CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', ddo.created_at)::DATE          AS created_at_pst,
       dd.creator_id,
       CASE
           WHEN COALESCE(mh.days_since_last_purchase, -1) BETWEEN 0 AND 28 THEN 'Active'
           WHEN COALESCE(mh.days_since_last_purchase, -1) BETWEEN 29 AND 90 THEN 'Dormant'
           WHEN COALESCE(mh.days_since_last_purchase, -1) > 90 THEN 'Churned'
           ELSE 'New'
           END                                                                       AS mp_segment,
       DAYNAME(dd.active_date)                                                       AS dow,
       CASE
           WHEN MIN(CASE WHEN nv.vertical_name IS NOT NULL THEN dd.order_number_nv END) = 1 THEN 'New'
           ELSE 'Existing' END                                                       AS new_cx_nv,
       CASE WHEN MIN(dd.order_number_marketplace) = 1 THEN 'New' ELSE 'Existing' END AS new_cx_dd,
       MAX(CASE WHEN nv.vertical_name IS NOT NULL THEN 1 ELSE 0 END)                 AS has_nv_order,
       MAX(CASE WHEN nv.vertical_name IS NULL THEN 1 ELSE 0 END)                     AS has_rx_order,
       MAX(CASE WHEN v.delivery_id IS NOT NULL THEN 1 ELSE 0 END)                    AS has_easter_order
--        MAX(CASE
--                WHEN nv.vertical_name = 'Flowers' OR nv.business_name = 'The Flower & Gift Boutique' THEN 1
--                ELSE 0 END)                                                           AS has_flowers_fgb_order,
--        MAX(CASE WHEN vl.delivery_id IS NOT NULL THEN 1 ELSE 0 END)                   AS has_vday_l1_order,
--        MAX(CASE WHEN ddo.is_asap = FALSE THEN 1 ELSE 0 END)                          AS has_scheduled_order
FROM (SELECT *
      FROM stefaniemontgomery.dimension_deliveries_ranked
      WHERE ((active_date BETWEEN '2022-12-18' AND '2024-02-12')
--           OR (active_date BETWEEN '2023-12-18' AND '2024-02-12')
                )) dd
     LEFT JOIN edw.cng.dimension_new_vertical_store_tags nv
        ON nv.store_id = dd.store_id
    AND is_filtered_mp_vertical = 1
     LEFT JOIN mattheitz.mh_customer_authority mh
        ON mh.creator_id = dd.creator_id
    AND mh.dte = DATE_TRUNC('week', dd.active_date)
     LEFT JOIN daily_volume v
        ON v.delivery_id = dd.delivery_id
     LEFT JOIN public.dimension_deliveries ddo
        ON dd.delivery_id = ddo.delivery_id
--      LEFT JOIN stefaniemontgomery._vday24_orders_l1 vl
--         ON vl.delivery_id = dd.delivery_id
WHERE dd.country_id = 1
  AND ((created_at_pst::DATE BETWEEN '2022-12-18' AND '2024-02-12')
--        (created_at_pst::DATE BETWEEN '2022-12-18' AND '2024-02-12')
    )
GROUP BY ALL
;

GRANT SELECT ON TABLE DIANEDOU.temp_easter_dau TO read_only_users
;

SELECT DATE_TRUNC(WEEK, created_at_pst)                                   AS dte,
--        mp_segment,
       COUNT(DISTINCT dd.creator_id)                                      AS wau,
       COUNT(DISTINCT CASE WHEN has_easter_order = 1 THEN creator_id END) AS easter_wau
FROM DIANEDOU.temp_easter_dau dd
GROUP BY ALL
;

WITH sub AS (SELECT 'last-year-baseline'                                               AS time_period,
                    DATE_TRUNC(WEEK, created_at_pst)                                   AS dte,
                    mp_segment,
                    COUNT(DISTINCT dd.creator_id)                                      AS wau,
                    COUNT(DISTINCT CASE WHEN has_easter_order = 1 THEN creator_id END) AS easter_wau,
                    easter_wau / wau                                                   AS pwau
             FROM DIANEDOU.temp_easter_dau_p0 dd
             WHERE created_at_pst BETWEEN '2022-12-19' AND '2023-02-12'
             GROUP BY ALL

             UNION ALL

             SELECT 'last-year-easter-week',
                    DATE_TRUNC(WEEK, created_at_pst)                                   AS dte,
                    mp_segment,
                    COUNT(DISTINCT dd.creator_id)                                      AS wau,
                    COUNT(DISTINCT CASE WHEN has_easter_order = 1 THEN creator_id END) AS easter_wau,
                    easter_wau / wau                                                   AS pwau
             FROM DIANEDOU.temp_easter_dau_p0 dd
             WHERE created_at_pst BETWEEN '2023-04-03' AND '2023-04-09'
             GROUP BY ALL

             UNION ALL

             SELECT 'last-year-easter-period',
                    DATE_TRUNC(WEEK, created_at_pst)                                   AS dte,
                    mp_segment,
                    COUNT(DISTINCT dd.creator_id)                                      AS wau,
                    COUNT(DISTINCT CASE WHEN has_easter_order = 1 THEN creator_id END) AS easter_wau,
                    easter_wau / wau                                                   AS pwau
             FROM DIANEDOU.temp_easter_dau_p0 dd
             WHERE created_at_pst BETWEEN '2023-03-31' AND '2023-04-09'
             GROUP BY ALL

             UNION ALL


             SELECT 'this-year-baseline',
                    DATE_TRUNC(WEEK, created_at_pst)                                   AS dte,
                    mp_segment,
                    COUNT(DISTINCT dd.creator_id)                                      AS wau,
                    COUNT(DISTINCT CASE WHEN has_easter_order = 1 THEN creator_id END) AS easter_wau,
                    easter_wau / wau                                                   AS pwau
             FROM DIANEDOU.temp_easter_dau_p0 dd
             WHERE created_at_pst BETWEEN '2023-12-18' AND '2024-02-11'
             GROUP BY ALL)

SELECT time_period
--      , MP_SEGMENT
--      , AVG(pwau)
, sum(wau)
, sum(easter_wau)
, sum(easter_wau) / sum(wau)
FROM sub
GROUP BY 1
;

CREATE OR REPLACE TABLE DIANEDOU.temp_easter_dau_region AS
WITH daily_volume AS (SELECT i.DELIVERY_CREATED_AT::DATE                     AS dt,
                             DATE_TRUNC('week', i.DELIVERY_CREATED_AT::DATE) AS week_cohort,
                             i.DELIVERY_ID,
                             i.CREATOR_ID,
                             SUM(i.quantity_requested)                       AS item_quantity,
                             SUM(i.TOTAL_ITEM_PRICE / 100)                   AS item_price
                      FROM edw.cng.fact_non_rx_order_item_details i
                           JOIN dianedou.easter_2024_item_list_p1 l
                              ON l.AISLE_NAME_L2 = i.AISLE_NAME_L2
                          AND ((i.delivery_created_at BETWEEN '2023-02-12' AND '2024-02-12') --OR
--                                (i.delivery_created_at BETWEEN '2023-12-18' AND '2024-02-12')
                                     )
                      --                            JOIN PRODDB.PUBLIC.DIMENSION_DELIVERIES dd
--                               ON i.DELIVERY_ID = dd.DELIVERY_ID
--                        WHERE p.product_category_id IN
--                              (SELECT DISTINCT PRODUCT_CATEGORY_ID FROM dianedou.easter_2024_item_list)

                      GROUP BY 1, 2, 3, 4
                      ORDER BY 1
                              DESC, 2)


SELECT CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', ddo.created_at)::DATE          AS created_at_pst,
       REGION_NAME,
       dd.creator_id,
       CASE
           WHEN COALESCE(mh.days_since_last_purchase, -1) BETWEEN 0 AND 28 THEN 'Active'
           WHEN COALESCE(mh.days_since_last_purchase, -1) BETWEEN 29 AND 90 THEN 'Dormant'
           WHEN COALESCE(mh.days_since_last_purchase, -1) > 90 THEN 'Churned'
           ELSE 'New'
           END                                                                       AS mp_segment,
       DAYNAME(dd.active_date)                                                       AS dow,
       CASE
           WHEN MIN(CASE WHEN nv.vertical_name IS NOT NULL THEN dd.order_number_nv END) = 1 THEN 'New'
           ELSE 'Existing' END                                                       AS new_cx_nv,
       CASE WHEN MIN(dd.order_number_marketplace) = 1 THEN 'New' ELSE 'Existing' END AS new_cx_dd,
       MAX(CASE WHEN nv.vertical_name IS NOT NULL THEN 1 ELSE 0 END)                 AS has_nv_order,
       MAX(CASE WHEN nv.vertical_name IS NULL THEN 1 ELSE 0 END)                     AS has_rx_order,
       MAX(CASE WHEN v.delivery_id IS NOT NULL THEN 1 ELSE 0 END)                    AS has_easter_order
--        MAX(CASE
--                WHEN nv.vertical_name = 'Flowers' OR nv.business_name = 'The Flower & Gift Boutique' THEN 1
--                ELSE 0 END)                                                           AS has_flowers_fgb_order,
--        MAX(CASE WHEN vl.delivery_id IS NOT NULL THEN 1 ELSE 0 END)                   AS has_vday_l1_order,
--        MAX(CASE WHEN ddo.is_asap = FALSE THEN 1 ELSE 0 END)                          AS has_scheduled_order
FROM (SELECT *
      FROM stefaniemontgomery.dimension_deliveries_ranked
      WHERE ((active_date BETWEEN '2023-02-12' AND '2024-02-12')
--           OR (active_date BETWEEN '2023-12-18' AND '2024-02-12')
                )) dd
     LEFT JOIN edw.cng.dimension_new_vertical_store_tags nv
        ON nv.store_id = dd.store_id
    AND is_filtered_mp_vertical = 1
     LEFT JOIN mattheitz.mh_customer_authority mh
        ON mh.creator_id = dd.creator_id
    AND mh.dte = DATE_TRUNC('week', dd.active_date)
     LEFT JOIN daily_volume v
        ON v.delivery_id = dd.delivery_id
     LEFT JOIN public.dimension_deliveries ddo
        ON dd.delivery_id = ddo.delivery_id
     LEFT JOIN public.fact_region re
        ON ddo.REGION_ID = re.REGION_ID
--      LEFT JOIN stefaniemontgomery._vday24_orders_l1 vl
--         ON vl.delivery_id = dd.delivery_id
WHERE dd.country_id = 1
  AND ((created_at_pst::DATE BETWEEN '2023-02-12' AND '2024-02-12')
--        (created_at_pst::DATE BETWEEN '2022-12-18' AND '2024-02-12')
    )
GROUP BY ALL
;


GRANT SELECT ON TABLE DIANEDOU.temp_easter_dau_region TO read_only_users;

SELECT re.REGION_NAME
, count(distinct ddo.DELIVERY_ID)

FROM (select * from public.dimension_deliveries where  DATE_TRUNC('week', active_date) = '2023-04-03') ddo
     LEFT JOIN public.fact_region re
        ON ddo.REGION_ID = re.REGION_ID
group by 1