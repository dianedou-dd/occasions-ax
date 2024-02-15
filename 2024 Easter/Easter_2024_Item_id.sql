CREATE OR REPLACE TABLE DIANEDOU.easter_2024_item_list AS
WITH data AS (SELECT DISTINCT p.MERCHANT_SUPPLIED_ID                                                               AS "itemMerchantSuppliedId",
                              p.dd_sic                                                                             AS dd_sic,
                              CASE
                                  WHEN p.DD_SIC IS NULL THEN p.ITEM_NAME
                                  ELSE g.INTERNAL_INFORMATION:navigation_component:ITEM_NAME:value::STRING END     AS item_name,
                              ds.NAME                                                                              AS business_name,
                              DD_BUSINESS_ID                                                                       AS "businessId",
                              ds.BUSINESS_VERTICAL_ID,
                              CASE
                                  WHEN p.DD_SIC IS NULL THEN p.IS_ACTIVE
                                  ELSE g.INTERNAL_INFORMATION:navigation_component:IS_ACTIVE:value::STRING END     AS "IS_ACTIVE",
                              CASE
                                  WHEN p.DD_SIC IS NULL THEN p.AISLE_ID_L1
                                  ELSE g.INTERNAL_INFORMATION:navigation_component:aisle_id_l1:value::STRING END   AS AISLE_ID_L1,
                              CASE
                                  WHEN p.DD_SIC IS NULL THEN p.AISLE_NAME_L1
                                  ELSE g.INTERNAL_INFORMATION:navigation_component:aisle_name_l1:value::STRING END AS AISLE_NAME_L1,
                              CASE
                                  WHEN p.DD_SIC IS NULL THEN p.AISLE_ID_L2
                                  ELSE g.INTERNAL_INFORMATION:navigation_component:aisle_id_l2:value::STRING END   AS AISLE_ID_L2,
                              CASE
                                  WHEN p.DD_SIC IS NULL THEN p.AISLE_NAME_L2
                                  ELSE g.INTERNAL_INFORMATION:navigation_component:aisle_name_l2:value::STRING END AS AISLE_NAME_L2,
                              CASE
                                  WHEN p.DD_SIC IS NULL THEN p.PHOTO_ID
                                  ELSE g.INTERNAL_INFORMATION:internal_information:primary_image:id::STRING END    AS photo_id,
                              CASE
                                  WHEN p.DD_SIC IS NULL THEN p.PHOTO_URL
                                  ELSE g.INTERNAL_INFORMATION:internal_information:primary_image:url::STRING END   AS photo_url,
                              CASE
                                  WHEN p.DD_SIC IS NULL THEN p.PRODUCT_CATEGORY_ID
                                  ELSE g.PRODUCT_CATEGORY_ID END                                                   AS product_category_id,
                              CASE WHEN p.DD_SIC IS NULL THEN p.BRAND_ID ELSE g.BRAND_ID END                       AS brand_id,
                              CASE
                                  WHEN p.DD_SIC IS NULL THEN JSON_EXTRACT_PATH_TEXT(p.traits, 'Size')
                                  ELSE g.INTERNAL_INFORMATION:product_group:l4_size:value::STRING END              AS "L4_size"

              FROM catalog_service_prod.public.product_item P --catalog table
                   LEFT JOIN catalog_service_prod.public.denormalized_brand b
                      ON b.PATH_ID = p.brand_id
                   LEFT JOIN CATALOG_SERVICE_PROD.PUBLIC.GLOBAL_PRODUCT_ITEM g
                      ON g.DD_SIC = p.DD_SIC
                   LEFT JOIN doordash_merchant.public.maindb_business ds
                      ON ds.ID = p.DD_BUSINESS_ID)

SELECT "itemMerchantSuppliedId",
       dd_sic,
       item_name,
       business_name,
       "businessId",
       business_vertical_id,
       d.IS_ACTIVE,
       AISLE_ID_L1,
       AISLE_NAME_L1,
       AISLE_ID_L2,
       AISLE_NAME_L2,
       PHOTO_ID,
       PHOTO_URL,
       PRODUCT_CATEGORY_ID,
       v.NAME_L1,
       v.name_l2,
       v.name_l3,
       v.NAME_L4,
       brand_id,
       "L4_size"
FROM data d
     LEFT JOIN catalog_service_prod.public.denormalized_product_category v
        ON v.path_id = d.PRODUCT_CATEGORY_ID -- taxonomy
WHERE d.is_active = TRUE                                  --look for active SKUs
  AND photo_id NOT IN ('4892685', '34848455', '32327627') --remove items with placeholder image in the catalog
  AND BUSINESS_VERTICAL_ID IN (68, 100, 265, 331)
  --AND "businessId" in ()

--- specific item selections ---

  AND (
    (AISLE_NAME_L2 ILIKE '%Decorations%'
        AND (item_name ILIKE '%Easter%'
            OR item_name ILIKE '%Bunny%'
            OR item_name ILIKE '%Bunnies%'
            OR item_name ILIKE '%Egg %'
            OR item_name ILIKE '%Easter Basket%'
            OR item_name ILIKE '%Easter Grass%'
         )
        )
        OR (AISLE_NAME_L2 ILIKE '%Arts & Crafts%'
        AND (item_name ILIKE '%Easter%'
            OR item_name ILIKE '%Bunny%'
            OR item_name ILIKE '%Bunnies%'
            OR item_name ILIKE '%Egg %'
            OR item_name ILIKE '%Easter Basket%'
            OR item_name ILIKE '%Easter Grass%'
                )
        )
        OR (v.NAME_L4 ILIKE '%Disposable Flatware%'
        AND (item_name ILIKE '%Easter%'
            OR item_name ILIKE '%Bunny%'
            OR item_name ILIKE '%Bunnies%'
            OR item_name ILIKE '%Egg %'
            OR item_name ILIKE '%Easter Basket%'
            OR item_name ILIKE '%Easter Grass%'
                )
        )
        OR (v.NAME_L4 ILIKE '%Disposable Plates & Bowls%'
        AND (item_name ILIKE '%Easter%'
            OR item_name ILIKE '%Bunny%'
            OR item_name ILIKE '%Bunnies%'
            OR item_name ILIKE '%Egg %'
            OR item_name ILIKE '%Easter Basket%'
            OR item_name ILIKE '%Easter Grass%'
                )
        )
        OR (v.NAME_L4 ILIKE '%Disposable Cups%'
        AND (item_name ILIKE '%Easter%'
            OR item_name ILIKE '%Bunny%'
            OR item_name ILIKE '%Bunnies%'
            OR item_name ILIKE '%Egg %'
            OR item_name ILIKE '%Easter Basket%'
            OR item_name ILIKE '%Easter Grass%'
                )
        )
        OR (v.NAME_L4 ILIKE '%Disposable Tablecloths%'
        AND (item_name ILIKE '%Easter%'
            OR item_name ILIKE '%Bunny%'
            OR item_name ILIKE '%Bunnies%'
            OR item_name ILIKE '%Egg %'
            OR item_name ILIKE '%Easter Basket%'
            OR item_name ILIKE '%Easter Grass%'
                )
        )
        OR (v.NAME_L3 ILIKE '%Disposable Tableware%'
        AND (item_name ILIKE '%Easter%'
            OR item_name ILIKE '%Bunny%'
            OR item_name ILIKE '%Bunnies%'
            OR item_name ILIKE '%Egg %'
            OR item_name ILIKE '%Easter Basket%'
            OR item_name ILIKE '%Easter Grass%'
                )
        )
        OR (v.NAME_L3 ILIKE '%Napkins%'
        AND (item_name ILIKE '%Easter%'
            OR item_name ILIKE '%Bunny%'
            OR item_name ILIKE '%Bunnies%'
            OR item_name ILIKE '%Egg %'
            OR item_name ILIKE '%Easter Basket%'
            OR item_name ILIKE '%Easter Grass%'
                )
        )
        OR (AISLE_NAME_L2 ILIKE '%Cards%'
        AND (item_name ILIKE '%Easter%'
            OR item_name ILIKE '%Bunny%'
            OR item_name ILIKE '%Bunnies%'
            OR item_name ILIKE '%Egg %'
            OR item_name ILIKE '%Easter Basket%'
            OR item_name ILIKE '%Easter Grass%'
                )
        )
        OR (AISLE_NAME_L2 ILIKE '%Linens & Bedding%'
        AND (item_name ILIKE '%Easter%'
            OR item_name ILIKE '%Bunny%'
            OR item_name ILIKE '%Bunnies%'
            OR item_name ILIKE '%Egg %'
            OR item_name ILIKE '%Easter Basket%'
            OR item_name ILIKE '%Easter Grass%'
                )
        )
        OR (AISLE_NAME_L2 ILIKE '%Toys%'
        AND (item_name ILIKE '%Easter%'
            OR item_name ILIKE '%Bunny%'
            OR item_name ILIKE '%Bunnies%'
            OR item_name ILIKE '%Egg %'
            OR item_name ILIKE '%Easter Basket%'
            OR item_name ILIKE '%Easter Grass%'
                )
        )
        OR (AISLE_NAME_L2 ILIKE '%Fresh Cut Flowers%'
        AND (item_name ILIKE '%Easter%'
            OR item_name ILIKE '%Bunny%'
            OR item_name ILIKE '%Bunnies%'
            OR item_name ILIKE '%Egg %'
            OR item_name ILIKE '%Easter Basket%'
            OR item_name ILIKE '%Easter Grass%'
                )
        )
        OR (AISLE_NAME_L2 ILIKE '%Holiday Decor%'
        AND (item_name ILIKE '%Easter%'
            OR item_name ILIKE '%Bunny%'
            OR item_name ILIKE '%Bunnies%'
            OR item_name ILIKE '%Egg %'
            OR item_name ILIKE '%Easter Basket%'
            OR item_name ILIKE '%Easter Grass%'
                )
        )
        OR (AISLE_NAME_L2 ILIKE '%Seasonal%'
        AND (item_name ILIKE '%Easter%'
            OR item_name ILIKE '%Bunny%'
            OR item_name ILIKE '%Bunnies%'
            OR item_name ILIKE '%Egg %'
            OR item_name ILIKE '%Easter Basket%'
            OR item_name ILIKE '%Easter Grass%'
                )
        )
        OR (AISLE_NAME_L1 ILIKE '%Candy%'
        AND (item_name ILIKE '%Easter%'
            OR item_name ILIKE '%Bunny%'
            OR item_name ILIKE '%Bunnies%'
            OR item_name ILIKE '%Eggs%'
            OR item_name ILIKE '%Egg %'
            OR item_name ILIKE '%Peeps%'
                )
        )
        OR (AISLE_NAME_L2 ILIKE '%Sweet Toppings%'
        AND (item_name ILIKE '%Easter%'
            OR item_name ILIKE '%Bunny%'
            OR item_name ILIKE '%Bunnies%'
            OR item_name ILIKE '%Egg%'
                )
        )
        OR (AISLE_NAME_L2 ILIKE '%Clothing%'
        AND (item_name ILIKE '%Easter%'
            OR item_name ILIKE '%Bunny%'
            OR item_name ILIKE '%Bunnies%'
            OR item_name ILIKE '%Egg %'
            OR item_name ILIKE '%Easter Basket%'
            OR item_name ILIKE '%Easter Grass%'
                )
        )
        OR (AISLE_NAME_L2 ILIKE '%Hair Care%'
        AND (item_name ILIKE '%Easter%'
            OR item_name ILIKE '%Bunny%'
            OR item_name ILIKE '%Bunnies%'
            OR item_name ILIKE '%Egg %'
            OR item_name ILIKE '%Easter Basket%'
            OR item_name ILIKE '%Easter Grass%'
                )
        )
        OR (AISLE_NAME_L1 ILIKE '%Bakery%'
        AND (item_name ILIKE '%Easter%'
            OR item_name ILIKE '%Bunny%'
            OR item_name ILIKE '%Bunnies%'
                )
        )
        OR (AISLE_NAME_L2 ILIKE '%Sweets%'
        AND (item_name ILIKE '%Easter%'
            OR item_name ILIKE '%Bunny%'
            OR item_name ILIKE '%Bunnies%'
                )
        )
        OR (AISLE_NAME_L2 ILIKE '%Baking Mixes%'
        AND (item_name ILIKE '%Easter %'
            OR item_name ILIKE '%Bunny%'
            OR item_name ILIKE '%Bunnies%'
                )
        )
        OR (item_name ILIKE '%Easter basket%')
    )
;

WITH weekly_volume AS (SELECT i.DELIVERY_CREATED_AT::DATE                     AS dt,
                              DATE_TRUNC('week', i.DELIVERY_CREATED_AT::DATE) AS week_cohort,
                              SUM(i.quantity_requested)                       AS item_quantity
                       FROM edw.cng.fact_non_rx_order_item_details i
                            JOIN catalog_service_prod.public.product_item p
                               ON i.ITEM_MERCHANT_SUPPLIED_ID = p.MERCHANT_SUPPLIED_ID AND
                                  i.BUSINESS_ID = p.DD_BUSINESS_ID
                       WHERE p.product_category_id IN
                             (SELECT DISTINCT PRODUCT_CATEGORY_ID FROM dianedou.easter_2024_item_list)
                         AND i.delivery_created_at BETWEEN DATEADD('day', -7 * 8, '2023-04-10') AND '2023-04-10'
                       GROUP BY 1, 2
                       ORDER BY 1 DESC, 2)

SELECT week_cohort
     , SUM(item_quantity)

FROM weekly_volume
GROUP BY 1;


-- SELECT DATE(dd.active_date_utc)         AS active_date_utc,
--        COALESCE(l1_name, 'null')        AS l1_name,
--        SUM(oi.total_item_price / 100)   AS total_sales,
--        SUM(oi.quantity)                 AS total_items,
--        -- orders and cx are not mece
--        COUNT(DISTINCT (dd.delivery_id)) AS num_orders,
--        COUNT(DISTINCT (dd.creator_id))  AS num_cx
-- FROM edw.cng.fact_non_rx_order_item_details oi
--      JOIN edw.cng.dimension_new_vertical_store_tags nvs
--         ON oi.store_id = nvs.store_id
--      JOIN dimension_deliveries dd
--         ON oi.delivery_id = dd.delivery_id
--      LEFT JOIN (SELECT business_id,
--                        item_merchant_supplied_id,
--                        TO_CHAR(item_taxonomy['L1']['name']) AS l1_name
--                 FROM edw.cng.dimension_catalog_product_item) c
--         ON oi.item_merchant_supplied_id = c.item_merchant_supplied_id
--     AND oi.business_id = c.business_id
-- WHERE DATE(dd.active_date_utc) BETWEEN CURRENT_DATE - 28 AND CURRENT_DATE - 1
--   AND (
--     was_found = 1
--         OR sub = 1
--     )
--   AND dd.country_id = 1
--   AND dd.is_filtered_core = TRUE
-- GROUP BY 1,
--          2;
CREATE OR REPLACE TABLE DIANEDOU.easter_2023_basket_building AS
WITH all_delivery_items AS (SELECT oi.*, l1_name,dd.ACTIVE_DATE
                            --     DATE(dd.active_date_utc)         AS active_date_utc,
--        COALESCE(l1_name, 'null')        AS l1_name,
--        SUM(oi.total_item_price / 100)   AS total_sales,
--        SUM(oi.quantity)                 AS total_items,
--        -- orders and cx are not mece
--        COUNT(DISTINCT (dd.delivery_id)) AS num_orders,
--        COUNT(DISTINCT (dd.creator_id))  AS num_cx
                            FROM edw.cng.fact_non_rx_order_item_details oi
                                 JOIN edw.cng.dimension_new_vertical_store_tags nvs
                                    ON oi.store_id = nvs.store_id
                                 JOIN dimension_deliveries dd
                                    ON oi.delivery_id = dd.delivery_id
                                 LEFT JOIN (SELECT business_id,
                                                   item_merchant_supplied_id,
                                                   TO_CHAR(item_taxonomy['L1']['name']) AS l1_name
                                            FROM edw.cng.dimension_catalog_product_item) c
                                    ON oi.item_merchant_supplied_id = c.item_merchant_supplied_id
                                AND oi.business_id = c.business_id
                            WHERE DATE(dd.active_date_utc) BETWEEN DATEADD('day', -7*4+1, '2023-04-09') AND '2023-04-09'
                              AND (
                                was_found = 1
                                    OR sub = 1
                                )
                              AND dd.country_id = 1
                              AND dd.is_filtered_core = TRUE
-- GROUP BY 1,
--          2
)

SELECT all_delivery_items.DELIVERY_ID
     , all_delivery_items.ACTIVE_DATE
     , l1_name
     , COUNT(DISTINCT all_delivery_items.ITEM_MERCHANT_SUPPLIED_ID)                  AS num_items
     , COUNT(DISTINCT CASE
                          WHEN list."itemMerchantSuppliedId" IS NOT NULL
                              THEN all_delivery_items.ITEM_MERCHANT_SUPPLIED_ID END) AS easter_items


FROM all_delivery_items
     LEFT JOIN dianedou.easter_2024_item_list list
        ON all_delivery_items.ITEM_MERCHANT_SUPPLIED_ID = list."itemMerchantSuppliedId"
GROUP BY 1,2,3
;

GRANT SELECT ON TABLE DIANEDOU.easter_2023_basket_building TO read_only_users;

select count (distinct case when easter_items > 0 then delivery_id end) as easter_basket
, count (distinct delivery_id) as nv_orders
, easter_basket / nv_orders

from DIANEDOU.easter_2023_basket_building;

select l1_name
, count(distinct delivery_id)


from DIANEDOU.easter_2023_basket_building
where EASTER_ITEMS > 0
group by 1
;

with sub as (select DISTINCT delivery_id
                  , active_date
                  , num_items
                  , easter_items
                  , date_trunc('week', active_date) as week
                  , count(distinct delivery_id) over (PARTITION BY date_trunc('week', active_date)) as week_of_deliveries
                  , EASTER_ITEMS/NUM_ITEMS as perc_of_basket
                  , case when perc_of_basket <= 0.25 then '0-25%'
             when perc_of_basket >0.25 and perc_of_basket <= 0.5 then '25%-50%'
             when perc_of_basket >0.5 and perc_of_basket < 1 then '50%-99%'
             when perc_of_basket >= 1 then '100%' end as perc_of_basket_range

from (select delivery_id, active_date, sum(num_items) as num_items, sum(easter_items) as easter_items  from DIANEDOU.easter_2023_basket_building group by 1,2)
where EASTER_ITEMS > 0)

select week
, perc_of_basket_range
, week_of_deliveries
, count(DELIVERY_ID) / week_of_deliveries
from sub
group by 1,2,3
;
