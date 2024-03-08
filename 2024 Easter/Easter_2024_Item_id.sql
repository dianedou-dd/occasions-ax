CREATE OR REPLACE TABLE DIANEDOU.easter_2024_item_list_vold AS
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
  AND business_name NOT ILIKE '%test%'
  AND business_name NOT ILIKE '%[Inactive]%'
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

CREATE OR REPLACE TABLE DIANEDOU.easter_2024_item_list_p1_part1 AS

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
  AND BUSINESS_VERTICAL_ID IN (68)
  AND business_name NOT ILIKE '%test%'
  AND business_name NOT ILIKE '%[Inactive]%'
  --AND "businessId" in ()

--- specific item selections ---

  AND (
    (AISLE_NAME_L2 ILIKE '%Wine%'
        AND (item_name ILIKE '%Sparkling%'
            OR item_name ILIKE '%Champagne%'
         )
        )
        OR (AISLE_NAME_L2 IN ('Yogurt'))
        OR (AISLE_NAME_L2 IN ('Eggs'))
        OR (AISLE_NAME_L2 IN ('Bacon'))
        OR (AISLE_NAME_L2 IN ('Juice')
        AND (item_name ILIKE '%Orange%'
            OR item_name ILIKE '%Pineapple%'
            OR item_name ILIKE '%Lemonade%'
            OR item_name ILIKE '%Cranberry%'
                )
        )
        OR (AISLE_NAME_L2 ILIKE '%Pork%'
        AND (item_name ILIKE '%Ham%'
                )
        )
        OR (AISLE_NAME_L2 ILIKE '%Sausage%'
        AND (item_name ILIKE '%Pork sausage%'
            OR item_name ILIKE '%breakfast%'
                )
        )
        OR (AISLE_NAME_L2 ILIKE '%Vegetables%'
        AND (item_name ILIKE '%Carrots%'
            OR item_name ILIKE '%Salad%'
            OR item_name ILIKE '%Potatoes%'
                )
        )
        OR (AISLE_NAME_L2 ILIKE '%Fruit%'
        AND (item_name ILIKE '%Grapes%'
            OR item_name ILIKE '%Strawberries%'
            OR item_name ILIKE '%Blueberries%'
            OR item_name ILIKE '%Blackberries%'
            OR item_name ILIKE '%Raspberries%'
            OR item_name ILIKE '%Bananas%'
                )
        )
        OR (AISLE_NAME_L2 ILIKE '%Leafy Greens%'
        AND (item_name ILIKE '%Salad Mix%'
            OR item_name ILIKE '%Salad Kit%'
            OR item_name ILIKE '%Salad%'
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
        OR (AISLE_NAME_L2 ILIKE '%Toys%'
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
                )
        )
        OR (item_name ILIKE '%Easter basket%')
        OR (AISLE_NAME_L2 ILIKE '%Fresh Cut Flowers%'
        AND (item_name ILIKE '%Easter%'
            OR item_name ILIKE '%Bunny%'
            OR item_name ILIKE '%Bunnies%'
            OR item_name ILIKE '%Egg %'
            OR item_name ILIKE '%Easter Basket%'
            OR item_name ILIKE '%Easter Grass%'
                )
        )
        OR (AISLE_NAME_L1 ILIKE '%Candy%'
        AND (item_name ILIKE '%Easter %'
            OR item_name ILIKE '%Bunny%'
            OR item_name ILIKE '%Bunnies%'
            OR item_name ILIKE '%Eggs%'
            OR item_name ILIKE '%Egg %'
                )
        )
        OR (AISLE_NAME_L2 ILIKE '%Sweet Toppings%'
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
        OR (AISLE_NAME_L1 IN ('Bakery')
        AND (item_name ILIKE '%coffee cake%'
            OR item_name ILIKE '%danish%'
            OR item_name ILIKE '%cinnamon roll%'
            OR item_name ILIKE '%english muffins%'
            OR item_name ILIKE '%Banana bread%'
            OR item_name ILIKE '%marble%'
            OR item_name ILIKE '%Easter %'
            OR item_name ILIKE '%Bunny%'
            OR item_name ILIKE '%Bunnies%'
                )
        )
        OR (AISLE_NAME_L1 IN ('Frozen')
        AND (item_name ILIKE '%Quiche%'
                )
        )
        OR (AISLE_NAME_L2 IN ('Bread & Dough')
        AND (item_name ILIKE '%cinnamon roll%'
                )
        )
        OR (AISLE_NAME_L2 ILIKE '%Sweets%'
        AND (item_name ILIKE '%Easter%'
            OR item_name ILIKE '%Bunny%'
            OR item_name ILIKE '%Bunnies%'
                )
        )
        OR (AISLE_NAME_L1 IN ('Prepared Food')
        AND (item_name ILIKE '%Deviled eggs%'
            OR item_name ILIKE '%Quiche%'
            OR item_name ILIKE '%Fruit salad%'
                )
        )
        OR (v.NAME_L4 IN ('Cottage Cheese'))
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
        OR (AISLE_NAME_L2 ILIKE '%Linens & Bedding%'
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
        OR (AISLE_NAME_L2 ILIKE '%Decorations%'
        AND (item_name ILIKE '%Easter%'
            OR item_name ILIKE '%Bunny%'
            OR item_name ILIKE '%Bunnies%'
            OR item_name ILIKE '%Egg %'
            OR item_name ILIKE '%Easter Basket%'
            OR item_name ILIKE '%Easter Grass%'
                )
        )
    )
;

CREATE OR REPLACE TABLE DIANEDOU.easter_2024_item_list_p1_part2 AS
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
  AND BUSINESS_VERTICAL_ID IN (68)
  AND business_name NOT ILIKE '%test%'
  AND business_name NOT ILIKE '%[Inactive]%'
  --AND "businessId" in (799015)

--- specific item selections ---

  AND (
    (AISLE_NAME_L2 IN ('Wine')
        AND (item_name ILIKE '%Sparkling%'
            OR item_name ILIKE '%Champagne%'
         )
        )
        OR (AISLE_NAME_L2 IN ('Yogurt'))
        OR (AISLE_NAME_L2 IN ('Eggs'))
        OR (AISLE_NAME_L2 IN ('Bacon'))
        OR (AISLE_NAME_L2 IN ('Juice')
        AND (item_name ILIKE '%Orange%'
            OR item_name ILIKE '%Pineapple%'
            OR item_name ILIKE '%Lemonade%'
            OR item_name ILIKE '%Cranberry%'
                )
        )
        OR (AISLE_NAME_L2 IN ('Pork')
        AND (item_name ILIKE '%Ham%'
                )
        )
        OR (AISLE_NAME_L2 IN ('Sausage')
        AND (item_name ILIKE '%Pork sausage%'
            OR item_name ILIKE '%breakfast%'
                )
        )
        OR (AISLE_NAME_L2 IN ('Vegetables')
        AND (item_name ILIKE '%Carrots%'
            OR item_name ILIKE '%Salad%'
            OR item_name ILIKE '%shredded lettuce%'
            OR item_name ILIKE '%Potatoes%'
            OR item_name ILIKE 'Sweet potato'
                )
        )
        OR (AISLE_NAME_L2 IN ('Fruit')
        AND (item_name ILIKE '%Grapes%'
            OR item_name ILIKE '%Strawberries%'
            OR item_name ILIKE '%Blueberries%'
            OR item_name ILIKE '%Blackberries%'
            OR item_name ILIKE '%Raspberries%'
            OR item_name ILIKE '%Bananas%'
                )
        )
        OR (AISLE_NAME_L2 IN ('Leafy Greens')
        AND (item_name ILIKE '%Salad Mix%'
            OR item_name ILIKE '%Salad Kit%'
            OR item_name ILIKE '%Salad%'
            OR item_name ILIKE '%shredded lettuce%'
                )
        )
        OR (AISLE_NAME_L1 IN ('Prepared Food')
        AND (item_name ILIKE '%Deviled eggs%'
            OR item_name ILIKE '%Quiche%'
            OR item_name ILIKE '%Fruit salad%'
                )
        )
        OR (AISLE_NAME_L1 IN ('Bakery')
        AND (item_name ILIKE '%coffee cake%'
            OR item_name ILIKE '%danish%'
            OR item_name ILIKE '%cinnamon roll%'
            OR item_name ILIKE '%english muffins%'
            OR item_name ILIKE '%Banana bread%'
            OR item_name ILIKE '%marble%'
                )
        )
        OR (AISLE_NAME_L1 IN ('Frozen')
        AND (item_name ILIKE '%Quiche%'
                )
        )
        OR (AISLE_NAME_L2 IN ('Bread & Dough')
        AND (item_name ILIKE '%cinnamon roll%'
                )
        )
        OR (v.NAME_L4 IN ('Cottage Cheese'))
    )
ORDER BY BUSINESS_NAME ASC
;

CREATE OR REPLACE TABLE DIANEDOU.easter_2024_item_list_p1 AS
SELECT *
FROM DIANEDOU.easter_2024_item_list_p1_part1
UNION ALL
SELECT *
FROM DIANEDOU.easter_2024_item_list_p1_part2
;

WITH weekly_volume AS (SELECT i.DELIVERY_CREATED_AT::DATE                     AS dt,
                              DATE_TRUNC('week', i.DELIVERY_CREATED_AT::DATE) AS week_cohort,
                              DELIVERY_ID,
                              SUM(i.quantity_requested)                       AS item_quantity,
                              SUM(i.ITEM_PRICE / 100)                         AS item_price

                       FROM edw.cng.fact_non_rx_order_item_details i
                            JOIN catalog_service_prod.public.product_item p
                               ON i.ITEM_MERCHANT_SUPPLIED_ID = p.MERCHANT_SUPPLIED_ID AND
                                  i.BUSINESS_ID = p.DD_BUSINESS_ID
                           AND
                                  i.delivery_created_at BETWEEN DATEADD('day', -7 * 53 - 1, CURRENT_DATE) AND DATEADD('day', -1, CURRENT_DATE)
                            JOIN dianedou.easter_2024_item_list l
                               ON p.merchant_supplied_id = l."itemMerchantSuppliedId"
                           AND p.dd_business_id = l."businessId"
                       --                        WHERE p.product_category_id IN
--                              (SELECT DISTINCT PRODUCT_CATEGORY_ID FROM dianedou.easter_2024_item_list)

                       GROUP BY 1, 2, 3
                       ORDER BY 1 DESC, 2)

SELECT week_cohort
     , SUM(item_quantity)
     , COUNT(DISTINCT DELIVERY_ID) AS basket_vol
     , SUM(item_price)             AS item_subtotal
FROM weekly_volume
GROUP BY 1;


WITH weekly_volume AS (SELECT i.DELIVERY_CREATED_AT::DATE                     AS dt,
                              DATE_TRUNC('week', i.DELIVERY_CREATED_AT::DATE) AS week_cohort,
                              i.DELIVERY_ID,
                              SUM(i.quantity_requested)                       AS item_quantity,
                              SUM(i.TOTAL_ITEM_PRICE / 100)                   AS item_price,
                              SUM(GOV)                                        AS order_gov,
                              SUM(SUBTOTAL)                                   AS order_subtotal


                       FROM edw.cng.fact_non_rx_order_item_details i
                            JOIN catalog_service_prod.public.product_item p
                               ON i.ITEM_MERCHANT_SUPPLIED_ID = p.MERCHANT_SUPPLIED_ID AND
                                  i.BUSINESS_ID = p.DD_BUSINESS_ID
                           AND
                                  i.delivery_created_at BETWEEN DATEADD('day', -7 * 65 - 1, CURRENT_DATE) AND DATEADD('day', -1, CURRENT_DATE)
                            JOIN dianedou.easter_2024_item_list_p1 l
                               ON p.merchant_supplied_id = l."itemMerchantSuppliedId"
                           AND p.dd_business_id = l."businessId"
                            JOIN PRODDB.PUBLIC.DIMENSION_DELIVERIES dd
                               ON i.DELIVERY_ID = dd.DELIVERY_ID
                       --                        WHERE p.product_category_id IN
--                              (SELECT DISTINCT PRODUCT_CATEGORY_ID FROM dianedou.easter_2024_item_list)

                       GROUP BY 1, 2, 3
                       ORDER BY 1 DESC, 2)

SELECT MIN(dt)
     , SUM(item_quantity)
     , COUNT(DISTINCT DELIVERY_ID) AS basket_vol
     , SUM(item_price)             AS item_subtotal
     , SUM(order_gov)              AS TTL_ORDER_GOV
     , SUM(order_subtotal)         AS TTL_ORDER_SUBTOTAL

FROM weekly_volume
WHERE dt BETWEEN '2023-03-30' AND '2023-04-09'
-- GROUP BY 1
;


WITH daily_volume AS (SELECT i.DELIVERY_CREATED_AT::DATE                     AS dt,
                             DATE_TRUNC('week', i.DELIVERY_CREATED_AT::DATE) AS week_cohort,
                             i.DELIVERY_ID,
                             SUM(i.quantity_requested)                       AS item_quantity,
                             SUM(i.TOTAL_ITEM_PRICE / 100)                   AS item_price
                      --                              SUM(GOV / 100)                                  AS order_gov,
--                              SUM(SUBTOTAL / 100)                             AS order_subtotal


                      FROM edw.cng.fact_non_rx_order_item_details i
                           JOIN catalog_service_prod.public.product_item p
                              ON i.ITEM_MERCHANT_SUPPLIED_ID = p.MERCHANT_SUPPLIED_ID AND
                                 i.BUSINESS_ID = p.DD_BUSINESS_ID
                          AND
                                 i.delivery_created_at BETWEEN DATEADD('day', -7 * 65 - 1, CURRENT_DATE) AND DATEADD('day', -1, CURRENT_DATE)
                           JOIN dianedou.easter_2024_item_list l
                              ON p.merchant_supplied_id = l."itemMerchantSuppliedId"
                          AND p.dd_business_id = l."businessId"
                      --                            JOIN PRODDB.PUBLIC.DIMENSION_DELIVERIES dd
--                               ON i.DELIVERY_ID = dd.DELIVERY_ID
                      --                        WHERE p.product_category_id IN
--                              (SELECT DISTINCT PRODUCT_CATEGORY_ID FROM dianedou.easter_2024_item_list)

                      GROUP BY 1, 2, 3
                      ORDER BY 1 DESC, 2)

   , sub AS (SELECT i.*,
                    SUM(GOV / 100)      AS order_gov,
                    SUM(SUBTOTAL / 100) AS order_subtotal

             FROM daily_volume i
                  JOIN PRODDB.PUBLIC.DIMENSION_DELIVERIES dd
                     ON i.DELIVERY_ID = dd.DELIVERY_ID
             GROUP BY 1, 2, 3, 4, 5)


   , summary AS (SELECT 'last-year-baseline'        AS time_period --8 weeks before Vday
                      , SUM(item_quantity)          AS item_quantity
                      , COUNT(DISTINCT DELIVERY_ID) AS basket_vol
                      , SUM(item_price)             AS item_subtotal
                      , SUM(order_gov)              AS TTL_ORDER_GOV
                      , SUM(order_subtotal)         AS TTL_ORDER_SUBTOTAL

                 FROM sub
                 WHERE dt BETWEEN '2022-12-19' AND '2023-02-12'

                 UNION ALL

                 SELECT 'last-year-easter' --10days easter last year
                      , SUM(item_quantity)          AS item_quantity
                      , COUNT(DISTINCT DELIVERY_ID) AS basket_vol
                      , SUM(item_price)             AS item_subtotal
                      , SUM(order_gov)              AS TTL_ORDER_GOV
                      , SUM(order_subtotal)         AS TTL_ORDER_SUBTOTAL

                 FROM sub
                 WHERE dt BETWEEN '2023-03-30' AND '2023-04-09'

                 UNION ALL

                 SELECT 'this-year-baseline' --8 weeks before Vday this year
                      , SUM(item_quantity)          AS item_quantity
                      , COUNT(DISTINCT DELIVERY_ID) AS basket_vol
                      , SUM(item_price)             AS item_subtotal
                      , SUM(order_gov)              AS TTL_ORDER_GOV
                      , SUM(order_subtotal)         AS TTL_ORDER_SUBTOTAL

                 FROM sub
                 WHERE dt BETWEEN '2023-12-18' AND '2024-02-11')

SELECT *
FROM summary
;

----------------------Basket building behavior deep dive----------------------
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
WITH all_delivery_items AS (SELECT oi.*, l1_name, dd.ACTIVE_DATE
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
                            WHERE DATE(dd.active_date_utc) BETWEEN DATEADD('day', -7 * 4 + 1, '2023-04-09') AND '2023-04-09'
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
GROUP BY 1, 2, 3
;

GRANT SELECT ON TABLE DIANEDOU.easter_2023_basket_building TO read_only_users;

SELECT COUNT(DISTINCT CASE WHEN easter_items > 0 THEN delivery_id END) AS easter_basket
     , COUNT(DISTINCT delivery_id)                                     AS nv_orders
     , easter_basket / nv_orders

FROM DIANEDOU.easter_2023_basket_building;

SELECT l1_name
     , COUNT(DISTINCT delivery_id)


FROM DIANEDOU.easter_2023_basket_building
WHERE EASTER_ITEMS > 0
GROUP BY 1
;

WITH sub AS (SELECT DISTINCT delivery_id
                           , active_date
                           , num_items
                           , easter_items
                           , DATE_TRUNC('week', active_date)                           AS week
                           , COUNT(DISTINCT delivery_id)
                                   OVER (PARTITION BY DATE_TRUNC('week', active_date)) AS week_of_deliveries
                           , EASTER_ITEMS / NUM_ITEMS                                  AS perc_of_basket
                           , CASE
                                 WHEN perc_of_basket <= 0.25 THEN '0-25%'
                                 WHEN perc_of_basket > 0.25 AND perc_of_basket <= 0.5 THEN '25%-50%'
                                 WHEN perc_of_basket > 0.5 AND perc_of_basket < 1 THEN '50%-99%'
                                 WHEN perc_of_basket >= 1
                                     THEN '100%' END                                   AS perc_of_basket_range

             FROM (SELECT delivery_id, active_date, SUM(num_items) AS num_items, SUM(easter_items) AS easter_items
                   FROM DIANEDOU.easter_2023_basket_building
                   GROUP BY 1, 2)
             WHERE EASTER_ITEMS > 0)

SELECT week
     , perc_of_basket_range
     , week_of_deliveries
     , COUNT(DELIVERY_ID) / week_of_deliveries
FROM sub
GROUP BY 1, 2, 3
;

--------------------------------------------End--------------------------------------------

WITH daily_volume AS (SELECT i.DELIVERY_CREATED_AT::DATE                     AS dt,
                             DATE_TRUNC('week', i.DELIVERY_CREATED_AT::DATE) AS week_cohort,
                             i.DELIVERY_ID,
                             i.CREATOR_ID,
                             SUM(i.quantity_requested)                       AS item_quantity,
                             SUM(i.TOTAL_ITEM_PRICE / 100)                   AS item_price
                      --                              SUM(GOV / 100)                                  AS order_gov,
--                              SUM(SUBTOTAL / 100)                             AS order_subtotal


                      FROM edw.cng.fact_non_rx_order_item_details i
                           JOIN catalog_service_prod.public.product_item p
                              ON i.ITEM_MERCHANT_SUPPLIED_ID = p.MERCHANT_SUPPLIED_ID AND
                                 i.BUSINESS_ID = p.DD_BUSINESS_ID
                          AND
                                 i.delivery_created_at BETWEEN '2023-03-30' AND '2023-04-09'
                           JOIN dianedou.easter_2024_item_list l
                              ON p.merchant_supplied_id = l."itemMerchantSuppliedId"
                          AND p.dd_business_id = l."businessId"
                      --                            JOIN PRODDB.PUBLIC.DIMENSION_DELIVERIES dd
--                               ON i.DELIVERY_ID = dd.DELIVERY_ID
                      --                        WHERE p.product_category_id IN
--                              (SELECT DISTINCT PRODUCT_CATEGORY_ID FROM dianedou.easter_2024_item_list)

                      GROUP BY 1, 2, 3, 4
                      ORDER BY 1 DESC, 2)


SELECT COUNT(DISTINCT CREATOR_ID)
FROM daily_volume; --12730

SELECT DATE_TRUNC(WEEK, ddr.active_date_utc) AS wbr_week,
       nv.vertical_name,
       COUNT(DISTINCT ddr.creator_id)        AS NV_orderers
--         count(distinct case when daily_volume.CREATOR_ID is not null then ddr.creator_id end) as Easter_orderer
FROM dimension_deliveries ddr --stefaniemontgomery.dimension_deliveries_ranked ddr
     LEFT JOIN edw.cng.dimension_new_vertical_store_tags nv
        ON nv.store_id = ddr.store_id
    AND is_filtered_mp_vertical = 1

WHERE ddr.country_id = 1
  AND nv.org_id = 1
--     and nv.vertical_name in ('1P Convenience','3P Convenience','Alcohol','Grocery','Pets','Flowers','Emerging Retail')
  AND DATE_TRUNC(WEEK, ddr.active_date_utc) BETWEEN '2023-03-30' AND '2023-04-09'
--     and ddr.order_number_vertical > 1
GROUP BY ALL;

----easter item orderers
-- WITH daily_volume AS (SELECT i.DELIVERY_CREATED_AT::DATE                     AS dt,
--                              DATE_TRUNC('week', i.DELIVERY_CREATED_AT::DATE) AS week_cohort,
--                              i.DELIVERY_ID,
--                              i.CREATOR_ID,
--                              SUM(i.quantity_requested)                       AS item_quantity,
--                              SUM(i.TOTAL_ITEM_PRICE / 100)                   AS item_price
--                       --                              SUM(GOV / 100)                                  AS order_gov,
-- --                              SUM(SUBTOTAL / 100)                             AS order_subtotal
--
--
--                       FROM edw.cng.fact_non_rx_order_item_details i
--                                --                            JOIN catalog_service_prod.public.product_item p
-- --                               ON i.ITEM_MERCHANT_SUPPLIED_ID = p.MERCHANT_SUPPLIED_ID AND
-- --                                  i.BUSINESS_ID = p.DD_BUSINESS_ID
--
--                            JOIN dianedou.easter_2024_item_list_p1 l
--                           --                               ON p.merchant_supplied_id = l."itemMerchantSuppliedId"
-- --                           AND p.dd_business_id = l."businessId"
--                               ON l.AISLE_NAME_L2 = i.AISLE_NAME_L2
--                           AND ((i.delivery_created_at BETWEEN '2022-12-18' AND '2023-04-09') OR
--                                (i.delivery_created_at BETWEEN '2023-12-18' AND '2024-02-12'))
--                       --                            JOIN PRODDB.PUBLIC.DIMENSION_DELIVERIES dd
-- --                               ON i.DELIVERY_ID = dd.DELIVERY_ID
-- --                        WHERE p.product_category_id IN
-- --                              (SELECT DISTINCT PRODUCT_CATEGORY_ID FROM dianedou.easter_2024_item_list)
--
--                       GROUP BY 1, 2, 3, 4
--                       ORDER BY 1
--                               DESC, 2)
--
--    , sub AS (SELECT i.*,
--                     SUM(GOV / 100)      AS order_gov,
--                     SUM(SUBTOTAL / 100) AS order_subtotal
--              FROM daily_volume i
--                   JOIN PRODDB.PUBLIC.DIMENSION_DELIVERIES dd
--                      ON i.DELIVERY_ID = dd.DELIVERY_ID
--              GROUP BY 1, 2, 3, 4, 5, 6)
--
--    , summary AS (SELECT 'last-year-baseline'        AS time_period --8 weeks before Vday
--                       , SUM(item_quantity)          AS item_quantity
--                       , COUNT(DISTINCT DELIVERY_ID) AS basket_vol
--                       , SUM(item_price)             AS item_subtotal
--                       , SUM(order_gov)              AS TTL_ORDER_GOV
--                       , SUM(order_subtotal)         AS TTL_ORDER_SUBTOTAL
--                  FROM sub
--                  WHERE dt BETWEEN '2022-12-19' AND '2023-02-12'
--                  UNION ALL
--                  SELECT 'last-year-easter' --10days easter last year
--                       , SUM(item_quantity)          AS item_quantity
--                       , COUNT(DISTINCT DELIVERY_ID) AS basket_vol
--                       , SUM(item_price)             AS item_subtotal
--                       , SUM(order_gov)              AS TTL_ORDER_GOV
--                       , SUM(order_subtotal)         AS TTL_ORDER_SUBTOTAL
--                  FROM sub
--                  WHERE dt BETWEEN '2023-03-30' AND '2023-04-09'
--                  UNION ALL
--                  SELECT 'this-year-baseline' --8 weeks before Vday this year
--                       , SUM(item_quantity)          AS item_quantity
--                       , COUNT(DISTINCT DELIVERY_ID) AS basket_vol
--                       , SUM(item_price)             AS item_subtotal
--                       , SUM(order_gov)              AS TTL_ORDER_GOV
--                       , SUM(order_subtotal)         AS TTL_ORDER_SUBTOTAL
--                  FROM sub
--                  WHERE dt BETWEEN '2023-12-18' AND '2024-02-11')
--
-- SELECT *
-- FROM summary;




----easter item orderers
WITH daily_volume AS (SELECT i.DELIVERY_CREATED_AT::DATE                     AS dt,
                             DATE_TRUNC('week', i.DELIVERY_CREATED_AT::DATE) AS week_cohort,
                             i.DELIVERY_ID,
                             i.CREATOR_ID,
                             SUM(i.quantity_requested)                       AS item_quantity,
                             SUM(i.TOTAL_ITEM_PRICE / 100)                   AS item_price
                      --                              SUM(GOV / 100)                                  AS order_gov,
--                              SUM(SUBTOTAL / 100)                             AS order_subtotal


                      FROM edw.cng.fact_non_rx_order_item_details i
                      WHERE i.AISLE_NAME_L2 IN (SELECT DISTINCT AISLE_NAME_L2 FROM dianedou.easter_2024_item_list)
                        AND ((i.delivery_created_at BETWEEN '2022-12-18' AND '2023-04-09') OR
                             (i.delivery_created_at BETWEEN '2023-12-18' AND '2024-02-12'))
                      --                            JOIN catalog_service_prod.public.product_item p
-- --                               ON i.ITEM_MERCHANT_SUPPLIED_ID = p.MERCHANT_SUPPLIED_ID AND
-- --                                  i.BUSINESS_ID = p.DD_BUSINESS_ID

--                            JOIN dianedou.easter_2024_item_list_p1 l
--                           --                               ON p.merchant_supplied_id = l."itemMerchantSuppliedId"
-- --                           AND p.dd_business_id = l."businessId"
--                               ON l.AISLE_NAME_L2 = i.AISLE_NAME_L2
--                               AND ((i.delivery_created_at BETWEEN '2022-12-18' AND '2023-04-09') OR
--                                   (i.delivery_created_at BETWEEN '2023-12-18' AND '2024-02-12'))
--                       --                            JOIN PRODDB.PUBLIC.DIMENSION_DELIVERIES dd
-- --                               ON i.DELIVERY_ID = dd.DELIVERY_ID
-- --                        WHERE p.product_category_id IN
--                              (SELECT DISTINCT PRODUCT_CATEGORY_ID FROM dianedou.easter_2024_item_list)

                      GROUP BY 1, 2, 3, 4
                      ORDER BY 1
                              DESC, 2)

   , sub AS (SELECT i.*,
                    SUM(GOV / 100)      AS order_gov,
                    SUM(SUBTOTAL / 100) AS order_subtotal
             FROM daily_volume i
                  JOIN PRODDB.PUBLIC.DIMENSION_DELIVERIES dd
                     ON i.DELIVERY_ID = dd.DELIVERY_ID
             GROUP BY 1, 2, 3, 4, 5, 6)

   , summary AS (SELECT 'last-year-baseline'        AS time_period --8 weeks before Vday
                      , SUM(item_quantity)          AS item_quantity
                      , COUNT(DISTINCT DELIVERY_ID) AS basket_vol
                      , SUM(item_price)             AS item_subtotal
                      , SUM(order_gov)              AS TTL_ORDER_GOV
                      , SUM(order_subtotal)         AS TTL_ORDER_SUBTOTAL
                 FROM sub
                 WHERE dt BETWEEN '2022-12-19' AND '2023-02-12'
                 UNION ALL
                 SELECT 'last-year-easter' --10days easter last year
                      , SUM(item_quantity)          AS item_quantity
                      , COUNT(DISTINCT DELIVERY_ID) AS basket_vol
                      , SUM(item_price)             AS item_subtotal
                      , SUM(order_gov)              AS TTL_ORDER_GOV
                      , SUM(order_subtotal)         AS TTL_ORDER_SUBTOTAL
                 FROM sub
                 WHERE dt BETWEEN '2023-03-30' AND '2023-04-09'
                 UNION ALL
                 SELECT 'this-year-baseline' --8 weeks before Vday this year
                      , SUM(item_quantity)          AS item_quantity
                      , COUNT(DISTINCT DELIVERY_ID) AS basket_vol
                      , SUM(item_price)             AS item_subtotal
                      , SUM(order_gov)              AS TTL_ORDER_GOV
                      , SUM(order_subtotal)         AS TTL_ORDER_SUBTOTAL
                 FROM sub
                 WHERE dt BETWEEN '2023-12-18' AND '2024-02-11')

SELECT *
FROM summary;

GRANT SELECT ON TABLE dianedou.temp_easter_weekly_sizing_p0_v2 TO read_only_users;

CREATE TABLE dianedou.temp_easter_weekly_sizing_p1_summary AS

SELECT week_cohort
     , SUM(item_price) AS item_subtotals


FROM dianedou.temp_easter_weekly_sizing_p1
GROUP BY 1
;
-- CREATE TABLE dianedou.temp_easter_weekly_sizing_p0_v2 AS

WITH sub AS (SELECT i.DELIVERY_CREATED_AT::DATE                     AS dt,
                    DATE_TRUNC('week', i.DELIVERY_CREATED_AT::DATE) AS week_cohort,
--                              i.DELIVERY_ID,
--                              i.CREATOR_ID,
                    i.AISLE_NAME_L2,
                    SUM(i.quantity_requested)                       AS item_quantity,
                    SUM(i.TOTAL_ITEM_PRICE / 100)                   AS item_price,
                    SUM(dd.GOV / 100)                               AS order_gov,
                    SUM(dd.SUBTOTAL / 100)                          AS order_subtotal


             FROM edw.cng.fact_non_rx_order_item_details i
                  JOIN PRODDB.PUBLIC.DIMENSION_DELIVERIES dd
                     ON i.DELIVERY_ID = dd.DELIVERY_ID
                 AND i.AISLE_NAME_L2 IN (SELECT DISTINCT AISLE_NAME_L2 FROM dianedou.easter_2024_item_list)
                 AND (i.delivery_created_at BETWEEN '2022-12-18' AND '2024-02-12')
             --                            JOIN catalog_service_prod.public.product_item p
-- --                               ON i.ITEM_MERCHANT_SUPPLIED_ID = p.MERCHANT_SUPPLIED_ID AND
-- --                                  i.BUSINESS_ID = p.DD_BUSINESS_ID

--                            JOIN dianedou.easter_2024_item_list_p1 l
--                           --                               ON p.merchant_supplied_id = l."itemMerchantSuppliedId"
-- --                           AND p.dd_business_id = l."businessId"
--                               ON l.AISLE_NAME_L2 = i.AISLE_NAME_L2
--                               AND ((i.delivery_created_at BETWEEN '2022-12-18' AND '2023-04-09') OR
--                                   (i.delivery_created_at BETWEEN '2023-12-18' AND '2024-02-12'))


-- --                        WHERE p.product_category_id IN
--                              (SELECT DISTINCT PRODUCT_CATEGORY_ID FROM dianedou.easter_2024_item_list)

             GROUP BY 1, 2, 3 --, 4
             ORDER BY 1
                     DESC, 2)

SELECT week_cohort
     , AISLE_NAME_L2
     , SUM(item_price)     AS item_subtotals
     , SUM(order_gov)      AS order_gov_ttl
     , SUM(order_subtotal) AS order_subtotals


FROM sub
GROUP BY 1, 2
;

CREATE TABLE dianedou.temp_easter_weekly_sizing_p0_v3 AS

WITH sub AS (SELECT i.DELIVERY_CREATED_AT::DATE                     AS dt,
                    DATE_TRUNC('week', i.DELIVERY_CREATED_AT::DATE) AS week_cohort,
--                              i.DELIVERY_ID,
--                              i.CREATOR_ID,
                    i.AISLE_NAME_L2,
                    SUM(i.quantity_requested)                       AS item_quantity,
                    SUM(i.TOTAL_ITEM_PRICE / 100)                   AS item_price
             --                              SUM(GOV / 100)                                  AS order_gov,
--                              SUM(SUBTOTAL / 100)                             AS order_subtotal


             FROM edw.cng.fact_non_rx_order_item_details i
             WHERE TRUE
--                           i.AISLE_NAME_L2 in (select distinct AISLE_NAME_L2 from dianedou.easter_2024_item_list)
               AND (i.delivery_created_at BETWEEN '2022-12-18' AND '2024-02-12')
             --                            JOIN catalog_service_prod.public.product_item p
-- --                               ON i.ITEM_MERCHANT_SUPPLIED_ID = p.MERCHANT_SUPPLIED_ID AND
-- --                                  i.BUSINESS_ID = p.DD_BUSINESS_ID

--                            JOIN dianedou.easter_2024_item_list_p1 l
--                           --                               ON p.merchant_supplied_id = l."itemMerchantSuppliedId"
-- --                           AND p.dd_business_id = l."businessId"
--                               ON l.AISLE_NAME_L2 = i.AISLE_NAME_L2
--                               AND ((i.delivery_created_at BETWEEN '2022-12-18' AND '2023-04-09') OR
--                                   (i.delivery_created_at BETWEEN '2023-12-18' AND '2024-02-12'))
--                       --                            JOIN PRODDB.PUBLIC.DIMENSION_DELIVERIES dd
-- --                               ON i.DELIVERY_ID = dd.DELIVERY_ID
-- --                        WHERE p.product_category_id IN
--                              (SELECT DISTINCT PRODUCT_CATEGORY_ID FROM dianedou.easter_2024_item_list)

             GROUP BY 1, 2, 3 --, 4
             ORDER BY 1
                     DESC, 2)

SELECT week_cohort
     , AISLE_NAME_L2
     , SUM(item_price) AS item_subtotals


FROM sub
GROUP BY 1, 2
;

SELECT AISLE_NAME_L2, MIN(week_cohort), MAX(week_cohort)
FROM dianedou.temp_easter_weekly_sizing_p0_v3
GROUP BY 1;

SELECT AISLE_NAME_L2
     , AVG(CASE
               WHEN week_cohort BETWEEN DATEADD('day', -37, '2023-04-03') AND DATEADD('day', -6, '2023-04-03')
                   THEN item_subtotals END)                              AS baseline_subtotals
     , AVG(CASE WHEN week_cohort = '2023-04-03' THEN item_subtotals END) AS easter_subtotals
     , easter_subtotals / baseline_subtotals
FROM dianedou.temp_easter_weekly_sizing_p0_v3

-- where week_cohort = '2023-04-03'
GROUP BY 1
HAVING easter_subtotals IS NOT NULL
   AND baseline_subtotals IS NOT NULL
ORDER BY 4 DESC;



CREATE TABLE dianedou.temp_easter_weekly_sizing_p1 AS
    (SELECT i.DELIVERY_CREATED_AT::DATE                     AS dt,
            DATE_TRUNC('week', i.DELIVERY_CREATED_AT::DATE) AS week_cohort,
            i.DELIVERY_ID,
            i.CREATOR_ID,
            SUM(i.quantity_requested)                       AS item_quantity,
            SUM(i.TOTAL_ITEM_PRICE / 100)                   AS item_price
     --                              SUM(GOV / 100)                                  AS order_gov,
--                              SUM(SUBTOTAL / 100)                             AS order_subtotal


     FROM edw.cng.fact_non_rx_order_item_details i
     WHERE i.AISLE_NAME_L2 IN (SELECT DISTINCT AISLE_NAME_L2 FROM dianedou.easter_2024_item_list_p1)
       AND (i.delivery_created_at BETWEEN '2022-12-18' AND '2024-02-12')


     GROUP BY 1, 2, 3, 4
     ORDER BY 1
             DESC, 2)
;

SELECT AISLE_NAME_L2
     , AVG(CASE
               WHEN week_cohort BETWEEN DATEADD('day', -37, '2023-04-03') AND DATEADD('day', -6, '2023-04-03')
                   THEN item_subtotals END)                              AS baseline_subtotals
     , AVG(CASE WHEN week_cohort = '2023-04-03' THEN item_subtotals END) AS easter_subtotals
     , easter_subtotals / baseline_subtotals
FROM dianedou.temp_easter_weekly_sizing_p1_V2
-- where week_cohort = '2023-04-03'
GROUP BY 1
ORDER BY 3 DESC;

SELECT MIN(week_cohort)
FROM dianedou.temp_easter_weekly_sizing_p0_v2;


SELECT DISTINCT AISLE_NAME_L2, COUNT()
FROM dianedou.easter_2024_item_list_p1;

SELECT DATEDIFF('day', '2022-12-19', '2023-02-12');

-------Easter retention
-------68% 2023 cx are still active on the platform in 2024 march, 10% dormant, 22% churned
WITH sub AS (SELECT i.DELIVERY_CREATED_AT::DATE                     AS dt,
                    DATE_TRUNC('week', i.DELIVERY_CREATED_AT::DATE) AS week_cohort,
                    i.DELIVERY_ID,
                    i.CREATOR_ID,
                    i.AISLE_NAME_L2,
                    SUM(i.quantity_requested)                       AS item_quantity,
                    SUM(i.TOTAL_ITEM_PRICE / 100)                   AS item_price,
                    SUM(dd.GOV / 100)                               AS order_gov,
                    SUM(dd.SUBTOTAL / 100)                          AS order_subtotal


             FROM edw.cng.fact_non_rx_order_item_details i
                  JOIN PRODDB.PUBLIC.DIMENSION_DELIVERIES dd
                     ON i.DELIVERY_ID = dd.DELIVERY_ID
                 AND i.AISLE_NAME_L2 IN (SELECT DISTINCT AISLE_NAME_L2 FROM dianedou.easter_2024_item_list)
                 AND (i.delivery_created_at::DATE BETWEEN '2023-03-31' AND '2023-04-09')

             GROUP BY ALL
             ORDER BY 1
                     DESC, 2)

SELECT week_cohort,
       CASE
           WHEN COALESCE(mh.days_since_last_purchase, -1) BETWEEN 0 AND 28 THEN 'Active'
           WHEN COALESCE(mh.days_since_last_purchase, -1) BETWEEN 29 AND 90 THEN 'Dormant'
           WHEN COALESCE(mh.days_since_last_purchase, -1) > 90 THEN 'Churned'
           ELSE 'New'
           END AS mp_segment,
       COUNT(DISTINCT sub.CREATOR_ID)
FROM sub
     LEFT JOIN mattheitz.mh_customer_authority mh
        ON mh.creator_id = sub.creator_id
    AND mh.dte = '2024-03-03'

GROUP BY 1, 2
;
SELECT DISTINCT AISLE_NAME_L2
FROM dianedou.easter_2024_item_list
;

---easter day top merchants
WITH sub AS (SELECT i.*
             FROM edw.cng.fact_non_rx_order_item_details i
                  JOIN dimension_deliveries dd
                     ON i.DELIVERY_ID = dd.DELIVERY_ID
                 AND is_filtered_core = 1
                 AND i.AISLE_NAME_L2 IN (SELECT DISTINCT AISLE_NAME_L2 FROM dianedou.easter_2024_item_list_p1)
                 AND i.delivery_created_at::DATE = '2023-04-09')

SELECT DELIVERY_CREATED_AT::DATE AS dt,
       BUSINESS_NAME,
       COUNT(DELIVERY_ID) / (SELECT COUNT(DELIVERY_ID) FROM sub)
FROM sub
GROUP BY ALL
ORDER BY 3 DESC, 2;

WITH sub AS (SELECT *
             FROM dimension_deliveries dd
             WHERE TRUE
               AND is_filtered_core = 1
               AND is_caviar = 0
               AND ACTIVE_DATE::DATE = '2023-04-09')

SELECT dd.BUSINESS_NAME
     , COUNT(DISTINCT dd.DELIVERY_ID) AS volume
     , COUNT(DISTINCT dd.DELIVERY_ID) / (SELECT COUNT(DELIVERY_ID) FROM sub)
FROM sub dd
GROUP BY ALL
ORDER BY 2 DESC
