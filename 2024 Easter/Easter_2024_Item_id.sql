create or replace table DIANEDOU.easter_2024_item_list as
with data as (

SELECT
  DISTINCT
  p.MERCHANT_SUPPLIED_ID AS "itemMerchantSuppliedId",
  p.dd_sic as dd_sic,
  case when p.DD_SIC is null then p.ITEM_NAME else g.INTERNAL_INFORMATION:navigation_component:ITEM_NAME:value::string end as item_name,
  ds.NAME as business_name,
  DD_BUSINESS_ID AS "businessId",
  ds.BUSINESS_VERTICAL_ID,
  case when p.DD_SIC is null then p.IS_ACTIVE else g.INTERNAL_INFORMATION:navigation_component:IS_ACTIVE:value::string end as "IS_ACTIVE",
  case when p.DD_SIC is null then p.AISLE_ID_L1 else g.INTERNAL_INFORMATION:navigation_component:aisle_id_l1:value::string end as AISLE_ID_L1,
  case when p.DD_SIC is null then p.AISLE_NAME_L1 else g.INTERNAL_INFORMATION:navigation_component:aisle_name_l1:value::string end as AISLE_NAME_L1,
  case when p.DD_SIC is null then p.AISLE_ID_L2 else g.INTERNAL_INFORMATION:navigation_component:aisle_id_l2:value::string end as AISLE_ID_L2,
  case when p.DD_SIC is null then p.AISLE_NAME_L2 else g.INTERNAL_INFORMATION:navigation_component:aisle_name_l2:value::string end as AISLE_NAME_L2,
  case when p.DD_SIC is null then p.PHOTO_ID else  g.INTERNAL_INFORMATION:internal_information:primary_image:id::string end as photo_id,
  case when p.DD_SIC is null then p.PHOTO_URL else g.INTERNAL_INFORMATION:internal_information:primary_image:url::string end as photo_url,
  case when p.DD_SIC is null then p.PRODUCT_CATEGORY_ID else g.PRODUCT_CATEGORY_ID end as product_category_id,
  case when p.DD_SIC is null then p.BRAND_ID else g.BRAND_ID end as brand_id,
  case when p.DD_SIC is null then JSON_EXTRACT_PATH_TEXT(p.traits,'Size') else g.INTERNAL_INFORMATION:product_group:l4_size:value::string end as "L4_size"

FROM catalog_service_prod.public.product_item P --catalog table
LEFT JOIN catalog_service_prod.public.denormalized_brand b on b.PATH_ID = p.brand_id
LEFT JOIN CATALOG_SERVICE_PROD.PUBLIC.GLOBAL_PRODUCT_ITEM g  on g.DD_SIC = p.DD_SIC
LEFT JOIN doordash_merchant.public.maindb_business ds on ds.ID = p.DD_BUSINESS_ID

)

select
"itemMerchantSuppliedId",
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
from data d
LEFT JOIN catalog_service_prod.public.denormalized_product_category v ON v.path_id = d.PRODUCT_CATEGORY_ID -- taxonomy
WHERE
  d.is_active = TRUE --look for active SKUs
  AND photo_id NOT IN ('4892685','34848455', '32327627') --remove items with placeholder image in the catalog
  AND BUSINESS_VERTICAL_ID IN (68, 100, 265, 331)
  --AND "businessId" in ()

--- specific item selections ---

AND (
      (AISLE_NAME_L2 ilike '%Decorations%'
        AND (item_name ilike '%Easter%'
            OR item_name ilike '%Bunny%'
            OR item_name ilike '%Bunnies%'
            OR item_name ilike '%Egg %'
            OR item_name ilike '%Easter Basket%'
            OR item_name ilike '%Easter Grass%'
            )
        )
        OR (AISLE_NAME_L2 ilike '%Arts & Crafts%'
          AND (item_name ilike '%Easter%'
            OR item_name ilike '%Bunny%'
            OR item_name ilike '%Bunnies%'
            OR item_name ilike '%Egg %'
            OR item_name ilike '%Easter Basket%'
            OR item_name ilike '%Easter Grass%'
              )
            )
        OR (v.NAME_L4 ilike '%Disposable Flatware%'
          AND (item_name ilike '%Easter%'
            OR item_name ilike '%Bunny%'
            OR item_name ilike '%Bunnies%'
            OR item_name ilike '%Egg %'
            OR item_name ilike '%Easter Basket%'
            OR item_name ilike '%Easter Grass%'
              )
            )
        OR (v.NAME_L4 ilike '%Disposable Plates & Bowls%'
          AND (item_name ilike '%Easter%'
            OR item_name ilike '%Bunny%'
            OR item_name ilike '%Bunnies%'
            OR item_name ilike '%Egg %'
            OR item_name ilike '%Easter Basket%'
            OR item_name ilike '%Easter Grass%'
              )
            )
        OR (v.NAME_L4 ilike '%Disposable Cups%'
          AND (item_name ilike '%Easter%'
            OR item_name ilike '%Bunny%'
            OR item_name ilike '%Bunnies%'
            OR item_name ilike '%Egg %'
            OR item_name ilike '%Easter Basket%'
            OR item_name ilike '%Easter Grass%'
              )
            )
        OR (v.NAME_L4 ilike '%Disposable Tablecloths%'
          AND (item_name ilike '%Easter%'
            OR item_name ilike '%Bunny%'
            OR item_name ilike '%Bunnies%'
            OR item_name ilike '%Egg %'
            OR item_name ilike '%Easter Basket%'
            OR item_name ilike '%Easter Grass%'
              )
            )
        OR (v.NAME_L3 ilike '%Disposable Tableware%'
          AND (item_name ilike '%Easter%'
            OR item_name ilike '%Bunny%'
            OR item_name ilike '%Bunnies%'
            OR item_name ilike '%Egg %'
            OR item_name ilike '%Easter Basket%'
            OR item_name ilike '%Easter Grass%'
              )
            )
        OR (v.NAME_L3 ilike '%Napkins%'
          AND (item_name ilike '%Easter%'
            OR item_name ilike '%Bunny%'
            OR item_name ilike '%Bunnies%'
            OR item_name ilike '%Egg %'
            OR item_name ilike '%Easter Basket%'
            OR item_name ilike '%Easter Grass%'
              )
            )
        OR (AISLE_NAME_L2 ilike '%Cards%'
          AND (item_name ilike '%Easter%'
            OR item_name ilike '%Bunny%'
            OR item_name ilike '%Bunnies%'
            OR item_name ilike '%Egg %'
            OR item_name ilike '%Easter Basket%'
            OR item_name ilike '%Easter Grass%'
              )
            )
        OR (AISLE_NAME_L2 ilike '%Linens & Bedding%'
          AND (item_name ilike '%Easter%'
            OR item_name ilike '%Bunny%'
            OR item_name ilike '%Bunnies%'
            OR item_name ilike '%Egg %'
            OR item_name ilike '%Easter Basket%'
            OR item_name ilike '%Easter Grass%'
              )
            )
        OR (AISLE_NAME_L2 ilike '%Toys%'
          AND (item_name ilike '%Easter%'
            OR item_name ilike '%Bunny%'
            OR item_name ilike '%Bunnies%'
            OR item_name ilike '%Egg %'
            OR item_name ilike '%Easter Basket%'
            OR item_name ilike '%Easter Grass%'
              )
            )
        OR (AISLE_NAME_L2 ilike '%Fresh Cut Flowers%'
          AND (item_name ilike '%Easter%'
            OR item_name ilike '%Bunny%'
            OR item_name ilike '%Bunnies%'
            OR item_name ilike '%Egg %'
            OR item_name ilike '%Easter Basket%'
            OR item_name ilike '%Easter Grass%'
              )
            )
        OR (AISLE_NAME_L2 ilike '%Holiday Decor%'
          AND (item_name ilike '%Easter%'
            OR item_name ilike '%Bunny%'
            OR item_name ilike '%Bunnies%'
            OR item_name ilike '%Egg %'
            OR item_name ilike '%Easter Basket%'
            OR item_name ilike '%Easter Grass%'
              )
            )
        OR (AISLE_NAME_L2 ilike '%Seasonal%'
          AND (item_name ilike '%Easter%'
            OR item_name ilike '%Bunny%'
            OR item_name ilike '%Bunnies%'
            OR item_name ilike '%Egg %'
            OR item_name ilike '%Easter Basket%'
            OR item_name ilike '%Easter Grass%'
              )
            )
        OR (AISLE_NAME_L1 ilike '%Candy%'
          AND (item_name ilike '%Easter%'
            OR item_name ilike '%Bunny%'
            OR item_name ilike '%Bunnies%'
            OR item_name ilike '%Eggs%'
            OR item_name ilike '%Egg %'
            OR item_name ilike '%Peeps%'
              )
            )
        OR (AISLE_NAME_L2 ilike '%Sweet Toppings%'
          AND (item_name ilike '%Easter%'
            OR item_name ilike '%Bunny%'
            OR item_name ilike '%Bunnies%'
            OR item_name ilike '%Egg%'
              )
            )
        OR (AISLE_NAME_L2 ilike '%Clothing%'
          AND (item_name ilike '%Easter%'
            OR item_name ilike '%Bunny%'
            OR item_name ilike '%Bunnies%'
            OR item_name ilike '%Egg %'
            OR item_name ilike '%Easter Basket%'
            OR item_name ilike '%Easter Grass%'
              )
            )
        OR (AISLE_NAME_L2 ilike '%Hair Care%'
          AND (item_name ilike '%Easter%'
            OR item_name ilike '%Bunny%'
            OR item_name ilike '%Bunnies%'
            OR item_name ilike '%Egg %'
            OR item_name ilike '%Easter Basket%'
            OR item_name ilike '%Easter Grass%'
              )
            )
        OR (AISLE_NAME_L1 ilike '%Bakery%'
          AND (item_name ilike '%Easter%'
            OR item_name ilike '%Bunny%'
            OR item_name ilike '%Bunnies%'
              )
            )
        OR (AISLE_NAME_L2 ilike '%Sweets%'
          AND (item_name ilike '%Easter%'
            OR item_name ilike '%Bunny%'
            OR item_name ilike '%Bunnies%'
              )
            )
        OR (AISLE_NAME_L2 ilike '%Baking Mixes%'
          AND (item_name ilike '%Easter %'
            OR item_name ilike '%Bunny%'
            OR item_name ilike '%Bunnies%'
              )
            )
        OR (item_name ilike '%Easter basket%')
    )

;

with weekly_volume as (select
i.DELIVERY_CREATED_AT::date as dt,
date_trunc('week',i.DELIVERY_CREATED_AT::date) as week_cohort,
sum(i.quantity_requested) as item_quantity
from edw.cng.fact_non_rx_order_item_details i
join catalog_service_prod.public.product_item p on i.ITEM_MERCHANT_SUPPLIED_ID = p.MERCHANT_SUPPLIED_ID and i.BUSINESS_ID = p.DD_BUSINESS_ID
where p.product_category_id in (select distinct PRODUCT_CATEGORY_ID from dianedou.easter_2024_item_list)
  and i.delivery_created_at between dateadd('day', -7*8, '2023-04-10') and '2023-04-10'
group by 1,2
order by 1 desc,2)

select week_cohort
, sum(item_quantity)

from weekly_volume
group by 1;