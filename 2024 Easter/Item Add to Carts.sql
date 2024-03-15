SELECT order_date,
       ORDER_WEEK,
       DATE_TRUNC(MONTH, ORDER_WEEK)                                                     month,
       daypart,
       cx_profile_order_labels.business_line,
       income_type,
       locale_type,
       region_name,
       country_id,
       submit_platform,
       vertical_name,
       is_snap,
       is_wbd,
       is_promo,
       is_dbd,
       is_dashpass,
       new_cx_dd,
       new_cx_nv,
       new_cx_vertical,
       nv_cx_purchaser_status_bod,
       mp_segment,
       is_occx,
       ps_cohort,
       active_dm_submarket_flag
        ,
       CASE
           WHEN LATE_NIGHT_IMPULSE_SNACKERS IS NOT NULL THEN 'Late Night Impulse Snackers'
           WHEN SNACKERS IS NOT NULL THEN 'Impulse Snackers'
           WHEN URGENT_CANT_LEAVE_HOME_HANGOVER IS NOT NULL THEN 'Urgent Cant Leave Home: Hangover'
           WHEN URGENT_CANT_LEAVE_HOME_SICK IS NOT NULL THEN 'Urgent Cant Leave Home: Sick'
           WHEN URGENT_CANT_LEAVE_HOME_KIDS_ANCHOR_ITEM IS NOT NULL THEN 'Urgent Cant Leave Home: Kids'
           WHEN URGENT_CANT_LEAVE_HOME_PETS_ANCHOR_ITEM IS NOT NULL THEN 'Urgent Cant Leave Home: Pets'
           WHEN PARTY_STOCK_UP IS NOT NULL THEN 'Party Stock Up'
           WHEN PARTY_HAS_STARTED_URGENT_TOP_UP IS NOT NULL THEN 'Party Has Started, Urgent Top Up'
           WHEN LAST_MINUTE_GIFTING IS NOT NULL THEN 'Last Minute Gifting'
           WHEN GROCERY_TOP_UP IS NOT NULL THEN 'Grocery Top Up'
           WHEN PERSONAL_ITEMS_DONT_WANT_TO_SHOP_INPERSON IS NOT NULL THEN 'Personal Items, Dont Want to Shop in Person'
           WHEN HEAVY_BULK_ITEMS IS NOT NULL THEN 'Heavy Bulk Items'
           WHEN AWAY_FROM_HOME_SAME_CITY IS NOT NULL THEN 'Away From Home, Same City'
           WHEN AWAY_FROM_HOME_DIFF_CITY IS NOT NULL THEN 'Away from Home, Diff City'
           WHEN party_alcohol IS NOT NULL THEN 'Party: Alcohol'
           WHEN Bad_weather_flag IS NOT NULL THEN 'Bad Weather'
           WHEN occasion IS NOT NULL THEN 'Occasion'
           WHEN spearfisher IS NOT NULL THEN 'Spearfisher'
           WHEN retail_beauty IS NOT NULL THEN 'Retail: Beauty'
           WHEN scheduled_order IS NOT NULL THEN 'Scheduled Order'
           WHEN immediate_consumption IS NOT NULL THEN 'Immediate Consumption'
           WHEN premium_protein_basket_starter IS NOT NULL THEN 'Premium Protein'

           ELSE 'No Profile' END                                                      AS shopper_profiles
        ,
       CASE
           WHEN shopper_profiles = 'No Profile' THEN 'No Profile'
           ELSE TRIM(CONCAT(COALESCE(CONCAT(LATE_NIGHT_IMPULSE_SNACKERS, ' , '), '')
                         , COALESCE(CONCAT(SNACKERS, ' , '), '')
                         , COALESCE(CONCAT(URGENT_CANT_LEAVE_HOME_HANGOVER, ' , '), '')
                         , COALESCE(CONCAT(URGENT_CANT_LEAVE_HOME_SICK, ' , '), '')
                         , COALESCE(CONCAT(URGENT_CANT_LEAVE_HOME_KIDS_ANCHOR_ITEM, ' , '), '')
                         , COALESCE(CONCAT(URGENT_CANT_LEAVE_HOME_PETS_ANCHOR_ITEM, ' , '), '')
                         , COALESCE(CONCAT(PARTY_STOCK_UP, ' , '), '')
                         , COALESCE(CONCAT(PARTY_HAS_STARTED_URGENT_TOP_UP, ' , '), '')
                         , COALESCE(CONCAT(LAST_MINUTE_GIFTING, ' , '), '')
                         , COALESCE(CONCAT(GROCERY_TOP_UP, ' , '), '')
                         , COALESCE(CONCAT(PERSONAL_ITEMS_DONT_WANT_TO_SHOP_INPERSON, ' , '), '')
                         , COALESCE(CONCAT(HEAVY_BULK_ITEMS, ' , '), '')
                         , COALESCE(CONCAT(AWAY_FROM_HOME_SAME_CITY, ' , '), '')
                         , COALESCE(CONCAT(AWAY_FROM_HOME_DIFF_CITY, ' , '), '')
                         , COALESCE(CONCAT(party_alcohol, ' , '), '')
                         , COALESCE(CONCAT(occasion, ' , '), '')
                         , COALESCE(CONCAT(spearfisher, ' , '), '')
                         , COALESCE(CONCAT(retail_beauty, ' , '), '')
                         , COALESCE(CONCAT(scheduled_order, ' , '), '')
                         , COALESCE(CONCAT(immediate_consumption, ' , '), '')
                         , COALESCE(CONCAT(premium_protein_basket_starter, ' , '), '')
                         , COALESCE(CONCAT(Bad_weather_flag, ' , '), '')), ' , ') END AS order_label
        ,
       COALESCE(SNACKERS, 'z. General')                                                  IMPULSE_SNACKERS,
       COALESCE(LATE_NIGHT_IMPULSE_SNACKERS, 'z. General')                               LATE_NIGHT_IMPULSE_SNACKERS,
       COALESCE(URGENT_CANT_LEAVE_HOME_HANGOVER, 'z. General')                           URGENT_CANT_LEAVE_HOME_HANGOVER,
       COALESCE(URGENT_CANT_LEAVE_HOME_SICK, 'z. General')                               URGENT_CANT_LEAVE_HOME_SICK,
       COALESCE(URGENT_CANT_LEAVE_HOME_KIDS_ANCHOR_ITEM, 'z. General')                   URGENT_CANT_LEAVE_HOME_KIDS_ANCHOR_ITEM,
       COALESCE(URGENT_CANT_LEAVE_HOME_PETS_ANCHOR_ITEM, 'z. General')                   URGENT_CANT_LEAVE_HOME_PETS_ANCHOR_ITEM,
       --coalesce(party_alcohol, 'z. General') party_alcohol,
       COALESCE(PARTY_STOCK_UP, 'z. General')                                            PARTY_STOCK_UP,
       COALESCE(PARTY_HAS_STARTED_URGENT_TOP_UP, 'z. General')                           PARTY_HAS_STARTED_URGENT_TOP_UP,
       COALESCE(LAST_MINUTE_GIFTING, 'z. General')                                       LAST_MINUTE_GIFTING,
       COALESCE(grocery_top_up, 'z. General')                                            grocery_top_up,
       COALESCE(party_alcohol, 'z. General')                                             party_alcohol,
       COALESCE(personal_items_dont_want_to_shop_inperson, 'z. General')                 personal_items_dont_want_to_shop_inperson,
       COALESCE(heavy_bulk_items, 'z. General')                                          heavy_bulk_items,
       COALESCE(away_from_home_same_city, 'z. General')                                  away_from_home_same_city,
       COALESCE(away_from_home_diff_city, 'z. General')                                  away_from_home_diff_city,
       COALESCE(BAD_WEATHER_FLAG, 'z. General')                                          BAD_WEATHER,
       COALESCE(occasion, 'z. General')                                                  occasion,
       COALESCE(retail_beauty, 'z. General')                                             retail_beauty,
       COALESCE(spearfisher, 'z. General')                                               spearfisher,
       COALESCE(scheduled_order, 'z. General')                                           scheduled_order,
       COALESCE(immediate_consumption, 'z. General')                                     immediate_consumption,
       COALESCE(premium_protein_basket_starter, 'z. General')                            premium_protein_basket_starter,


       CASE
           WHEN SNACKERS IS NULL
               AND LATE_NIGHT_IMPULSE_SNACKERS IS NULL
               AND URGENT_CANT_LEAVE_HOME_HANGOVER IS NULL
               AND URGENT_CANT_LEAVE_HOME_SICK IS NULL
               AND URGENT_CANT_LEAVE_HOME_KIDS_ANCHOR_ITEM IS NULL
               AND URGENT_CANT_LEAVE_HOME_PETS_ANCHOR_ITEM IS NULL
               AND PARTY_STOCK_UP IS NULL
               AND PARTY_HAS_STARTED_URGENT_TOP_UP IS NULL
               AND LAST_MINUTE_GIFTING IS NULL
               AND GROCERY_TOP_UP IS NULL
               AND PERSONAL_ITEMS_DONT_WANT_TO_SHOP_INPERSON IS NULL
               AND HEAVY_BULK_ITEMS IS NULL
               AND AWAY_FROM_HOME_SAME_CITY IS NULL
               AND AWAY_FROM_HOME_DIFF_CITY IS NULL
               AND party_alcohol IS NULL
               AND occasion IS NULL
               AND spearfisher IS NULL
               AND Bad_weather_flag IS NULL THEN 'No Profile - General Order'
           ELSE 'Has Profile' END                                                     AS general_order_flag,


       l1_name,
       l2_name,
       anchor_item_name,
       COUNT(DISTINCT delivery_id)                                                       orders

FROM brycebeckwith.cx_profile_order_labels

WHERE cx_profile_order_labels.BUSINESS_LINE IN (
                                                '3P Convenience',
                                                'Grocery',
                                                '1P Convenience',
                                                'Alcohol',
                                                'Pets',
                                                'Active & Office',
                                                'Home & Wellness',
                                                'Flowers'
    )
GROUP BY ALL;

SELECT
    Business_line
    , ANCHOR_ITEM_NAME
    , count(distinct delivery_id) as num_deliveries
FROM brycebeckwith.cx_profile_order_labels a
join dianedou.easter_2024_item_list b
on a.
WHERE ANCHOR_ITEM_AISLE_NAME_L2 IN (SELECT DISTINCT AISLE_NAME_L2 FROM dianedou.easter_2024_item_list)
  AND created_at::DATE BETWEEN '2023-04-06' AND '2023-04-09'
GROUP BY ALL
ORDER BY 3 DESC
-- LIMIT 50
;

select *
FROM brycebeckwith.cx_profile_order_labels a
limit 100;

SELECT count(distinct delivery_id) as num_deliveries
FROM brycebeckwith.cx_profile_order_labels a
-- join dianedou.easter_2024_item_list b
-- on a.ANCHOR_ITEM_NAME = b.ITEM_NAME
WHERE ANCHOR_ITEM_AISLE_NAME_L2 IN (SELECT DISTINCT AISLE_NAME_L2 FROM dianedou.easter_2024_item_list)
  AND created_at::DATE BETWEEN '2023-04-06' AND '2023-04-09'
GROUP BY ALL
-- ORDER BY 3 DESC