create or replace temp table _vday_orders_for_traffic as
   select
    convert_timezone('UTC','America/Los_Angeles',ddo.created_at)::date as created_at_pst,
    dd.creator_id,
    nv.vertical_name,
    dd.delivery_id,
    nv.business_name,
    case
        when coalesce(mh.days_since_last_purchase,-1) between 0 and 28 then 'Active'
        when coalesce(mh.days_since_last_purchase,-1) between 29 and 90 then 'Dormant'
        when coalesce(mh.days_since_last_purchase,-1) > 90 then 'Churned'
        else 'New'
    end as mp_segment,
    dd.is_bundle_order,
    dayname(dd.active_date) as dow,
    case when dd.order_number_nv = 1 then 'New' else 'Existing' end as new_cx_nv,
    case when v.delivery_id is not null then 1 else 0 end as is_flowers_order,
    case when nv.vertical_name = 'Flowers' or nv.business_name = 'The Flower & Gift Boutique' then 1 else 0 end as has_flowers_fgb_order,
    ddo.is_asap,
    case when vl.delivery_id is not null then 1 else 0 end is_vday_l1_order,
    coalesce(vc.product_surface_area, 'Unattributable') as product_surface_area,
    coalesce(vc.product_feature, 'Unattributable') as product_feature
  from stefaniemontgomery.dimension_deliveries_ranked dd
  left join edw.cng.dimension_new_vertical_store_tags nv
      on nv.store_id = dd.store_id
      and is_filtered_mp_vertical = 1
left join mattheitz.mh_customer_authority mh
    on mh.creator_id = dd.creator_id
    and mh.dte = dd.active_date
left join stefaniemontgomery._vday24_orders v
    on v.delivery_id = dd.delivery_id
left join public.dimension_deliveries ddo
    on dd.delivery_id = ddo.delivery_id
left join stefaniemontgomery._vday24_orders_l1 vl
    on vl.delivery_id = dd.delivery_id
left join r vc
    on vc.order_uuid = dd.order_cart_uuid
  where dd.country_id = 1
  and nv.vertical_name is not null
  and (created_at_pst::date between '2024-02-01'::date and '2024-02-16'::date)
;

create or replace table _vday_traffic as
select
    convert_timezone('UTC','America/Los_Angeles', pfa.page_event_time)::date as event_date_pt,
    try_to_numeric(pfa.user_id) as consumer_id,
    count(distinct pfa.store_id) as distinct_nv_stores,
    count(distinct case when nv.vertical_name = 'Grocery' then pfa.store_id end) as distinct_grocery_stores,
    count(distinct case when nv.vertical_name in ('Flowers','1P Convenience') then pfa.store_id end) as distinct_flowers_stores
from stefaniemontgomery.fact_consumer_product_feature_attribution_extended pfa
left join edw.cng.dimension_new_vertical_store_tags nv
    on nv.store_id = pfa.store_id
    and is_filtered_mp_vertical = 1
where (event_date_pt, try_to_numeric(pfa.user_id)) in (select created_at_pst, creator_id from _vday_orders_for_traffic)
and nv.vertical_name is not null
group by all
;


select
    o.vertical_name,
    o.created_at_pst,
    o.mp_segment,
    o.is_bundle_order,
    o.new_cx_nv,
    o.is_asap,
    o.product_surface_area,
    -- greatest(o.is_flowers_order, o.is_vday_l1_order) as is_vday_order,
    o.is_flowers_order as is_flowers_order,
    case when distinct_grocery_stores > 0 then 1 else 0 end as visited_grocery_store,
    case when distinct_flowers_stores > 0 then 1 else 0 end as visited_flowers_store,
    case
        when distinct_nv_stores = 1 then '1'
        when distinct_nv_stores between 2 and 4 then '2-4'
        when distinct_nv_stores > 4 then '5+'
    end  as distinct_nv_stores_bucket,
    count(*) as volume,
    sum(distinct_nv_stores) as sum_distinct_nv_stores,
    sum(distinct_grocery_stores) as sum_distinct_grocery_stores,
    sum(distinct_flowers_stores) as sum_distinct_flowers_stores
from _vday_orders_for_traffic o
left join _vday_traffic t
    on o.created_at_pst = t.event_date_pt
    and o.creator_id = t.consumer_id
where t.consumer_id is not null
and o.vertical_name in ('1P Convenience','Flowers','Grocery')
and created_at_pst between '2024-02-12'::date and '2024-02-14'::date
group by all;