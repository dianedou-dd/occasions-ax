SET campaign_start = '2023-12-01'::DATE;
SET campaign_end = '2023-12-12'::DATE;

CREATE OR REPLACE TABLE dianedou.dtd_2023_non_tested_campaign_impact AS
WITH calc_aov AS (SELECT AVG(IFNULL(gov / 100, 0)) AS avg_gov, campaign_id
                  FROM edw.invoice.fact_promotion_deliveries
                  WHERE active_date >= $campaign_start - 30 ---- past 30 days' gov
                  GROUP BY 2)

   , agg_performance AS (SELECT ACTIVE_DATE,
                                CAMPAIGN_ID,
                                vertical,
                                adjusted_cohort,
                                SUM(MX_FUNDED_PROMO_DOLLARS)         AS MX_FUNDED_PROMO_DOLLARS,
                                SUM(ALL_CX_DISCOUNT)                 AS ALL_CX_DISCOUNT,
                                SUM(MX_FUNDED_CX_DISCOUNT)           AS MX_FUNDED_CX_DISCOUNT,
                                SUM(MX_MARKETING_FEE)                AS MX_MARKETING_FEE,
                                SUM(DD_SUBTOTALS)                    AS DD_SUBTOTALS,
                                SUM(MX_FUNDED_PROMO_SUBTOTALS)       AS MX_FUNDED_PROMO_SUBTOTALS,
                                SUM(DP_CX_SAVINGS)                   AS DP_CX_SAVINGS,
                                COUNT(DISTINCT delivery_id)          AS num_redemptions,
                                COUNT(DISTINCT CREATOR_ID)           AS num_redeemers,
                                num_redemptions / num_redeemers      AS redemp_per_cx,
                                IFNULL(SUM(IFNULL(gov, 0) / 100), 0) AS total_gov,
                                DIV0(total_gov, num_redemptions)     AS avg_gov
                         FROM dianedou.dtd_2023_cx_level_performance
                         GROUP BY 1, 2, 3, 4)

   , staging AS (SELECT DISTINCT a.active_date
                               , a.campaign_id
                               , a.vertical
                               , a.adjusted_cohort

                               , IFF(b.avg_gov > 0, b.avg_gov, CASE WHEN vertical = 'Other' THEN 36.12
                                                                   ELSE 37.48 END) AS adjusted_aov_step1
                               , IFNULL(CASE WHEN adjusted_aov_step1 > 83 THEN 83
                                             WHEN adjusted_aov_step1 < 30 THEN 30
                                             ELSE adjusted_aov_step1 END, 0)       AS adjusted_aov

                               , CASE
                                     WHEN vertical = 'Other' THEN (
                                         mx_funded_cx_discount --- assumption: $1 promo spend = $1 incr GMV
                                             + (all_cx_discount - mx_funded_cx_discount) /
                                               (all_cx_discount / num_redemptions) * 0.63 * adjusted_aov ---- Rx
                                         )
                                     WHEN vertical NOT IN ('Other') THEN (
                                         mx_funded_cx_discount --- assumption: $1 promo spend = $1 incr GMV
                                             + (all_cx_discount - mx_funded_cx_discount) /
                                               (all_cx_discount / num_redemptions) * 0.46 * adjusted_aov ---- Big G GOV
                                         )
                                     ELSE 0 END                                    AS incr_gmv_raw
                               , incr_gmv_raw / 2                                  AS incr_gmv
                               , DIV0(incr_gmv, adjusted_aov)                      AS incr_orders
                               , b.avg_gov
                               , mx_funded_cx_discount
                               , all_cx_discount
                               , num_redemptions
                 FROM agg_performance a
                      LEFT JOIN calc_aov b
                         ON a.campaign_id = b.campaign_id
                 --                  WHERE campaign_week IS NOT NULL
                 ORDER BY 4 DESC)

SELECT active_date,
       SUM(incr_gmv)    AS incr_gmv,
       SUM(incr_orders) AS incr_orders
FROM staging
GROUP BY 1
;
GRANT SELECT ON TABLE dianedou.dtd_2023_non_tested_campaign_impact TO read_only_users;


CREATE OR REPLACE TABLE dianedou.dtd_2023_tested_campaign_impact AS
with raw as (
select * from yvonneliu.incr_gmv_reskin_daily_dashmart_50trydm
where last_updated = (select max(last_updated) from yvonneliu.incr_gmv_reskin_daily_dashmart_50trydm)

union

select * from yvonneliu.incr_gmv_reskin_daily_dashmart_super10eps
where last_updated = (select max(last_updated) from yvonneliu.incr_gmv_reskin_daily_dashmart_super10eps)

union

select * from yvonneliu.incr_gmv_reskin_daily_grocery --- done
where last_updated = (select max(last_updated) from yvonneliu.incr_gmv_reskin_daily_grocery)
),


sub as (select

sum(incr_gmv/2) as incremental_gmv,
sum(volume_3m/2) as incremental_orders

from raw
where calendar_date between '2023-12-01' and '2023-12-12'

union all

select incr_gmv/2, incr_orders/2
from dianedou.dtd_milestone_lift )

select sum(incremental_gmv) as inc_gmv , sum(INCRemental_ORDERS) as inc_orders from sub;

GRANT SELECT ON TABLE dianedou.dtd_2023_tested_campaign_impact TO read_only_users;

SELECT *
FROM dianedou.dtd_2023_topup_campaign_impact
LIMIT 10;

CREATE OR REPLACE TABLE dianedou.dtd_2023_topup_campaign_impact AS
with staging as (select *,
greatest(incr_gmv/2,0) as increm_gmv,
greatest(incr_volume/2,0) as increm_orders
 from yvonneliu.dtd_top_up_daily where last_updated = (select max(last_updated) from yvonneliu.dtd_top_up_daily))

select calendar_date
, 14290000/12/2 as incremental_gmv_goal
-- , sum(increm_gmv) as incremental_gmv
, sum(increm_orders*36/2) as incremental_gmv
, sum(increm_orders) as incremental_orders
, div0(incremental_gmv,incremental_gmv_goal) as pacing
from staging group by 1,2;

GRANT SELECT ON TABLE dianedou.dtd_2023_topup_campaign_impact  TO read_only_users;
