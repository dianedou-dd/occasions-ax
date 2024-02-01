CREATE OR REPLACE TABLE dianedou.milestone_audience_can_20240131 AS
SELECT DISTINCT c.consumer_id
              , created_at::DATE                                            AS exposure_date
              , CASE
                    WHEN email_list_ids IN ('[2948678]') THEN 'treatment'
                    WHEN email_list_ids IN ('[2948679]') THEN 'control' END AS bucket
FROM segment_events_raw.iterable.subscribed AS a
     JOIN edw.core.dimension_users AS b
        ON a.email::VARCHAR = b.email::VARCHAR
     INNER JOIN edw.consumer.dimension_consumers c
        ON b.user_id = c.user_id
WHERE b.is_employee = 0
  AND b.email NOT LIKE '%doordash.com%'
  AND b.is_guest = FALSE
  AND LOWER(c.experience) = 'doordash'
  AND a.email_list_ids IN ('[2948678]', '[2948679]');


CREATE OR REPLACE TABLE dianedou.milestone_audience_can_20240131_clean AS
SELECT consumer_id
     , exposure_date
     , bucket AS overall_variant
     , bucket AS variant
FROM dianedou.milestone_audience_can_20240131;

CREATE OR REPLACE TEMP TABLE dtd_milestone_exposed_cx AS
SELECT *
FROM dianedou.milestone_audience_can_20240131_clean
;



CREATE OR REPLACE TEMP TABLE dtd_milestone_order_results AS
WITH dd_deliv AS (SELECT dd.*
                  FROM dimension_deliveries dd
                  -- inner join (select distinct store_id, cohort from public.fact_ads_promo_store_categorization) sg
                  -- on sg.store_id = dd.store_id
                  WHERE dd.is_filtered_core = TRUE
                    AND dd.is_caviar = 0
                    AND created_at::DATE BETWEEN '2023-12-01'::DATE AND '2023-12-12'::DATE ---- DTD CAMPAIGN PERIOD
-- and sg.cohort = 'SMB'

)
SELECT g.*,
       COUNT(DISTINCT dd.delivery_id)           AS orders,
       SUM(IFNULL(dd.variable_profit / 100, 0)) AS vp_sum
FROM dtd_milestone_exposed_cx g
     LEFT JOIN dd_deliv dd
        ON dd.creator_id = g.consumer_id
    AND
           dd.created_at::DATE BETWEEN g.exposure_date AND DATEADD(DAY, 10, g.exposure_date) ---- REPLACE WITH REDEMPTION WINDOW HERE
GROUP BY ALL
;


SELECT *
FROM DTD_MILESTONE_ORDER_RESULTS
LIMIT 3;

CREATE OR REPLACE TEMP TABLE dtd_milestone_order_rate AS
SELECT overall_variant             AS     consumer_bucket
     , COUNT(DISTINCT consumer_id) AS     cx

--- OR
     , SUM(orders)                 AS     order_count
     , AVG(orders)                 AS     order_rate
     , VARIANCE(orders)            AS     var_or

--- Converted Cx ---
     , SUM(IFF(orders > 0, 1, 0))  AS     converted_cx --843,837
     , AVG(IFF(orders > 0, 1, 0))  AS     conversions--0.047347
     , AVG(IFF(orders > 0, orders, NULL)) order_freq   --6.034498
     , SUM(IFNULL(vp_sum, 0))      AS     vp_sum
FROM DTD_MILESTONE_ORDER_RESULTS
GROUP BY 1;



CREATE OR REPLACE TABLE dianedou.dtd_milestone_lift AS
SELECT s1.consumer_bucket,
       s1.Cx,
       s1.cx + s2.cx                                                                            AS total_audience,
       s1.cx / total_audience                                                                   AS test_split,
       s2.order_rate                                                                            AS holdout_or,
       s1.order_rate                                                                            AS test_or,
       s2.order_count                                                                           AS holdout_orders,
       s1.order_count                                                                           AS test_orders,
       test_or - holdout_or                                                                     AS abs_lift,
       abs_lift / holdout_or                                                                    AS rel_lift,
       abs_lift / SQRT(s1.var_or / s1.cx + s2.var_or / s2.cx)                                   AS t_score,
       abs_lift * s1.cx                                                                         AS incr_orders,
       incr_orders * 36.5                                                                       AS incr_gmv,

       s2.converted_cx                                                                          AS holdout_cc,
       s1.converted_cx                                                                          AS test_cc,
       (s2.converted_cx + s1.converted_cx) / (s2.Cx + s1.Cx)                                    AS p_hat,

       s2.conversions                                                                           AS holdout_rr,
       s1.conversions                                                                           AS test_rr,
       s1.conversions - s2.conversions                                                          AS rr_abs_lift,
       s1.conversions / s2.conversions - 1                                                      AS rr_rel_lift,
       DIV0(rr_abs_lift * 100, SQRT(p_hat * (1 - p_hat)) * SQRT(10000 / s2.Cx + 10000 / s1.Cx)) AS z_score,

       s2.order_freq                                                                            AS holdout_of,
       s1.order_freq                                                                            AS test_of,
       s1.vp_sum - s2.vp_sum * s1.cx / s2.cx                                                    AS incr_vp,
       s1.vp_sum                                                                                AS trt_vp,
       s2.vp_sum                                                                                AS control_vp

FROM dtd_milestone_order_rate s1
     INNER JOIN dtd_milestone_order_rate s2
        ON s1.consumer_bucket ILIKE '%treatment%'
    AND s2.consumer_bucket ILIKE '%control%'
;

SELECT *
FROM dianedou.dtd_milestone_lift;

GRANT SELECT ON TABLE dianedou.dtd_milestone_lift TO read_only_users;