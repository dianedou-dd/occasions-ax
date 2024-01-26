-- =========================Request from jessica.reed=========================
-- Hi both! The team is working on another post-survey but this time for Deck the Doorstep in order to measure awareness, deal richness, marketing effectiveness, etc. -- and we could use your help to pull the list of users we'd like to hit, targeting EOD Friday 1/19 (based on whatever is possible).
-- What we're imagining:
-- 60k: Deck the Doorstep Deals Users
-- 30k: DashPass users
-- 30k: Classic users
-- 60k: Deck the Doorstep Deals NonUsers (but made at least 1 other purchase in between Dec 1-12)
-- 30k: DashPass users
-- 30k: Classic users
-- Attributes to include:
-- email
-- consumer_id
-- Used DTD promo (Y/N)
-- Number of DTD deals/promos used
-- Engaged with DTD CRM (Y/N)
-- Number of purchased during DTD - Dec 1-12th
-- Number of DD orders in month of December
-- DashPass user (Y/N)
-- ===========================================================================
SET crm_start_date = '2023-12-01'::DATE;
SET crm_end_date = '2023-12-12'::DATE;

-- Step 1: Deck the Doorstep Deals Users

--- Cx who has used DtD deals
CREATE OR REPLACE TABLE DIANEDOU.dtd_deal_users AS
WITH has_dd_spend AS (SELECT *
                      FROM yvonneliu.dtd_2023_master_performance_data
                      WHERE all_cx_discount - mx_funded_cx_discount > 0 ---- has DD funded spend
                        AND active_date BETWEEN '2023-12-01'::DATE AND '2023-12-12'::DATE
                      )

   , staging AS (SELECT tb1.creator_id
                      , tb1.delivery_id
                      , tb1.active_date
                      , tb1.campaign_id
---- double insurance. has marketing promo spend attached to this user.
                      , SUM(FDA_OTHER_PROMOTIONS_BASE + FDA_PROMOTION_CATCH_ALL + FDA_CONSUMER_RETENTION) -
                        SUM(FDA_BUNDLES_PRICING_DISCOUNT) -
                        SUM(DISCOUNT_GROUP_CONSUMER_TRAIN_DISCOUNT_COMPONENT_AMOUNT) / 100 AS promo_spend
                 FROM has_dd_spend tb1
                      LEFT JOIN fact_order_discounts_and_promotions_extended tb2 ON tb1.delivery_id = tb2.delivery_id
                     AND tb1.campaign_id = tb2.campaign_id
                     AND tb2.active_date BETWEEN '2023-12-01'::DATE AND '2023-12-12'::DATE
                 GROUP BY 1, 2, 3, 4
                 HAVING promo_spend > 0)
SELECT DISTINCT creator_id
FROM staging;

SELECT COUNT(DISTINCT creator_id)
FROM dianedou.dtd_deal_users;

GRANT SELECT ON TABLE DIANEDOU.dtd_deal_users TO read_only_users;

-- select * from static.dtd_2023_master_campaign_list where campaign_id in ('4de57691-12bc-4204-a4e3-83effaebcb2e');

-- step 2: Number of DTD deals/promos used
SELECT u.creator_id
     , COUNT(DISTINCT campaign_id) AS num_campaigns_redeemed
     , SUM(num_redemptions)        AS num_redemptions
FROM yvonneliu.dtd_deal_users u
     LEFT JOIN yvonneliu.dtd_2023_master_performance_data p
               ON u.creator_id = p.creator_id AND p.active_date BETWEEN '2023-12-01'::DATE AND '2023-12-12'::DATE
GROUP BY 1
ORDER BY 3 DESC
;

-- Step 3: Deck the Doorstep Deals NonUsers (but made at least 1 other purchase in between Dec 1-12)
--- cx exposed to dtd deals in the treatment bucket
CREATE OR REPLACE TABLE dianedou.dtd_treatment_cx AS
SELECT campaign_name,
       consumer_bucket,
       consumer_id,
       MIN(start_time_derived) AS start_time_derived,
       MIN(exposure_time)      AS min_exposure_time
FROM EDW.CONSUMER.CAMPAIGN_ANALYZER_EXPOSURES
WHERE campaign_name IN (
                        '[Big G] Non-DP Adoption Series XVertical', ---- STOCKED4U
                        '[Big G] DP Adoption Series XVertical', ---- STOCKED40
                        '[Big G] DP Adoption Series XVertical - Updated' ---- STOCKED40
    )
  AND exposure_time::DATE BETWEEN '2023-12-01'::DATE AND '2023-12-12'::DATE
  AND consumer_bucket NOT IN ('control', 'holdout', 'false')
GROUP BY 1, 2, 3

UNION
SELECT LOWER(s.program_name)                   AS campaign_name,
       CASE
           WHEN program_experiment_variant IN ('holdout', 'control') THEN 'holdout'
           ELSE program_experiment_variant END AS consumer_bucket,
       consumer_id,
       MIN(received_at)                        AS start_time_derived,
       MIN(received_at)                        AS min_exposure_time
FROM SEGMENT_EVENTS_RAW.CONSUMER_PRODUCTION.ENGAGEMENT_PROGRAM s
WHERE LOWER(s.program_name) IN ('ep_consumer_bigg_us_dp_adoption', --- STOCKED40
                                'ep_consumer_bigg_us_ndp_adoption' ---- STOCKED4U
    )
  AND received_at::DATE BETWEEN '2023-12-01'::DATE AND '2023-12-12'::DATE
  AND consumer_bucket NOT IN ('control', 'holdout', 'false')
GROUP BY 1, 2, 3

UNION
SELECT campaign_name,
       consumer_bucket,
       consumer_id,
       MIN(start_time_derived) AS start_time_derived,
       MIN(exposure_time)      AS min_exposure_time
FROM EDW.CONSUMER.CAMPAIGN_ANALYZER_EXPOSURES
WHERE campaign_name IN ('ep_consumer_dashmartfmx_us_v2_t3' ---- SUPER10EPS ---- 14d
    , 'ep_consumer_dashmartfmx_us_game_t1' ---- 50TRYDM ---- 7d
    )
  AND exposure_time::DATE BETWEEN '2023-12-01'::DATE AND '2023-12-12'::DATE
  AND consumer_bucket NOT IN ('control', 'holdout', 'false')
GROUP BY 1, 2, 3

UNION
---- dv exposures
SELECT experiment_name    AS campaign_name,
       result             AS consumer_bucket,
       bucket_key         AS consumer_id,
       '2023-12-01'::DATE AS start_time_derived,
       MIN(exposure_time) AS min_exposure_time
FROM iguazu.server_events_production.experiment_exposure
WHERE experiment_name IN (
--- we selected the highest spending DtD ad ops campaigns.
--- these campaigns have dv names that allow us to pull cx in the treatment bucket
                          'dollargeneral-120123-121223-dv' --change this to your DV name
    , 'aldi-120423-120823-dv', 'walgreens-120123-121223-dv', 'sprouts-120523-121123-dv', 'cvs-120123-121223-dv'
    )
  AND exposure_time::DATE BETWEEN '2023-12-01'::DATE AND '2023-12-12'::DATE --change this to the timeframe your test ran
  AND result = 'treatment'                                                  -- update to correct bucket name
GROUP BY 1, 2, 3, 4;

SELECT *
FROM dianedou.dtd_treatment_cx
LIMIT 10;
GRANT SELECT ON TABLE dianedou.dtd_treatment_cx TO read_only_users;

CREATE OR REPLACE TABLE dianedou.test_no_redempt AS
--- pull redemptions data from 12/1-12/12 (dtd campaign period)
WITH has_dtd_redempt AS (SELECT creator_id, SUM(num_redemptions) AS num_redempt
                         FROM yvonneliu.dtd_2023_master_performance_data
                         WHERE active_date BETWEEN '2023-12-01'::DATE AND '2023-12-12'::DATE
                         GROUP BY 1
                         HAVING num_redempt > 0)
--- double insurance. has dtd promo spend
   , has_promo_spend AS (SELECT creator_id
                              , SUM(FDA_OTHER_PROMOTIONS_BASE + FDA_PROMOTION_CATCH_ALL + FDA_CONSUMER_RETENTION) -
                                SUM(FDA_BUNDLES_PRICING_DISCOUNT) -
                                SUM(DISCOUNT_GROUP_CONSUMER_TRAIN_DISCOUNT_COMPONENT_AMOUNT) / 100 AS promo_spend
                         FROM fact_order_discounts_and_promotions_extended
                         WHERE 1 = 1
                           AND campaign_id IN (SELECT DISTINCT campaign_id FROM static.dtd_2023_master_campaign_list)
                           AND active_date BETWEEN '2023-12-01'::DATE AND '2023-12-12'::DATE
                         GROUP BY 1
                         HAVING promo_spend > 0)
-- placed at least one order
   , has_orders AS (SELECT creator_id
                         , COUNT(DISTINCT delivery_id) AS cnt
                    FROM PRODDB.PUBLIC.dimension_deliveries
                    WHERE is_filtered_core = TRUE
                      AND is_caviar = 0
                      AND active_date BETWEEN '2023-12-01'::DATE AND '2023-12-12'::DATE
                    GROUP BY 1
                    HAVING cnt > 0)

SELECT DISTINCT c.campaign_name
              , consumer_bucket
              , consumer_id::INTEGER AS consumer_id
FROM dianedou.dtd_treatment_cx c
     JOIN has_orders h
          ON c.consumer_id::INTEGER = h.creator_id::INTEGER --- but made at least 1 other purchase in between Dec 1-12
     LEFT JOIN has_dtd_redempt p ON c.consumer_id::INTEGER = p.creator_id::INTEGER
     LEFT JOIN has_promo_spend s ON c.consumer_id::INTEGER = s.creator_id::INTEGER

WHERE consumer_bucket NOT IN ('control', 'holdout', 'false')
  AND p.creator_id IS NULL --- no dtd redemptions
  AND s.creator_id IS NULL;

GRANT SELECT ON TABLE dianedou.test_no_redempt TO read_only_users;

CREATE OR REPLACE TABLE dianedou.dtd_deal_nonusers AS
SELECT DISTINCT consumer_id
FROM dianedou.test_no_redempt;


GRANT SELECT ON TABLE dianedou.dtd_deal_nonusers TO read_only_users;

CREATE OR REPLACE TABLE dianedou.master_dtd_survey_cx AS
SELECT creator_id::INTEGER AS consumer_id, 1 AS used_deal_flag
FROM dianedou.dtd_deal_users
UNION
SELECT consumer_id::INTEGER AS consumer_id, 0 AS used_deal_flag
FROM dianedou.dtd_deal_nonusers;

GRANT SELECT ON TABLE dianedou.master_dtd_survey_cx TO read_only_users;

-- Step 4: Attribute tables for DtD deals users
CREATE OR REPLACE TABLE dianedou.dtd_crm_eng AS
SELECT eng.consumer_id,
       SUM(CASE WHEN notification_channel = 'EMAIL' THEN open_within_24h ELSE 0 END) AS open_email_within_24h_count,
       SUM(CASE WHEN notification_channel = 'PUSH' THEN open_within_24h ELSE 0 END)  AS open_push_within_24h_count,
       SUM(CASE
               WHEN notification_channel = 'EMAIL' THEN LINK_CLICK_WITHIN_24H
               ELSE 0 END)                                                           AS LINK_CLICK_email_WITHIN_24H_count,
       SUM(CASE
               WHEN notification_channel = 'PUSH' THEN LINK_CLICK_WITHIN_24H
               ELSE 0 END)                                                           AS LINK_CLICK_push_WITHIN_24H_count
FROM edw.consumer.fact_consumer_notification_engagement eng
WHERE sent_at_date BETWEEN $crm_start_date AND $crm_end_date
GROUP BY 1;


CREATE OR REPLACE TABLE dianedou.dtd_dec_1_12_orders AS
WITH dd_dtd_deliv AS (SELECT creator_id
                           , IFNULL(COUNT(DISTINCT delivery_id), 0) AS num_dtd_orders
                      FROM PRODDB.PUBLIC.dimension_deliveries
                      WHERE is_filtered_core = TRUE
                        AND is_caviar = 0
                        AND CREATED_AT::DATE BETWEEN '2023-12-01'::DATE AND '2023-12-12'::DATE
                      GROUP BY 1)

SELECT DISTINCT a.CREATOR_ID AS consumer_id
              , num_dtd_orders
FROM yvonneliu.master_dtd_survey_cx a
     LEFT JOIN dd_dtd_deliv dd ON dd.creator_id = a.CREATOR_ID;


CREATE OR REPLACE TABLE dianedou.dtd_dec_orders AS
WITH dd_dec_deliveries AS (SELECT *
                           FROM PRODDB.PUBLIC.dimension_deliveries
                           WHERE is_filtered_core = TRUE
                             AND is_caviar = 0
                             AND DATE_TRUNC('month', CREATED_AT::DATE) = '2023-12-01'
                           GROUP BY 1)

SELECT a.CREATOR_ID                           AS consumer_id
--        a.consumer_bucket,
--        a.consumer_id,
--        a.start_time_derived::DATE                AS campaign_start,
--        a.end_time_derived::DATE                  AS campaign_end,
--        IFNULL(COUNT(DISTINCT dd.creator_id), 0)  AS active_cx,
     , IFNULL(COUNT(DISTINCT DELIVERY_ID), 0) AS num_dec_orders
FROM yvonneliu.master_dtd_survey_cx a --yvonneliu.bts_crm_exposed_cx a
     LEFT JOIN dd_dec_deliveries dd ON dd.creator_id = a.CREATOR_ID
--                AND dd.created_at::DATE BETWEEN a.start_time_derived::DATE AND a.end_time_derived::DATE
GROUP BY 1, 2 --, 3, 4, 5
;

-- Step 5: Attribute tables for DtD deals users
CREATE OR REPLACE TABLE dianedou.dtd_survey_sample_2023 AS
WITH sub AS (SELECT u.creator_id                             AS consumer_id
                  , u.used_deal_flag --- whether the Cx has used DtD deals or not
                  , IFNULL(COUNT(DISTINCT p.campaign_id), 0) AS num_campaigns_redeemed
                  , IFNULL(SUM(p.num_redemptions), 0)        AS num_redemptions
             FROM yvonneliu.master_dtd_survey_cx u
                  LEFT JOIN yvonneliu.dtd_2023_master_performance_data p
                            ON u.creator_id = p.creator_id AND p.active_date BETWEEN '2023-12-01'::DATE AND '2023-12-12'::DATE
             GROUP BY 1, 2
             ORDER BY 3 DESC)

   , dashpass_sub AS (SELECT DISTINCT CONSUMER_ID
                      FROM edw.consumer.fact_consumer_subscription__daily__extended
                      WHERE dte = (SELECT MAX(dte) FROM edw.consumer.fact_consumer_subscription__daily__extended)
                        AND (SELECT MAX(dte)
                             FROM edw.consumer.fact_consumer_subscription__daily__extended) BETWEEN start_time AND end_time
                        AND DYNAMIC_SUBSCRIPTION_STATUS IN
                            ('active_free_subscription', 'active_paid', 'active_trial', 'trial_waiting_for_payment',
                             'dtp_waiting_for_payment', 'paid_waiting_for_payment'))


SELECT DISTINCT u.consumer_id,
                u.used_deal_flag,
                du.email                                AS email,
                IFF(u.num_campaigns_redeemed > 0, 1, 0) AS used_promo_y_n,  ---- has this cx redeemed bts promos -- y/n
                u.num_campaigns_redeemed                AS num_promos_used,
                u.num_redemptions                       AS num_of_dtd_redemptions,
                dt.num_dtd_orders                       AS num_of_dtd_total_purchases,
                IFF(e.consumer_id IS NULL, 0, 1)        AS engaged_crm_y_n, ---- Engaged with CRM  (Open Email/Push in 24H)
                IFF(o.num_dec_orders IS NULL, 0, 1)     AS num_dec_orders,  ---- Placed order during BTS
                IFF(dp.consumer_id IS NULL, 0, 1)       AS dashpass_y_n     ---- if they have dashpass or not

FROM sub u
     LEFT JOIN dianedou.dtd_crm_eng e ON u.consumer_id = e.consumer_id AND
                                         e.open_email_within_24h_count + e.open_push_within_24h_count +
                                         e.link_click_email_within_24h_count + e.link_click_push_within_24h_count > 0
     LEFT JOIN dianedou.dtd_dec_orders o ON u.consumer_id = o.consumer_id
     LEFT JOIN yvonneliu.dtd_dec_1_12_orders dt ON u.consumer_id = dt.consumer_id
     LEFT JOIN dashpass_sub dp ON u.consumer_id = dp.consumer_id
     LEFT JOIN public.dimension_users du
               ON du.consumer_id = u.consumer_id AND du.experience = 'doordash' AND du.is_guest = FALSE;
;


--- get 60K each
-- What we're imagining:
-- 60k: Deck the Doorstep Deals Users
-- 30k: DashPass users
SELECT *
FROM dianedou.dtd_survey_sample_2023
WHERE used_deal_flag = 1
  AND dashpass_y_n = 1
LIMIT 30000;
-- 30k: Classic users
SELECT *
FROM dianedou.dtd_survey_sample_2023
WHERE used_deal_flag = 1
  AND dashpass_y_n = 0
LIMIT 30000;
-- 60k: Deck the Doorstep Deals NonUsers (but made at least 1 other purchase in between Dec 1-12)
-- 30k: DashPass users
SELECT *
FROM dianedou.dtd_survey_sample_2023
WHERE used_deal_flag = 0
  AND dashpass_y_n = 1
LIMIT 30000;
-- 30k: Classic users
SELECT *
FROM dianedou.dtd_survey_sample_2023
WHERE used_deal_flag = 0
  AND dashpass_y_n = 0
LIMIT 30000;





