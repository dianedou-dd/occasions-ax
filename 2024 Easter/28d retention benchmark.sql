----back to school 2023
SET campaign_start = '2023-07-20';
SET campaign_end = '2023-09-04';

WITH campaign_list AS (SELECT DISTINCT "Campaign Series" AS campaign_name, "Campaign Manager Campaign ID" AS campaign_id

                       FROM dianedou.CRM_campaign_info_tracker_snapshot_03072024
                       WHERE "Campaign Series" IN (
                                                   '[Big G] Non-DP Adoption Series XVertical', ---- STOCKED4U
                                                   '[Big G] DP Adoption Series XVertical', ---- STOCKED40
                                                   '[Big G] DP Adoption Series XVertical - Updated',---- STOCKED40
                                                   '[Big G] DP Adoption Series Single Vertical',
                                                   '[Big G] DP Adoption Series XVertical',
                                                   '[Big G] Non-DP Adoption Series Single Vertical',
                                                   '[Big G] Non-DP Adoption Series XVertical',
                                                   'BackToSchool | Classics | HighIntent | SHOP23 | Q3\'23',
                                                   'BackToSchool | DP | HighIntent | SHOP25 | Q3\'23',
                                                   'ep_consumer_dashmartfmx_us_game_t1',
                                                   'ep_consumer_dashmartfmx_us_v2_t3'
                           )

                       UNION

                       SELECT DISTINCT "Name of the Initiative", "Campaign Manager Campaign ID"
                       FROM dianedou.CRM_campaign_info_tracker_One_off_snapshot_03072024
                       WHERE "Name of the Initiative" IN (
                                                          '[Big G] Non-DP Adoption Series XVertical', ---- STOCKED4U
                                                          '[Big G] DP Adoption Series XVertical', ---- STOCKED40
                                                          '[Big G] DP Adoption Series XVertical - Updated',---- STOCKED40
                                                          '[Big G] DP Adoption Series Single Vertical',
                                                          '[Big G] DP Adoption Series XVertical',
                                                          '[Big G] Non-DP Adoption Series Single Vertical',
                                                          '[Big G] Non-DP Adoption Series XVertical',
                                                          'BackToSchool | Classics | HighIntent | SHOP23 | Q3\'23',
                                                          'BackToSchool | DP | HighIntent | SHOP25 | Q3\'23',
                                                          'ep_consumer_dashmartfmx_us_game_t1',
                                                          'ep_consumer_dashmartfmx_us_v2_t3'
                           ))

   , redeemers AS (SELECT DISTINCT dd.CONSUMER_ID
                   FROM edw.invoice.fact_promotion_deliveries dd
                        JOIN campaign_list lst
                           ON dd.CAMPAIGN_ID = lst.CAMPAIGN_ID
                       AND dd.active_date BETWEEN $campaign_start AND $campaign_end
                       AND consumer_discount > 0)

   , retention AS (SELECT CEIL((DATEDIFF('day', $campaign_end, dd.active_date)) / 28)         AS p_n
                        , COUNT(DISTINCT dd.CREATOR_ID)                                       AS retained_user
                        , retained_user / (SELECT COUNT(DISTINCT consumer_id) FROM redeemers) AS retention

                   FROM dimension_deliveries dd
                        JOIN redeemers red
                           ON dd.CREATOR_ID = red.CONSUMER_ID AND
                              dd.active_date > $campaign_end --dtd.first_redemption_date
                       AND is_filtered_core = 1
                       AND is_caviar = 0
                   GROUP BY ALL)

SELECT *
FROM retention;


---halloween 2023

SET campaign_start = '2023-10-02';
SET campaign_end = '2023-10-31';

WITH redeemers AS (SELECT DISTINCT dd.CONSUMER_ID
                   FROM edw.invoice.fact_promotion_deliveries dd
                        JOIN dianedou.Halloween_2023_campaign_list lst
                           ON dd.CAMPAIGN_ID = lst."CAMPAIGN ID"
                       AND dd.active_date BETWEEN $campaign_start AND $campaign_end
                       AND consumer_discount > 0)

   , retention AS (SELECT CEIL((DATEDIFF('day', $campaign_end, dd.active_date)) / 28)         AS p_n
                        , COUNT(DISTINCT dd.CREATOR_ID)                                       AS retained_user
                        , retained_user / (SELECT COUNT(DISTINCT consumer_id) FROM redeemers) AS retention

                   FROM dimension_deliveries dd
                        JOIN redeemers red
                           ON dd.CREATOR_ID = red.CONSUMER_ID AND
                              dd.active_date > $campaign_end --dtd.first_redemption_date
                       AND is_filtered_core = 1
                       AND is_caviar = 0
                   GROUP BY ALL)

SELECT *
FROM retention
ORDER BY 1;
