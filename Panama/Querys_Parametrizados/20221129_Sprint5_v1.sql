-----------------------------------------------------------------------------------------
------------------------- SPRINT 5 PARAMETRIZADO - V1 -----------------------------------
-----------------------------------------------------------------------------------------

WITH 

parameters AS (
-- Seleccionar el mes en que se desea realizar la corrida
SELECT DATE_TRUNC('month',DATE('2022-10-01')) AS input_month
)

,fmc_table AS (
SELECT month, B_Final_TechFlag, B_FMCSegment, B_FMCType, E_Final_TechFlag, E_FMCSegment, E_FMCType, b_final_tenure, e_final_tenure, B_FixedTenure, E_FixedTenure, finalchurnflag, fixedchurntype, fixedchurnflag, fixedmainmovement, waterfall_flag, finalaccount, fixedaccount, f_activebom, mobile_activeeom, mobilechurnflag
FROM "lla_cco_int_ana_prod"."cwp_fmc_churn_prod"
WHERE month = DATE(dt)
    AND month = (SELECT input_month FROM parameters)
)

,repeated_accounts AS (
SELECT month, fixedaccount, count(*) AS records_per_user
FROM fmc_table
GROUP BY 1,2
)

,fmc_table_adj AS (
SELECT F.*, records_per_user
FROM fmc_table F LEFT JOIN repeated_accounts R ON F.fixedaccount = R.fixedaccount AND F.month = R.month
)

---------------interactions Table Fields---------------------------------
,clean_interaction_time AS (
SELECT *
FROM "db-stage-prod"."interactions_cwp"
    WHERE (CAST(interaction_start_time AS VARCHAR) != ' ') AND (interaction_start_time IS NOT NULL)
    AND DATE_TRUNC('month',CAST(SUBSTR(CAST(interaction_start_time AS VARCHAR),1,10) AS DATE)) BETWEEN ((SELECT input_month FROM parameters)) AND ((SELECT input_month FROM parameters) + interval '1' month)
)

,interactions_fields AS (
SELECT  *
        ,CAST(SUBSTR(CAST(interaction_start_time AS VARCHAR),1,10) AS DATE) AS interaction_date
        ,DATE_TRUNC('month',CAST(SUBSTR(CAST(interaction_start_time AS VARCHAR),1,10) AS DATE)) AS month
FROM clean_interaction_time
)
---------------------Users with interactions (Tiers)--------------------------
,last_interaction AS (
SELECT  account_id AS last_account
        ,first_value(interaction_date) OVER(PARTITION BY account_id, DATE_TRUNC('month',interaction_date) ORDER BY interaction_date DESC) AS last_interaction_date
FROM interactions_fields
)

,join_last_interaction AS (
SELECT  account_id
        ,interaction_id
        ,interaction_date
        ,DATE_TRUNC('month',last_interaction_date) AS interaction_month
        ,last_interaction_date
        ,DATE_ADD('DAY',-60, last_interaction_date) AS window_day
FROM interactions_fields W INNER JOIN last_interaction L ON W.account_id = L.last_account
)

,interactions_count AS (
SELECT  interaction_month
        ,account_id
        ,count(DISTINCT interaction_id) AS interactions
FROM join_last_interaction
WHERE interaction_date BETWEEN window_day AND last_interaction_date
GROUP BY 1,2
)

,interactions_tier AS (
SELECT  i.*
        ,CASE   WHEN interactions = 1 THEN '1' 
                WHEN interactions = 2 THEN '2' 
                WHEN interactions >= 3 THEN '>3'
                ELSE NULL 
            END AS interaction_tier
FROM interactions_count i 
)
---------------------Users with tickets (Tiers)--------------------------
,users_tickets AS (
SELECT  DISTINCT account_id
        ,interaction_id
        ,interaction_date
FROM interactions_fields
WHERE interaction_purpose_descrip = 'TICKET'
)

,last_ticket AS (
SELECT  account_id AS last_account
        ,first_value(interaction_date) OVER(PARTITION BY account_id,DATE_TRUNC('month',interaction_date) ORDER BY interaction_date DESC) AS last_interaction_date
FROM users_tickets
)

,join_last_ticket AS (
SELECT  account_id
        ,interaction_id
        ,interaction_date
        ,DATE_TRUNC('month',last_interaction_date) AS interaction_month
        ,last_interaction_date
        ,DATE_ADD('DAY',-60, last_interaction_date) AS window_day
FROM users_tickets W INNER JOIN last_ticket L ON W.account_id = L.last_account
)

,tickets_count AS (
SELECT  interaction_month
        ,account_id
        ,count(DISTINCT interaction_id) AS tickets
FROM join_last_ticket
WHERE interaction_date BETWEEN window_day AND last_interaction_date
GROUP BY 1,2
)

,tickets_tier AS (
SELECT  i.*,
        CASE    WHEN tickets = 1 THEN '1' 
                WHEN tickets = 2 THEN '2' 
                WHEN tickets >= 3 THEN '>3'
        ELSE NULL END AS ticket_tier
FROM tickets_count i 
)
------------------tickets per month-----------------------
,tickets_per_month AS (
SELECT  DATE_TRUNC('month',interaction_date) AS month
        ,account_id
        ,count(interaction_date) AS number_tickets
FROM users_tickets
WHERE interaction_id IS NOT NULL
GROUP BY 1,2
)
------------------Repair Times----------------------------
,repair_times AS (
SELECT  account_id
        ,CAST(SUBSTR(CAST(interaction_start_time AS VARCHAR),1,10) AS DATE) AS interaction_start_time
        ,CAST(SUBSTR(CAST(interaction_end_time AS VARCHAR),1,10) AS DATE) AS interaction_end_time
        ,DATE_DIFF('day',CAST(SUBSTR(CAST(interaction_start_time AS VARCHAR),1,10) AS DATE),CAST(SUBSTR(CAST(interaction_end_time AS VARCHAR),1,10) AS DATE)) AS duration
        ,DATE_TRUNC ('Month',CAST(SUBSTR(CAST(interaction_start_time AS VARCHAR),1,10) AS DATE)) AS Month
FROM clean_interaction_time
WHERE interaction_purpose_descrip = 'TICKET' AND interaction_status ='CLOSED'
)
-----------------Missed Visits------------------------------
,missed_visits AS (
SELECT  month
        ,account_id
        ,CASE WHEN other_interaction_info8 IN ('Cliente reagenda cita','Cliente ausente','Cliente no deja entrar') THEN account_id 
            ELSE NULL END AS missed_visits
FROM interactions_fields
WHERE interaction_purpose_descrip = 'TRUCKROLL' and interaction_status ='CLOSED'
)
-----------------Sprint 5 Flags-----------------------------
,interaction_tier_flag AS(
SELECT  F.*
        ,CASE WHEN i.account_id IS NOT NULL THEN finalaccount ELSE NULL END AS interactions
        ,interaction_tier
FROM fmc_table_adj F LEFT JOIN interactions_tier i ON F.finalaccount = i.account_id AND F.month = i.interaction_month
)

,ticket_tier_flag AS (
SELECT  F.*
        ,CASE WHEN i.account_id IS NOT NULL THEN finalaccount ELSE NULL END AS tickets
        ,ticket_tier
FROM interaction_tier_flag F LEFT JOIN tickets_tier i ON F.fixedaccount = i.account_id AND F.month = i.interaction_month
)

,number_tickets_flag AS (
SELECT  F.*
        ,number_tickets
FROM ticket_tier_flag F LEFT JOIN tickets_per_month i ON F.fixedaccount = i.account_id AND F.month = i.Month
)

,repair_times_flag AS (
SELECT  F.*
        ,CASE WHEN duration >= 4 THEN finalaccount ELSE NULL END AS outlier_repair
FROM number_tickets_flag F LEFT JOIN repair_times R ON F.finalaccount = R.account_id AND F.month = R.month
)

,missed_visits_flag AS (
SELECT  F.*
        ,CASE WHEN M.account_id IS NOT NULL THEN finalaccount ELSE NULL END AS users_truckrolls
        ,missed_visits
FROM repair_times_flag F LEFT JOIN missed_visits M ON F.finalaccount = M.account_id AND F.month = M.month

)

,final_fields AS (
SELECT  DISTINCT month
        ,B_Final_TechFlag
        ,B_FMCSegment
        ,B_FMCType
        ,E_Final_TechFlag
        ,E_FMCSegment
        ,E_FMCType
        ,b_final_tenure
        ,e_final_tenure
        ,B_FixedTenure
        ,E_FixedTenure
        ,finalchurnflag
        ,fixedchurnflag
        ,fixedchurntype
        ,fixedmainmovement
        ,waterfall_flag
        ,mobile_activeeom
        ,mobilechurnflag
        ,interaction_tier
        ,ticket_tier
        ,finalaccount
        ,fixedaccount
        ,interactions
        ,tickets
        ,number_tickets AS prevnumber_tickets
        ,records_per_user
        ,number_tickets AS number_tickets
        ,outlier_repair
        ,users_truckrolls
        ,missed_visits
FROM missed_visits_flag
)

SELECT  month
        ,B_Final_TechFlag, B_FMCSegment, B_FMCType,E_Final_TechFlag, E_FMCSegment, E_FMCType,b_final_tenure,e_final_tenure,B_FixedTenure,E_FixedTenure,interaction_tier,ticket_tier,finalchurnflag,fixedchurnflag,waterfall_flag
        ,count(DISTINCT finalaccount) AS Total_Accounts
        ,count(DISTINCT fixedaccount) AS Fixed_Accounts
        ,count(DISTINCT interactions) AS Usersinteractions
        ,count(DISTINCT tickets) AS Userstickets
        ,round(SUM(number_tickets),0) AS number_tickets
        ,count(DISTINCT outlier_repair) AS outlier_repairs
        ,count(DISTINCT users_truckrolls) AS users_truckrolls
        ,count(DISTINCT missed_visits) AS missed_visits
FROM final_fields
WHERE ((Fixedchurntype != 'Fixed Voluntary Churner' AND Fixedchurntype != 'Fixed Involuntary Churner') OR Fixedchurntype IS NULL) AND finalchurnflag !='Fixed Churner'
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16
