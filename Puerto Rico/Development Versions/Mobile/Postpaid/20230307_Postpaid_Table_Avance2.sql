-----------------------------------------------------------------------------------------
-------------------------- LCPR POSTPAID TABLE - V1 -------------------------------------
-----------------------------------------------------------------------------------------
--CREATE TABLE IF NOT EXISTS "db_stage_dev"."lcpr_postpaid_table_jan_mar06" AS

WITH 

parameters AS (
--> Seleccionar el mes en que se desea realizar la corrida
SELECT  DATE_TRUNC('month',DATE('2023-01-01')) AS input_month
        ,85 as overdue_days
)

,BOM_active_base AS (
SELECT  subsrptn_id AS account
        ,cust_id AS parent_account
        ,(date(dt) + interval '1' month - interval '1' day) AS mob_b_dim_date
        ,DATE_DIFF('day',date(subsrptn_actvtn_dt),(date(dt) + interval '1' month - interval '1' day)) AS mob_b_mes_TenureDays
        ,date(subsrptn_actvtn_dt) AS mob_b_att_MaxStart
        ,CASE   WHEN DATE_DIFF('day',date(subsrptn_actvtn_dt),(date(dt) + interval '1' month - interval '1' day)) <= 180 THEN 'Early Tenure'
                WHEN DATE_DIFF('day',date(subsrptn_actvtn_dt),(date(dt) + interval '1' month - interval '1' day)) <= 360 THEN 'Mid Tenure'        
                WHEN DATE_DIFF('day',date(subsrptn_actvtn_dt),(date(dt) + interval '1' month - interval '1' day)) >  360 THEN 'Late Tenure'
                    ELSE NULL END AS mob_b_fla_Tenure
        ,indiv_inslm_amt AS mob_b_mes_MRC
        ,1 AS mob_b_mes_numRGUS
FROM "lcpr.stage.dev"."tbl_pstpd_cust_mstr_ss_data"
WHERE date(dt) = (SELECT input_month FROM parameters) - interval '1' month
    --> Flags utilizadas para clasificar a los usuarios residenciales. Input de Juan C. Vega
    AND cust_sts = 'O'
    AND acct_type_cd = 'I'
    AND rgn_nm <> 'VI'
    AND subsrptn_sts = 'A'
)

,EOM_active_base AS (
SELECT  subsrptn_id as account
        ,cust_id AS parent_account
        ,(date(dt) + interval '1' month - interval '1' day) AS mob_e_dim_date
        ,DATE_DIFF('day',date(subsrptn_actvtn_dt),(date(dt) + interval '1' month - interval '1' day)) AS mob_e_mes_TenureDays
        ,date(subsrptn_actvtn_dt) AS mob_e_att_MaxStart
        ,CASE   WHEN DATE_DIFF('day',date(subsrptn_actvtn_dt),(date(dt) + interval '1' month - interval '1' day)) <= 180 THEN 'Early Tenure'
                WHEN DATE_DIFF('day',date(subsrptn_actvtn_dt),(date(dt) + interval '1' month - interval '1' day)) <= 360 THEN 'Mid Tenure'        
                WHEN DATE_DIFF('day',date(subsrptn_actvtn_dt),(date(dt) + interval '1' month - interval '1' day)) >  360 THEN 'Late Tenure'
                    ELSE NULL END AS mob_e_fla_Tenure
        ,indiv_inslm_amt AS mob_e_mes_MRC
        ,1 AS mob_e_mes_numRGUS
FROM "lcpr.stage.dev"."tbl_pstpd_cust_mstr_ss_data"
WHERE date(dt) = (SELECT input_month FROM parameters)
        --> Flags utilizadas para clasificar a los usuarios residenciales. Input de Juan C. Vega
        AND cust_sts = 'O'
        AND acct_type_cd = 'I'
        AND rgn_nm <> 'VI'
        AND subsrptn_sts = 'A'
)

,customer_status AS (
SELECT  (SELECT input_month FROM parameters) AS mob_s_dim_month
        ,CASE   WHEN (A.account IS NOT NULL AND B.account IS NOT NULL) OR (A.account IS NOT NULL AND B.account IS NULL) THEN A.account
                WHEN (A.account IS NULL AND B.account IS NOT NULL) THEN B.account
                    END AS mob_s_att_account
        ,CASE   WHEN (A.parent_account IS NOT NULL AND B.parent_account IS NOT NULL) OR (A.parent_account IS NOT NULL AND B.parent_account IS NULL) THEN A.parent_account
                WHEN (A.parent_account IS NULL AND B.parent_account IS NOT NULL) THEN B.parent_account
                    ELSE NULL END AS mob_s_att_ParentAccount
        ,'Postpaid' AS mob_s_att_MobileType
        ,IF(A.account IS NOT NULL,1,0) AS mob_b_att_active
        ,IF(B.account IS NOT NULL,1,0) AS mob_e_att_active
        ,mob_b_dim_date
        ,mob_b_mes_TenureDays
        ,mob_b_att_MaxStart
        ,mob_b_fla_Tenure
        ,mob_b_mes_MRC
        ,mob_b_mes_numRGUS
        ,mob_e_dim_date
        ,mob_e_mes_TenureDays
        ,mob_e_att_MaxStart
        ,mob_e_fla_Tenure
        ,mob_e_mes_MRC
        ,mob_e_mes_numRGUS
FROM BOM_active_base A FULL OUTER JOIN EOM_active_base B
    ON A.account = B.account
)

,main_movement_flag AS(
SELECT  *
        ,CASE   WHEN (mob_e_mes_numRGUS - mob_b_mes_numRGUS) = 0 THEN '1.SameRGUs' 
                WHEN (mob_e_mes_numRGUS - mob_b_mes_numRGUS) > 0 THEN '2.Upsell'
                WHEN (mob_e_mes_numRGUS - mob_b_mes_numRGUS) < 0 THEN '3.Downsell'
                WHEN (mob_b_mes_numRGUS IS NULL AND mob_e_mes_numRGUS > 0 AND DATE_TRUNC('MONTH',mob_e_att_MaxStart) =  mob_s_dim_month) THEN '4.New Customer'
                WHEN (mob_b_mes_numRGUS IS NULL AND mob_e_mes_numRGUS > 0 AND DATE_TRUNC('MONTH',mob_e_att_MaxStart) <> mob_s_dim_month) THEN '5.Come Back to Life'
                WHEN (mob_b_mes_numRGUS > 0 AND mob_e_mes_numRGUS IS NULL) THEN '6.Null last day'
                WHEN (mob_b_mes_numRGUS IS NULL AND mob_e_mes_numRGUS IS NULL) THEN '7.Always null'
                    END AS mob_s_fla_MainMovement
FROM customer_status
)

,spin_movement_flag AS(
SELECT  *
        ,ROUND((mob_e_mes_MRC - mob_b_mes_MRC),0) AS mob_s_mes_MRCdiff
        ,CASE   WHEN mob_s_fla_MainMovement = '1.SameRGUs' AND (mob_e_mes_MRC - mob_b_mes_MRC) = 0 THEN '1.Same'
                WHEN mob_s_fla_MainMovement = '1.SameRGUs' AND (mob_e_mes_MRC - mob_b_mes_MRC) > 0 THEN '2.Upspin'
                WHEN mob_s_fla_MainMovement = '1.SameRGUs' AND (mob_e_mes_MRC - mob_b_mes_MRC) < 0 THEN '3.Downspin'
                    ELSE '4.NoSpin' END AS mob_s_fla_SpinMovement
FROM main_movement_flag 
)

,disconnections AS (
SELECT  A.*
        ,IF(A.mob_s_fla_MainMovement = '6.Null last day',IF((lower(trim(B.lst_susp_rsn_desc)) LIKE '%no%pay%' OR lower(trim(B.lst_susp_rsn_desc)) LIKE '%invol%'),'2. Mobile Involuntary Churner','1. Mobile Voluntary Churner'),NULL) AS mob_s_fla_ChurnType
FROM spin_movement_flag A LEFT JOIN "lcpr.stage.dev"."tbl_pstpd_cust_mstr_ss_data" B
    ON A.mob_s_dim_month = DATE(B.dt) AND A.mob_s_att_account = B.subsrptn_id
)

,mobile_table_churn_flag AS(
SELECT  *
        ,CASE   WHEN mob_s_fla_ChurnType IS NOT NULL THEN '1. Mobile Churner'
                WHEN mob_s_fla_ChurnType IS NULL THEN '2. Mobile NonChurner'
                    END AS mob_s_fla_ChurnFlag
FROM disconnections 
)

,rejoiner_candidates AS (
SELECT  subsrptn_id AS rejoiner_account
FROM "lcpr.stage.dev"."tbl_pstpd_cust_mstr_ss_data"
WHERE date(dt) = (SELECT input_month FROM parameters) - interval '2' month
    AND cust_sts = 'O'
    AND acct_type_cd = 'I'
    AND rgn_nm <> 'VI'
    AND subsrptn_sts = 'A'
    AND subsrptn_id NOT IN (SELECT DISTINCT subsrptn_id
                            FROM "lcpr.stage.dev"."tbl_pstpd_cust_mstr_ss_data"
                            WHERE DATE(dt) = (SELECT input_month FROM parameters) - interval '1' month
                            AND cust_sts = 'O'
                            AND acct_type_cd = 'I'
                            AND rgn_nm <> 'VI'
                            AND subsrptn_sts = 'A')
)

,full_flags AS (
SELECT  mob_s_dim_month
        ,mob_s_att_account
        ,mob_s_att_ParentAccount
        --,mob_s_att_MobileType
        ,mob_b_att_active
        ,IF(mob_s_fla_ChurnFlag = '1. Mobile Churner',0,mob_e_att_active) AS mob_e_att_active
        ,mob_b_dim_date
        ,mob_b_mes_TenureDays
        ,mob_b_att_MaxStart
        ,mob_b_fla_Tenure
        ,mob_b_mes_MRC
        ,mob_b_mes_numRGUS
        ,mob_e_dim_date
        ,mob_e_mes_TenureDays
        ,mob_e_att_MaxStart
        ,mob_e_fla_Tenure
        ,mob_e_mes_MRC
        ,IF(mob_s_fla_ChurnFlag = '1. Mobile Churner',0,mob_e_mes_numRGUS) AS mob_e_mes_numRGUS
        ,IF(mob_s_fla_ChurnFlag = '1. Mobile Churner','6.Null last day',mob_s_fla_MainMovement) AS mob_s_fla_MainMovement
        ,mob_s_mes_MRCdiff
        ,mob_s_fla_SpinMovement
        ,mob_s_fla_ChurnFlag
        ,mob_s_fla_ChurnType
        ,IF(mob_b_att_active = 0 AND mob_e_att_active = 1 AND mob_s_att_account IN (SELECT DISTINCT rejoiner_account FROM rejoiner_candidates),1,0) AS mob_s_fla_Rejoiner
FROM mobile_table_churn_flag
)

SELECT *
FROM full_flags
WHERE mob_b_att_active + mob_e_att_active >= 1
