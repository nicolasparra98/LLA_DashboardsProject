-----------------------------------------------------------------------------------------
--------------------------- LCPR MOBILE TABLE - V1 --------------------------------------
-----------------------------------------------------------------------------------------
--CREATE TABLE IF NOT EXISTS "lla_cco_int_stg"."cwp_fix_stg_dashboardinput_dinamico_Prueba2SEPT_jul" AS

WITH 

parameters AS (
--> Seleccionar el mes en que se desea realizar la corrida
SELECT  DATE_TRUNC('month',DATE('2023-01-01')) AS input_month
        ,85 as overdue_days
)

,BOM_active_base AS (
SELECT  cust_id AS account
        ,MAX(cust_hm_phn) AS phone
        ,(date(dt) + interval '1' month - interval '1' day) AS mob_b_dim_date
        ,DATE_DIFF('day',date(MAX(cust_crtn_dt)),(date(dt) + interval '1' month - interval '1' day)) AS mob_b_mes_TenureDays
        ,date(MAX(cust_crtn_dt)) AS mob_b_att_MaxStart
        ,CASE   WHEN DATE_DIFF('day',date(MAX(cust_crtn_dt)),(date(dt) + interval '1' month - interval '1' day)) <= 180 THEN 'Early Tenure'
                WHEN DATE_DIFF('day',date(MAX(cust_crtn_dt)),(date(dt) + interval '1' month - interval '1' day)) <= 360 THEN 'Mid Tenure'        
                WHEN DATE_DIFF('day',date(MAX(cust_crtn_dt)),(date(dt) + interval '1' month - interval '1' day)) >  360 THEN 'Late Tenure'
                    ELSE NULL END AS mob_b_att_TenureSegment
        ,SUM(indiv_inslm_amt) AS mob_b_mes_MRC
        ,COUNT(DISTINCT subsrptn_id) AS mob_b_mes_numRGUS
FROM "lcpr.stage.dev"."tbl_pstpd_cust_mstr_ss_data"
WHERE date(dt) = (SELECT input_month FROM parameters) - interval '1' month
    AND cust_sts = 'O'
    AND cust_type = 'Liberty Regular'
    AND acct_type_cd = 'I'
    AND rgn_nm = 'PR'
GROUP BY 1,3
)

,EOM_active_base AS (
SELECT  cust_id AS account
        ,MAX(cust_hm_phn) AS phone
        ,(date(dt) + interval '1' month - interval '1' day) AS mob_e_dim_date
        ,DATE_DIFF('day',date(MAX(cust_crtn_dt)),(date(dt) + interval '1' month - interval '1' day)) AS mob_e_mes_TenureDays
        ,date(MAX(cust_crtn_dt)) AS mob_e_att_MaxStart
        ,CASE   WHEN DATE_DIFF('day',date(MAX(cust_crtn_dt)),(date(dt) + interval '1' month - interval '1' day)) <= 180 THEN 'Early Tenure'
                WHEN DATE_DIFF('day',date(MAX(cust_crtn_dt)),(date(dt) + interval '1' month - interval '1' day)) <= 360 THEN 'Mid Tenure'        
                WHEN DATE_DIFF('day',date(MAX(cust_crtn_dt)),(date(dt) + interval '1' month - interval '1' day)) >  360 THEN 'Late Tenure'
                    ELSE NULL END AS mob_e_att_TenureSegment
        ,SUM(indiv_inslm_amt) AS mob_e_mes_MRC
        ,COUNT(DISTINCT subsrptn_id) AS mob_e_mes_numRGUS
FROM "lcpr.stage.dev"."tbl_pstpd_cust_mstr_ss_data"
WHERE date(dt) = (SELECT input_month FROM parameters)
    AND cust_sts = 'O'
    AND cust_type = 'Liberty Regular'
    AND acct_type_cd = 'I'
    AND rgn_nm = 'PR'
GROUP BY 1,3
)

,customer_status AS (
SELECT  (SELECT input_month FROM parameters) AS mob_s_dim_month
        ,CASE   WHEN (A.account IS NOT NULL AND B.account IS NOT NULL) OR (A.account IS NOT NULL AND B.account IS NULL) THEN A.account
                WHEN (A.account IS NULL AND B.account IS NOT NULL) THEN B.account
                    END AS mob_s_att_account
        ,CASE   WHEN (A.phone IS NOT NULL AND B.phone IS NOT NULL) OR (A.phone IS NOT NULL AND B.phone IS NULL) THEN A.phone
                WHEN (A.phone IS NULL AND B.phone IS NOT NULL) THEN B.phone
                    ELSE NULL END AS mob_s_att_phone
        ,IF(A.account IS NOT NULL,1,0) AS mob_b_att_active
        ,IF(B.account IS NOT NULL,1,0) AS mob_e_att_active
        ,mob_b_dim_date
        ,mob_b_mes_TenureDays
        ,mob_b_att_MaxStart
        ,mob_b_att_TenureSegment
        ,mob_b_mes_MRC
        ,mob_b_mes_numRGUS
        ,mob_e_dim_date
        ,mob_e_mes_TenureDays
        ,mob_e_att_MaxStart
        ,mob_e_att_TenureSegment
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
SELECT  DATE_TRUNC('MONTH',DATE(dt)) AS month
        ,cust_id AS churn_account
        ,date(substr(subsrptn_sts_dt,1,10)) AS disconnection_date
        ,lst_susp_rsn_desc
        ,IF(lower(lst_susp_rsn_desc) LIKE '%no%pay%','Involuntary','Voluntary') AS churn_type
FROM "lcpr.stage.dev"."tbl_pstpd_cust_cxl_incr_data"
WHERE cust_sts = 'N'
    AND cust_type = 'Liberty Regular'
    AND acct_type_cd = 'I'
    AND rgn_nm = 'PR'
    AND DATE(dt) = (SELECT input_month FROM parameters)
    AND cust_id NOT IN (SELECT DISTINCT cust_id
                        FROM "lcpr.stage.dev"."tbl_pstpd_cust_cxl_incr_data"
                        WHERE cust_sts = 'O'
                            AND cust_type = 'Liberty Regular'
                            AND acct_type_cd = 'I'
                            AND rgn_nm = 'PR'
                            AND DATE(dt) = (SELECT input_month FROM parameters) - INTERVAL '1' MONTH
                        )
)

,all_churners AS (
SELECT  month
        ,churn_account
        ,churn_type
FROM    (SELECT *
                ,row_number() over (PARTITION BY churn_account order by disconnection_date DESC) as row_num
        FROM disconnections)
where row_num = 1
)

,mobile_table_churn_flag AS(
SELECT  A.*
        ,CASE   WHEN B.churn_account IS NOT NULL THEN '1. Mobile Churner'
                WHEN B.churn_account IS NULL THEN '2. Mobile NonChurner'
                    END AS mob_s_fla_ChurnFlag
        ,CASE   WHEN B.churn_type = 'Involuntary' THEN '2. Mobile Involuntary Churner'
                WHEN B.churn_type = 'Voluntary' THEN '1. Mobile Voluntary Churner'
                    ELSE NULL END AS mob_s_fla_ChurnType
FROM spin_movement_flag A LEFT JOIN all_churners B ON A.mob_s_att_account = B.churn_account AND A.mob_s_dim_month = B.month
)

SELECT *
FROM mobile_table_churn_flag
