-----------------------------------------------------------------------------------------
---------------------------- LCPR FIXED TABLE - V1 --------------------------------------
-----------------------------------------------------------------------------------------
--CREATE TABLE IF NOT EXISTS "lla_cco_int_stg"."cwp_fix_stg_dashboardinput_dinamico_Prueba2SEPT_jul" AS

WITH 

parameters AS (
-- Seleccionar el mes en que se desea realizar la corrida
SELECT DATE_TRUNC('month',DATE('2022-12-01')) AS input_month
)

,useful_fields AS (
SELECT  date(dt) AS dt
        ,DATE(as_of) AS as_of
        ,sub_acct_no_sbb AS fix_s_att_account
        ,home_phone_sbb AS fix_s_att_phone
        ,delinquency_days AS overdue
        ,(CAST(CAST(first_value(connect_dte_sbb) over (PARTITION BY sub_acct_no_sbb order by DATE(dt) DESC) AS TIMESTAMP) AS DATE)) AS max_start
        ,(video_chrg + hsd_chrg + voice_chrg) AS TOTAL_MRC
        ,IF(TRIM(drop_type) = 'FIBER','FTTH',IF(TRIM(drop_type) = 'FIBCO','HFC',IF(TRIM(drop_type) = 'COAX','COPPER',null))) AS tech_flag
        ,hsd AS numBB
        ,video AS numTV
        ,voice AS numVO
        ,null AS oldest_unpaid_bill_dt
        ,first_value(delinquency_days) over(PARTITION BY sub_acct_no_sbb,date_trunc('month',date(dt)) ORDER BY date(dt) DESC) AS Last_Overdue
FROM "lcpr.stage.dev"."customer_services_rate_lcpr"
WHERE play_type <> '0P'
    AND cust_typ_sbb = 'RES' 
    AND date(dt) BETWEEN ((SELECT input_month FROM parameters) + interval '1' MONTH - interval '1' DAY - interval '2' MONTH) AND  ((SELECT input_month FROM parameters) + interval '1' MONTH) 
)

,BOM_active_base AS (
SELECT  DATE_TRUNC('MONTH',dt) AS fix_s_dim_month
        ,fix_s_att_account
        ,dt AS fix_b_dim_date
        ,tech_flag AS fix_b_fla_tech
        ,CONCAT(CAST((numBB+numTV+numVO) AS VARCHAR),'P') AS fix_b_fla_MixCodeAdj
        ,CASE   WHEN NumBB = 1 AND NumTV = 0 AND NumVO = 0 THEN 'BB'
                WHEN NumBB = 0 AND NumTV = 1 AND NumVO = 0 THEN 'TV'
                WHEN NumBB = 0 AND NumTV = 0 AND NumVO = 1 THEN 'VO'
                WHEN NumBB = 1 AND NumTV = 1 AND NumVO = 0 THEN 'BB+TV'
                WHEN NumBB = 1 AND NumTV = 0 AND NumVO = 1 THEN 'BB+VO'
                WHEN NumBB = 0 AND NumTV = 1 AND NumVO = 1 THEN 'VO+TV'
                WHEN NumBB = 1 AND NumTV = 1 AND NumVO = 1 THEN 'BB+VO+TV'
                    END AS fix_b_fla_MixNameAdj
        ,NULL AS fix_b_att_bbCode
        ,NULL AS fix_b_att_tvCode
        ,NULL AS fix_b_att_voCode
        ,IF(numBB = 1,fix_s_att_account,NULL) AS fix_b_fla_BB
        ,IF(numTV = 1,fix_s_att_account,NULL) AS fix_b_fla_TV
        ,IF(numVO = 1,fix_s_att_account,NULL) AS fix_b_fla_VO
        ,(numBB + numTV + numVO) AS fix_b_mes_numRGUS
        ,total_MRC AS fix_b_mes_mrc
        ,overdue AS fix_b_mes_overdue
        ,max_start AS fix_b_att_MaxStart
        ,CASE   WHEN DATE_DIFF('DAY', Max_Start, DATE(dt)) <= 180 THEN 'Early-Tenure'
                WHEN DATE_DIFF('DAY', Max_Start, DATE(dt)) > 180 AND DATE_DIFF('DAY', Max_Start, DATE(dt)) <= 360 THEN 'Mid-Tenure'
                WHEN DATE_DIFF('DAY', Max_Start, DATE(dt)) > 360 THEN 'Late-Tenure'
                    END AS fix_b_fla_tenure
FROM useful_fields
WHERE dt = (SELECT input_month FROM parameters)
    AND (CAST(overdue AS INTEGER) < 90 OR overdue IS NULL)
)

,EOM_active_base AS (
SELECT  DATE_TRUNC('MONTH',(dt - interval '1' day))  AS fix_s_dim_month
        ,fix_s_att_account
        ,dt AS fix_e_dim_date
        ,tech_flag AS fix_e_fla_tech
        ,CONCAT(CAST((numBB+numTV+numVO) AS VARCHAR),'P') AS fix_e_fla_MixCodeAdj
        ,CASE   WHEN NumBB = 1 AND NumTV = 0 AND NumVO = 0 THEN 'BB'
                WHEN NumBB = 0 AND NumTV = 1 AND NumVO = 0 THEN 'TV'
                WHEN NumBB = 0 AND NumTV = 0 AND NumVO = 1 THEN 'VO'
                WHEN NumBB = 1 AND NumTV = 1 AND NumVO = 0 THEN 'BB+TV'
                WHEN NumBB = 1 AND NumTV = 0 AND NumVO = 1 THEN 'BB+VO'
                WHEN NumBB = 0 AND NumTV = 1 AND NumVO = 1 THEN 'VO+TV'
                WHEN NumBB = 1 AND NumTV = 1 AND NumVO = 1 THEN 'BB+VO+TV'
                    END AS fix_e_fla_MixNameAdj
        ,NULL AS fix_e_att_bbCode
        ,NULL AS fix_e_att_tvCode
        ,NULL AS fix_e_att_voCode
        ,IF(numBB = 1,fix_s_att_account,NULL) AS fix_e_fla_BB
        ,IF(numTV = 1,fix_s_att_account,NULL) AS fix_e_fla_TV
        ,IF(numVO = 1,fix_s_att_account,NULL) AS fix_e_fla_VO
        ,(numBB + numTV + numVO) AS fix_e_mes_numRGUS
        ,total_MRC AS fix_e_mes_mrc
        ,overdue AS fix_e_mes_overdue
        ,max_start AS fix_e_att_MaxStart
        ,CASE   WHEN DATE_DIFF('DAY', Max_Start, DATE(dt)) <= 180 THEN 'Early-Tenure'
                WHEN DATE_DIFF('DAY', Max_Start, DATE(dt)) > 180 AND DATE_DIFF('DAY', Max_Start, DATE(dt)) <= 360 THEN 'Mid-Tenure'
                WHEN DATE_DIFF('DAY', Max_Start, DATE(dt)) > 360 THEN 'Late-Tenure'
                    END AS fix_e_fla_tenure
FROM useful_fields
WHERE dt = (SELECT input_month FROM parameters) + interval '1' month
    AND (CAST(overdue AS INTEGER) <= 90 OR overdue IS NULL)
)

,customer_status AS (
SELECT  CASE    WHEN (A.fix_s_att_account IS NOT NULL AND B.fix_s_att_account IS NOT NULL) OR (A.fix_s_att_account IS NOT NULL AND B.fix_s_att_account IS NULL) THEN A.fix_s_dim_month
                WHEN (A.fix_s_att_account IS NULL AND B.fix_s_att_account IS NOT NULL) THEN B.fix_s_dim_month
                    END AS fix_s_dim_month
        ,CASE WHEN (A.fix_s_att_account IS NOT NULL AND B.fix_s_att_account IS NOT NULL) OR (A.fix_s_att_account IS NOT NULL AND B.fix_s_att_account IS NULL) THEN A.fix_s_att_account
        WHEN (A.fix_s_att_account IS NULL AND B.fix_s_att_account IS NOT NULL) THEN B.fix_s_att_account
                    END AS fix_s_att_account
        ,IF(A.fix_s_att_account IS NOT NULL,1,0) AS fix_b_att_active
        ,IF(B.fix_s_att_account IS NOT NULL,1,0) AS fix_e_att_active
        ,fix_b_dim_date
        ,fix_b_mes_overdue
        ,fix_b_att_MaxStart
        ,fix_b_fla_tenure
        ,fix_b_mes_mrc
        ,fix_b_fla_tech
        ,fix_b_mes_numRGUS
        ,fix_b_fla_MixNameAdj
        ,fix_b_fla_MixCodeAdj
        ,fix_b_fla_BB
        ,fix_b_fla_TV
        ,fix_b_fla_VO
        ,fix_b_att_bbCode
        ,fix_b_att_tvCode
        ,fix_b_att_voCode
        ,fix_e_dim_date
        ,fix_e_mes_overdue
        ,fix_e_att_MaxStart
        ,fix_e_fla_tenure
        ,fix_e_mes_mrc
        ,fix_e_fla_tech
        ,fix_e_mes_numRGUS
        ,fix_e_fla_MixNameAdj
        ,fix_e_fla_MixCodeAdj
        ,fix_e_fla_BB
        ,fix_e_fla_TV
        ,fix_e_fla_VO
        ,fix_e_att_bbCode
        ,fix_e_att_tvCode
        ,fix_e_att_voCode
FROM BOM_active_base A FULL OUTER JOIN EOM_active_base B
    ON A.fix_s_att_account = B.fix_s_att_account AND A.fix_s_dim_month = B.fix_s_dim_month
)

,main_movement_flag AS (
SELECT  A.*
        ,CASE   WHEN (fix_e_mes_numRGUS - fix_b_mes_numRGUS) = 0 THEN '1.SameRGUs'
                WHEN (fix_e_mes_numRGUS - fix_b_mes_numRGUS) > 0 THEN '2.Upsell'
                WHEN (fix_e_mes_numRGUS - fix_b_mes_numRGUS) < 0 THEN '3.Downsell'
                WHEN (fix_b_mes_numRGUS IS NULL AND fix_e_mes_numRGUS > 0 AND DATE_TRUNC ('MONTH', fix_e_att_MaxStart) =  fix_s_dim_month) THEN '4.New Customer'
                WHEN (fix_b_mes_numRGUS IS NULL AND fix_e_mes_numRGUS > 0 AND DATE_TRUNC ('MONTH', fix_e_att_MaxStart) <> fix_s_dim_month) THEN '5.Come Back to Life'
                WHEN (fix_b_mes_numRGUS > 0 AND fix_e_mes_numRGUS IS NULL) THEN '6.Null last day'
                WHEN (fix_b_mes_numRGUS IS NULL AND fix_e_mes_numRGUS IS NULL) THEN '7.Always null'
                WHEN (fix_b_mes_numRGUS IS NULL AND fix_e_mes_numRGUS > 0 AND DATE_TRUNC ('MONTH', fix_e_att_MaxStart) is null) THEN '8.Rejoiner-GrossAdd Gap'
                    END AS fix_s_fla_MainMovement
FROM customer_status A
)

,spin_movement_flag AS (
SELECT  A.*
        ,CASE   WHEN fix_s_fla_MainMovement = '1.SameRGUs' AND (fix_e_mes_mrc - fix_b_mes_mrc) > 0 THEN '1. Up-spin'
                WHEN fix_s_fla_MainMovement = '1.SameRGUs' AND (fix_e_mes_mrc - fix_b_mes_mrc) < 0 THEN '2. Down-spin'
                ELSE '3. No Spin' 
                    END AS fix_s_fla_SpinMovement
FROM main_movement_flag A
)

select fix_s_fla_MainMovement, fix_s_fla_SpinMovement, fix_b_att_active,fix_e_att_active,count(distinct fix_s_att_account) 
from spin_movement_flag
group by 1,2,3,4
order by 1,2,3,4
