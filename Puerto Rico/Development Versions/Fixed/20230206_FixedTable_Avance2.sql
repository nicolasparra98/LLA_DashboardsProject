-----------------------------------------------------------------------------------------
---------------------------- LCPR FIXED TABLE - V1 --------------------------------------
-----------------------------------------------------------------------------------------
--CREATE TABLE IF NOT EXISTS "lla_cco_int_stg"."cwp_fix_stg_dashboardinput_dinamico_Prueba2SEPT_jul" AS

WITH 

parameters AS (
-- Seleccionar el mes en que se desea realizar la corrida
SELECT  DATE_TRUNC('month',DATE('2022-12-01')) AS input_month
        ,85 as overdue_days
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
        ,first_value(delinquency_days) over(PARTITION BY sub_acct_no_sbb,date_trunc('month',date(dt)) ORDER BY date(dt) DESC) AS last_overdue
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
    AND (CAST(overdue AS INTEGER) < (SELECT overdue_days FROM parameters) OR overdue IS NULL)
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
    AND (CAST(overdue AS INTEGER) <= (SELECT overdue_days FROM parameters) OR overdue IS NULL)
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
FROM BOM_active_base A FULL OUTER JOIN EOM_active_base b 
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

--------------------------------- VOLUNTARY CHURN ---------------------------------------

-------------------------------- INVOLUNTARY CHURN --------------------------------------

,first_cust_record AS (
SELECT  DATE_TRUNC('MONTH',DATE_ADD('MONTH',1,DATE(dt))) AS mes
        ,fix_s_att_account
        ,MIN(date(dt)) AS first_cust_record
        ,DATE_ADD('day',-1,MIN(date(dt))) AS prev_first_cust_record
FROM useful_fields
WHERE date(dt) = date_trunc('MONTH', DATE(dt)) + interval '1' MONTH - interval '1' day
GROUP BY 1,2
)

,last_cust_record AS (
SELECT  DATE_TRUNC('MONTH',DATE(dt)) AS mes
        ,fix_s_att_account
        ,MAX(date(dt)) AS last_cust_record
        ,DATE_ADD('day',-1,MAX(date(dt))) AS prev_last_cust_record
        ,DATE_ADD('day',-2,MAX(date(dt))) AS prev_last_cust_record2
FROM useful_fields
GROUP BY 1,2
ORDER BY 1,2
)

,no_overdue AS (
SELECT  DATE_TRUNC('MONTH', DATE_ADD('MONTH',1, DATE(A.dt))) AS MES
        ,A.fix_s_att_account
        ,A.overdue
FROM useful_fields A INNER JOIN first_cust_record B ON A.fix_s_att_account = B.fix_s_att_account
WHERE (CAST(A.overdue as INT) < (SELECT overdue_days FROM parameters))
    AND (date(A.dt) = B.first_cust_record or date(A.dt) = B.prev_first_cust_record)
GROUP BY 1,2,3
)

,overdue_last_day AS (
SELECT  DATE_TRUNC('MONTH',DATE(dt)) AS MES
        ,A.fix_s_att_account
        ,A.overdue
        ,(DATE_DIFF('DAY',DATE(dt),max_start)) AS churn_tenure_days
FROM useful_fields A INNER JOIN last_cust_record B ON A.fix_s_att_account = B.fix_s_att_account
WHERE date(A.dt) IN (B.last_cust_record,B.prev_last_cust_record,B.prev_last_cust_record2)
    AND (CAST(A.overdue AS INTEGER) >= (SELECT overdue_days FROM parameters))
GROUP BY 1,2,3,4
)

,involuntary_net_churners AS(
SELECT  DISTINCT A.mes AS month
        ,A.fix_s_att_account
        ,B.churn_tenure_days
FROM no_overdue A INNER JOIN overdue_last_day B ON A.fix_s_att_account = B.fix_s_att_account and A.mes = B.mes
)

,involuntary_churners AS (
SELECT  DISTINCT A.month
        ,A.fix_s_att_account AS churn_account
        ,A.churn_tenure_days
        ,CASE WHEN A.fix_s_att_account IS NOT NULL THEN '2. Fixed Involuntary Churner' END AS fix_s_fla_ChurnType
FROM involuntary_net_churners A LEFT JOIN useful_fields B on A.fix_s_att_account = B.fix_s_att_account AND A.month = DATE_TRUNC('month',date(B.dt))
where last_overdue >= (SELECT overdue_days FROM parameters)
GROUP BY 1,2,4,3
)

,final_involuntary_churners AS (
SELECT  DISTINCT month
        ,churn_account
        ,fix_s_fla_ChurnType
FROM involuntary_churners
WHERE fix_s_fla_ChurnType = '2. Fixed Involuntary Churner'
)

/*
,all_churners AS (
SELECT  DISTINCT month
        ,churn_account
        ,fix_s_fla_ChurnType
FROM    (SELECT month,churn_account,fix_s_fla_ChurnType FROM final_voluntary_churners A 
         UNION ALL
         SELECT month,churn_account,fix_s_fla_ChurnType FROM final_involuntary_churners B
        )
)

,fixed_table_churn_flag AS(
SELECT  A.*
        ,CASE   WHEN B.ChurnAccount IS NOT NULL THEN '1. Fixed Churner'
                WHEN B.ChurnAccount IS NULL THEN '2. Fixed NonChurner'
                    END AS fix_s_fla_ChurnFlag
        ,CASE   WHEN B.ChurnAccount IS NOT NULL THE fix_s_fla_ChurnType
                    END AS fix_s_fla_ChurnType
FROM spin_movement_flag A LEFT JOIN all_churners B ON A.fix_s_att_account = B.churn_account AND A.fix_s_dim_month = B.month
)
*/

,fixed_table_churn_flag_PROVISIONAL AS(
SELECT  A.*
        ,CASE   WHEN B.churn_account IS NOT NULL THEN '1. Fixed Churner'
                WHEN B.churn_account IS NULL THEN '2. Fixed NonChurner'
                    END AS fix_s_fla_ChurnFlag
        ,CASE   WHEN B.churn_account IS NOT NULL THEN fix_s_fla_ChurnType
                    END AS fix_s_fla_ChurnType
FROM spin_movement_flag A LEFT JOIN final_involuntary_churners B ON A.fix_s_att_account = B.churn_account AND A.fix_s_dim_month = B.month
)

select distinct fix_s_dim_month, fix_s_fla_MainMovement,fix_s_fla_SpinMovement,fix_s_fla_ChurnFlag,fix_s_fla_ChurnType, count(distinct fix_s_att_account) as accounts, sum(fix_b_mes_numRGUS) as BOM_RGUs,sum(fix_e_mes_numRGUS) as BOM_RGUs
from fixed_table_churn_flag_PROVISIONAL 
group by 1,2,3,4,5
order by 1,2,3,4,5

-------------------------------- FMC AND REJOINERS --------------------------------------
